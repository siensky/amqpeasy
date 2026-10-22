import { afterAll, afterEach, beforeAll, expect, setDefaultTimeout, test } from "bun:test";
import amqp, { type Channel, type ChannelModel } from "amqplib";
import { z } from "zod";
import amqpEasy, { type AmqpEasyClient } from "../../src/public.js";
import { BaseError, PreconditionFailedError, ResourceLockedError, ValidationError } from "../../src/error.js";

setDefaultTimeout(15000);

const uri = process.env.RABBITMQ_URL;
const integration = test.skipIf(!uri);
let admin: ChannelModel;
let control: Channel;
let clients: AmqpEasyClient[] = [];
let exchanges: string[] = [];
let queues: string[] = [];
const unique = () => `amqpeasy.test.${crypto.randomUUID()}`;

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(r => { resolve = r; });
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error("RabbitMQ test timed out")), 5000);
  });
  try { return await Promise.race([promise, timeout]); }
  finally { clearTimeout(timer!); }
}

beforeAll(async () => {
  if (!uri) return;
  admin = await amqp.connect(uri);
  control = await admin.createChannel();
});

afterEach(async () => {
  if (!uri) return;
  const failures: unknown[] = [];
  for (const client of clients) {
    try { await client.disconnect(); } catch (error) { failures.push(error); }
  }
  clients = [];
  for (const name of queues) await control.deleteQueue(name);
  queues = [];
  for (const name of exchanges) await control.deleteExchange(name);
  exchanges = [];
  if (failures.length) throw new AggregateError(failures, "Test cleanup failed");
});

afterAll(async () => { if (admin) await admin.close(); });

integration("topic routing delivers validated payloads between independent clients", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const delivered = deferred<number>();
  const consumer = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName: "", subscriptions: [{ routingKey: "order.*", schema: z.object({ id: z.number() }),
      handler: payload => { delivered.resolve(payload.id); },
    }],
  } });
  const publisher = await amqpEasy({ uri: uri!, exchangeName });
  clients.push(consumer, publisher);
  await consumer.connect(); await publisher.connect();
  await publisher.publish("order.created", { id: 42 });
  expect(await within(delivered.promise)).toBe(42);
  await publisher.disconnect();
  await consumer.disconnect();
});

integration("topic # matches zero and multiple words", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const delivered = deferred<string[]>();
  const seen: string[] = [];
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName: "", subscriptions: [{ routingKey: "order.#", schema: z.string(), handler: value => {
      seen.push(value); if (seen.length === 2) delivered.resolve(seen);
    } }],
  } }); clients.push(client);
  await client.connect();
  await client.publish("order", "zero");
  await client.publish("order.eu.created", "many");
  expect(await within(delivered.promise)).toEqual(["zero", "many"]);
});

integration("direct routes exact keys and fanout reaches two separate queues", async () => {
  for (const exchangeType of ["direct", "fanout"] as const) {
    const exchangeName = unique(); exchanges.push(exchangeName);
    const received = [deferred<string>(), deferred<string>()];
    for (const result of received) {
      const client = await amqpEasy({ uri: uri!, exchangeName, exchangeType, subscribe: {
        queueName: "", subscriptions: [{
          routingKey: exchangeType === "direct" ? "exact" : undefined,
          schema: z.string(), handler: value => { result.resolve(value); },
        }],
      } }); clients.push(client); await client.connect();
    }
    const publisher = await amqpEasy({ uri: uri!, exchangeName, exchangeType });
    clients.push(publisher); await publisher.connect();
    await publisher.publish(exchangeType === "direct" ? "exact" : "ignored", "hello");
    expect(await within(Promise.all(received.map(r => r.promise)))).toEqual(["hello", "hello"]);
  }
});

integration("schema failure reports ValidationError without calling the handler", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const failed = deferred<BaseError>();
  let handled = false;
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName: "", subscriptions: [{ routingKey: "event", schema: z.object({ id: z.number() }),
      handler: () => { handled = true; },
    }],
  } }, error => { failed.resolve(error); }); clients.push(client);
  await client.connect(); await client.publish("event", { id: "invalid" });
  expect(await within(failed.promise)).toBeInstanceOf(ValidationError);
  expect(handled).toBe(false);
});

integration("a real exchange conflict is reported as AMQP 406", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  await control.assertExchange(exchangeName, "direct", { durable: true });
  const reported: BaseError[] = [];
  const client = await amqpEasy({ uri: uri!, exchangeName, exchangeType: "topic" }, error => { reported.push(error); });
  clients.push(client); await client.connect();
  await expect(client.publish("event", {})).rejects.toBeInstanceOf(PreconditionFailedError);
  expect(reported.some(error => error instanceof PreconditionFailedError)).toBe(true);
});

integration("an exclusive queue owned by another connection reports AMQP 405", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique();
  await control.assertQueue(queueName, { exclusive: true }); queues.push(queueName);
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, subscriptions: [{ routingKey: "event", schema: z.unknown(), handler: () => {} }],
  } }, () => {}); clients.push(client);
  await expect(client.connect()).rejects.toBeInstanceOf(ResourceLockedError);
});

function deadLetterQueues(queueName: string, maxRetries = 3) {
  queues.push(queueName, `${queueName}.dead`);
  if (maxRetries > 0) queues.push(`${queueName}.retry`);
}

async function nextDeadLetter(queueName: string) {
  const received = deferred<amqp.ConsumeMessage>();
  await control.consume(`${queueName}.dead`, message => {
    if (message) { control.ack(message); received.resolve(message); }
  });
  return received;
}

integration("invalid schemas go straight to the durable dead-letter queue", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName);
  let calls = 0;
  const reported: BaseError[] = [];
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: true, subscriptions: [{ routingKey: "order.*", schema: z.object({ id: z.number() }),
      handler: () => { calls++; },
    }],
  } }, error => { reported.push(error); }); clients.push(client);
  await client.connect();
  const dead = await nextDeadLetter(queueName);
  await client.publish("order.created", { id: "wrong" });
  const message = await within(dead.promise);
  expect(calls).toBe(0);
  expect(JSON.parse(message.content.toString())).toEqual({ id: "wrong" });
  expect(message.properties.headers?.["x-amqpeasy-retry-count"]).toBe(0);
  expect(message.properties.headers?.["x-amqpeasy-error-code"]).toBe("VALIDATION_ERROR");
  expect(reported[0]).toBeInstanceOf(ValidationError);
  await client.disconnect();
  // Dead-letter mode keeps its queues after the client disconnects.
  expect((await control.checkQueue(`${queueName}.dead`)).queue).toBe(`${queueName}.dead`);
});

integration("invalid JSON is preserved in dead-letter without calling the handler", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName, 0);
  let calls = 0;
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: { maxRetries: 0 }, subscriptions: [{ routingKey: "event", schema: z.unknown(), handler: () => { calls++; } }],
  } }, () => {}); clients.push(client); await client.connect();
  const dead = await nextDeadLetter(queueName);
  control.publish(exchangeName, "event", Buffer.from("not JSON"));
  const message = await within(dead.promise);
  expect(message.content.toString()).toBe("not JSON");
  expect(calls).toBe(0);
});

integration("temporary failures retry with a delay, then stop at the configured limit", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName, 2);
  const times: number[] = [];
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: { maxRetries: 2, retryDelayMs: 100 }, subscriptions: [{ routingKey: "order.*", schema: z.object({ id: z.number() }),
      handler: () => { times.push(Date.now()); throw new Error("Temporary outage"); },
    }],
  } }, () => {}); clients.push(client); await client.connect();
  const dead = await nextDeadLetter(queueName);
  await client.publish("order.created", { id: 7 });
  const message = await within(dead.promise);
  expect(times).toHaveLength(3);
  expect(times[1]! - times[0]!).toBeGreaterThanOrEqual(80);
  expect(times[2]! - times[1]!).toBeGreaterThanOrEqual(80);
  expect(message.properties.headers?.["x-amqpeasy-retry-count"]).toBe(2);
  expect(message.properties.headers?.["x-amqpeasy-routing-key"]).toBe("order.created");
  expect(JSON.parse(message.content.toString())).toEqual({ id: 7 });
});

integration("a successful retry does not enter the dead-letter queue", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName, 2);
  let calls = 0;
  const done = deferred<void>();
  const client = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: { maxRetries: 2, retryDelayMs: 100 }, subscriptions: [{ routingKey: "order.*", schema: z.number(),
      handler: () => { calls++; if (calls === 1) throw new Error("Retry me"); done.resolve(); },
    }],
  } }, () => {}); clients.push(client); await client.connect();
  await client.publish("order.created", 7);
  await within(done.promise);
  expect(calls).toBe(2);
  expect((await control.checkQueue(`${queueName}.dead`)).messageCount).toBe(0);
});

integration("fanout retry returns only to the failed consumer queue", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName, 1);
  let failedCalls = 0;
  let healthyCalls = 0;
  const received = deferred<void>();
  const healthy = await amqpEasy({ uri: uri!, exchangeName, exchangeType: "fanout", subscribe: {
    queueName: "", subscriptions: [{ schema: z.string(), handler: () => { healthyCalls++; received.resolve(); } }],
  } });
  const failing = await amqpEasy({ uri: uri!, exchangeName, exchangeType: "fanout", subscribe: {
    queueName, deadLetter: { maxRetries: 1, retryDelayMs: 100 }, subscriptions: [{ schema: z.string(),
      handler: () => { failedCalls++; throw new Error("Fail"); },
    }],
  } }, () => {}); clients.push(healthy, failing);
  await healthy.connect(); await failing.connect();
  const dead = await nextDeadLetter(queueName);
  await healthy.publish("", "hello");
  await within(Promise.all([received.promise, dead.promise]));
  expect(failedCalls).toBe(2);
  expect(healthyCalls).toBe(1);
});

integration("a scheduled retry survives disconnecting the original client", async () => {
  const exchangeName = unique(); exchanges.push(exchangeName);
  const queueName = unique(); deadLetterQueues(queueName, 1);
  const settings = { maxRetries: 1, retryDelayMs: 500 };
  const failed = deferred<void>();
  const first = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: settings, subscriptions: [{ routingKey: "order.*", schema: z.number(),
      handler: () => { throw new Error("Temporary error"); },
    }],
  } }, () => { failed.resolve(); }); clients.push(first);
  await first.connect(); await first.publish("order.created", 42);
  await within(failed.promise);
  const deadline = Date.now() + 3000;
  while ((await control.checkQueue(`${queueName}.retry`)).messageCount === 0) {
    if (Date.now() > deadline) throw new Error("Message did not enter the retry queue");
    await new Promise(resolve => setTimeout(resolve, 10));
  }
  await first.disconnect();
  const received = deferred<number>();
  const second = await amqpEasy({ uri: uri!, exchangeName, subscribe: {
    queueName, deadLetter: settings, subscriptions: [{ routingKey: "order.*", schema: z.number(),
      handler: value => { received.resolve(value); },
    }],
  } }, () => {}); clients.push(second);
  await second.connect();
  expect(await within(received.promise)).toBe(42);
});
