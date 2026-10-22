import assert from "node:assert/strict";
import * as api from "amqpeasy";
import { BaseError, NotFoundError, ValidationError } from "amqpeasy/errors";
import { z } from "zod";

assert.deepEqual(Object.keys(api), ["default"]);
assert.equal(api.default.name, "amqpEasy");
assert.ok(new NotFoundError() instanceof BaseError);
assert.ok(new ValidationError() instanceof BaseError);
await assert.rejects(import("amqpeasy/dist/connection.js"));

if (process.env.RABBITMQ_URL) {
  const amqp = (await import("amqplib")).default;
  const uri = process.env.RABBITMQ_URL;
  const exchangeName = `amqpeasy.package.${crypto.randomUUID()}`;
  let resolveMessage;
  const received = new Promise(resolve => { resolveMessage = resolve; });
  const failures = [];
  const queueName = `${exchangeName}.work`;
  let attempts = 0;
  const consumer = await api.default({ uri, exchangeName, subscribe: {
    queueName, deadLetter: { maxRetries: 1, retryDelayMs: 100 }, subscriptions: [{ routingKey: "order.*", schema: z.object({ id: z.number() }),
      handler: payload => {
        attempts++;
        if (attempts === 1) throw new Error("Retry this packaged message");
        resolveMessage(payload);
      },
    }],
  } }, error => { failures.push(error); });
  const publisher = await api.default({ uri, exchangeName });
  let timer;
  try {
    await consumer.connect();
    await publisher.connect();
    await publisher.publish("order.created", { id: 17 });
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error("Packaged client did not receive the message")), 10000);
    });
    assert.deepEqual(await Promise.race([received, timeout]), { id: 17 });
    assert.equal(attempts, 2);
    assert.equal(failures.length, 1);
    assert.ok(failures[0] instanceof BaseError);
  } finally {
    clearTimeout(timer);
    await Promise.all([consumer.disconnect(), publisher.disconnect()]);
    const connection = await amqp.connect(uri);
    try {
      const channel = await connection.createChannel();
      for (const name of [queueName, `${queueName}.retry`, `${queueName}.dead`]) await channel.deleteQueue(name);
      await channel.deleteExchange(exchangeName);
    } finally { await connection.close(); }
  }
  console.log("Installed package: real RabbitMQ round trip and delayed retry passed");
}
console.log("Installed package: public imports and error classes passed");
