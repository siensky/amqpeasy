import { afterEach, beforeEach, expect, mock, test } from "bun:test";
import { z } from "zod";
import { EventEmitter } from "node:events";
import type { ConsumeMessage } from "amqplib";
import * as errors from "../../src/error.js";

const connectMock = mock();
mock.module("amqplib", () => ({ default: { connect: connectMock } }));
const publicApi = await import("../../src/public.js");
const amqpEasy = publicApi.default;

let reported: errors.BaseError[];
let received: unknown[];
let consume: (message: ConsumeMessage | null) => void | Promise<void>;
let api: Awaited<ReturnType<typeof amqpEasy>>;
let channel: ReturnType<typeof makeChannel>;
let connection: ReturnType<typeof makeConnection>;

function makeChannel() {
  return Object.assign(new EventEmitter(), {
    assertExchange: mock(async () => {}),
    assertQueue: mock(async () => ({ queue: "orders-test" })),
    bindQueue: mock(async () => {}),
    consume: mock(async (_queue: string, handler: typeof consume) => {
      consume = handler;
    }),
    publish: mock(() => true),
    ack: mock(() => {}),
    nack: mock(() => {}),
    close: mock(async () => {}),
  });
}

function makeConnection(ownChannel = channel) {
  return Object.assign(new EventEmitter(), {
    createChannel: mock(async () => ownChannel),
    close: mock(async () => {}),
  });
}

function brokerError(code: number) {
  return Object.assign(new Error(`Broker error ${code}`), { code });
}

function message(content: string): ConsumeMessage {
  return {
    fields: { routingKey: "order.created" },
    content: Buffer.from(content),
  } as ConsumeMessage;
}

beforeEach(async () => {
  reported = [];
  received = [];
  channel = makeChannel();
  connection = makeConnection();
  connectMock.mockReset();
  connectMock.mockResolvedValue(connection);
  api = await amqpEasy({
    uri: "amqp://test",
    exchangeName: "orders",
    subscribe: {
      queueName: "orders-test",
      subscriptions: [{
        routingKey: "order.created",
        schema: z.object({ id: z.number() }),
        handler: (payload) => { received.push(payload); },
      }],
    },
  }, (error) => { reported.push(error); });
});

afterEach(async () => {
  await api.disconnect().catch(() => {});
});

test("the main entry only exports amqpEasy", () => {
  expect(Object.keys(publicApi)).toEqual(["default"]);
  expect(amqpEasy.name).toBe("amqpEasy");
});

test("every AMQP error code reaches onError as its own subclass", async () => {
  await api.connect();
  const cases = [
    [311, errors.ContentTooLargeError], [312, errors.NoRouteError],
    [313, errors.NoConsumersError], [320, errors.ConnectionForcedError],
    [402, errors.InvalidPathError], [403, errors.AccessRefusedError],
    [404, errors.NotFoundError], [405, errors.ResourceLockedError],
    [406, errors.PreconditionFailedError], [501, errors.FrameError],
    [502, errors.AmqpSyntaxError], [503, errors.CommandInvalidError],
    [504, errors.ChannelError], [505, errors.UnexpectedFrameError],
    [506, errors.ResourceError], [530, errors.NotAllowedError],
    [540, errors.NotImplementedError], [541, errors.InternalError],
  ] as const;
  for (const [code, ErrorClass] of cases) {
    const original = brokerError(code);
    channel.emit("error", original);
    const error = reported.at(-1)!;
    expect(error).toBeInstanceOf(ErrorClass);
    expect(error).toBeInstanceOf(errors.BaseError);
    expect(error).toBeInstanceOf(Error);
    expect(error.code).toBe(code);
    expect(error.name).toBe(ErrorClass.name);
    expect(error.cause).toBe(original);
  }
  expect(reported).toHaveLength(cases.length);
});

test("connection rejection reports and throws the same typed error", async () => {
  const original = brokerError(403);
  connectMock.mockRejectedValue(original);
  await expect(api.connect()).rejects.toBeInstanceOf(errors.AccessRefusedError);
  expect(reported).toHaveLength(1);
  expect(reported[0]!.cause).toBe(original);
});

test("system and unknown codes are preserved by BaseError", async () => {
  const original = Object.assign(new Error("Connection refused"), { code: "ECONNREFUSED" });
  connectMock.mockRejectedValue(original);
  await expect(api.connect()).rejects.toMatchObject({ code: "ECONNREFUSED", cause: original });
  expect(reported[0]).toBeInstanceOf(errors.BaseError);
  connectMock.mockResolvedValue(connection);
  await api.connect();
  connection.emit("error", brokerError(999));
  expect(reported.at(-1)).toBeInstanceOf(errors.BaseError);
  expect(reported.at(-1)!.code).toBe(999);
});

test("queue setup failure reports the code and closes the connection", async () => {
  channel.assertQueue.mockRejectedValue(brokerError(405));
  await expect(api.connect()).rejects.toBeInstanceOf(errors.ResourceLockedError);
  expect(channel.close).toHaveBeenCalledTimes(1);
  expect(connection.close).toHaveBeenCalledTimes(1);
});

test("a publish error emitted and rejected is only reported once", async () => {
  await api.connect();
  const original = brokerError(406);
  channel.assertExchange.mockImplementation(async () => {
    channel.emit("error", original);
    throw original;
  });
  await expect(api.publish("order.created", {})).rejects.toBeInstanceOf(errors.PreconditionFailedError);
  expect(reported).toHaveLength(1);
  expect(reported[0]!.cause).toBe(original);
});

test("publishing without a connection reports an error", async () => {
  await expect(api.publish("order.created", {})).rejects.toBeInstanceOf(errors.BaseError);
  expect(reported).toHaveLength(1);
});

test("successful messages and publishing keep working", async () => {
  await api.connect();
  expect(connectMock).toHaveBeenCalledWith("amqp://test");
  await consume(null);
  const delivery = message('{"id":1}');
  await consume(delivery);
  expect(received).toEqual([{ id: 1 }]);
  expect(channel.ack).toHaveBeenCalledWith(delivery);
  await api.publish("order.created", { id: 2 });
  expect(channel.publish).toHaveBeenCalledWith("orders", "order.created", Buffer.from('{"id":2}'));
  expect(reported).toHaveLength(0);
});

test("invalid JSON is reported without acknowledging the message", async () => {
  await api.connect();
  await consume(message("not JSON"));
  expect(reported[0]).toBeInstanceOf(errors.BaseError);
  expect(reported[0]!.cause).toBeInstanceOf(SyntaxError);
  expect(channel.ack).not.toHaveBeenCalled();
});

test("an async message handler failure is reported before any acknowledgement", async () => {
  const original = new Error("Handler failed");
  api = await amqpEasy({
    uri: "amqp://test",
    subscribe: {
      queueName: "orders-test",
      subscriptions: [{ routingKey: "order.created", schema: z.unknown(), handler: async () => { throw original; } }],
    },
  }, (error) => { reported.push(error); });
  await api.connect();
  await consume(message("{}"));
  expect(reported[0]!.cause).toBe(original);
  expect(channel.ack).not.toHaveBeenCalled();
});

test("returned messages use their AMQP reply code", async () => {
  await api.connect();
  channel.emit("return", { fields: { replyCode: 312, replyText: "NO_ROUTE" } });
  expect(reported[0]).toBeInstanceOf(errors.NoRouteError);
});

test("disconnect closes the connection even when closing the channel fails", async () => {
  await api.connect();
  channel.close.mockRejectedValue(brokerError(504));
  await expect(api.disconnect()).rejects.toBeInstanceOf(errors.ChannelError);
  expect(connection.close).toHaveBeenCalledTimes(1);
  expect(reported).toHaveLength(1);
  await api.disconnect();
  expect(connection.close).toHaveBeenCalledTimes(1);
});

test("onError is optional and operations still reject with typed errors", async () => {
  api = await amqpEasy({ uri: "amqp://test" });
  connectMock.mockRejectedValue(brokerError(403));
  await expect(api.connect()).rejects.toBeInstanceOf(errors.AccessRefusedError);
  expect(reported).toHaveLength(0);
});

test("invalid payloads report ValidationError and never reach the handler", async () => {
  await api.connect();
  await consume(message('{"id":"wrong"}'));
  expect(received).toEqual([]);
  expect(reported[0]).toBeInstanceOf(errors.ValidationError);
  expect(reported[0]!.cause).toBeInstanceOf(z.ZodError);
  expect(channel.ack).not.toHaveBeenCalled();
  expect(channel.nack).not.toHaveBeenCalled();
});

test("async schema transforms reach the handler before acknowledgement", async () => {
  api = await amqpEasy({ uri: "amqp://test", subscribe: {
    queueName: "orders-test", subscriptions: [{ routingKey: "order.created",
      schema: z.string().transform(async value => value.length),
      handler: async value => { received.push(value); expect(channel.ack).not.toHaveBeenCalled(); },
    }],
  } }, error => { reported.push(error); });
  await api.connect();
  await consume(message('"hello"'));
  expect(received).toEqual([5]);
  expect(channel.ack).toHaveBeenCalledTimes(1);
});

test("two clients keep connections, exchanges, subscriptions and errors separate", async () => {
  const otherErrors: errors.BaseError[] = [];
  const otherReceived: unknown[] = [];
  const otherChannel = makeChannel();
  const otherConnection = makeConnection(otherChannel);
  connectMock.mockResolvedValueOnce(connection).mockResolvedValueOnce(otherConnection);
  const other = await amqpEasy({ uri: "amqp://other", exchangeName: "other", subscribe: {
    queueName: "other", subscriptions: [{ routingKey: "order.created", schema: z.string(),
      handler: value => { otherReceived.push(value); },
    }],
  } }, error => { otherErrors.push(error); });
  try {
    await api.connect();
    await other.connect();
    await channel.consume.mock.calls[0]![1](message('{"id":7}'));
    await otherChannel.consume.mock.calls[0]![1](message('"other"'));
    expect(received).toEqual([{ id: 7 }]);
    expect(otherReceived).toEqual(["other"]);
    const sameError = brokerError(404);
    channel.emit("error", sameError);
    otherChannel.emit("error", sameError);
    expect(reported).toHaveLength(1);
    expect(otherErrors).toHaveLength(1);
    await api.disconnect();
    expect(otherConnection.close).not.toHaveBeenCalled();
    await other.publish("order.created", "still connected");
    expect(otherChannel.publish).toHaveBeenCalledWith("other", "order.created", Buffer.from('"still connected"'));
  } finally {
    await other.disconnect();
  }
});

test("concurrent connect calls create one connection and disconnect is repeatable", async () => {
  await Promise.all([api.connect(), api.connect(), api.connect()]);
  expect(connectMock).toHaveBeenCalledTimes(1);
  await Promise.all([api.disconnect(), api.disconnect()]);
  await api.disconnect();
  expect(connection.close).toHaveBeenCalledTimes(1);
});

test("disconnect waits for an in-flight connection", async () => {
  let resolve!: (value: typeof connection) => void;
  connectMock.mockImplementation(() => new Promise(r => { resolve = r; }));
  const opening = api.connect();
  const closing = api.disconnect();
  resolve(connection);
  await Promise.all([opening, closing]);
  expect(channel.close).toHaveBeenCalledTimes(1);
  expect(connection.close).toHaveBeenCalledTimes(1);
});

test("all matching topic handlers run before a single ack", async () => {
  const seen: string[] = [];
  api = await amqpEasy({ uri: "amqp://test", subscribe: {
    queueName: "orders-test", subscriptions: [
      { routingKey: "order.*", schema: z.unknown(), handler: () => { seen.push("star"); } },
      { routingKey: "order.#", schema: z.unknown(), handler: () => { seen.push("hash"); } },
    ],
  } });
  await api.connect();
  await consume(message("{}"));
  expect(seen).toEqual(["star", "hash"]);
  expect(channel.ack).toHaveBeenCalledTimes(1);
});

test("direct exchanges do not interpret wildcard characters", async () => {
  api = await amqpEasy({ uri: "amqp://test", exchangeType: "direct", subscribe: {
    queueName: "orders-test", subscriptions: [{ routingKey: "order.*", schema: z.unknown(),
      handler: () => { received.push("matched"); },
    }],
  } }, error => { reported.push(error); });
  await api.connect();
  await consume(message("{}"));
  expect(received).toHaveLength(0);
  expect(reported).toHaveLength(1);
  expect(channel.nack).not.toHaveBeenCalled();
});

test("fanout subscriptions do not need routing keys", async () => {
  api = await amqpEasy({ uri: "amqp://test", exchangeType: "fanout", subscribe: {
    queueName: "orders-test", subscriptions: [{ schema: z.number(), handler: n => { received.push(n); } }],
  } });
  await api.connect();
  await consume(message("42"));
  await api.publish("ignored", 42);
  expect(received).toEqual([42]);
  expect(channel.bindQueue).toHaveBeenCalledWith("orders-test", "events", "");
  expect(channel.publish).toHaveBeenCalledWith("events", "", Buffer.from("42"));
});

test("a throwing onError does not replace the operation error", async () => {
  const log = mock(() => {});
  const originalLog = console.error;
  console.error = log;
  try {
    api = await amqpEasy({ uri: "amqp://test" }, () => { throw new Error("callback failed"); });
    connectMock.mockRejectedValue(brokerError(403));
    await expect(api.connect()).rejects.toBeInstanceOf(errors.AccessRefusedError);
    expect(log).toHaveBeenCalledTimes(1);
  } finally { console.error = originalLog; }
});
