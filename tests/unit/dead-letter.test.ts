import { expect, mock, test } from "bun:test";
import { EventEmitter } from "node:events";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import { BaseError, FailureTransferError, ValidationError } from "../../src/error.js";
import { messageRoutingKey, resolveDeadLetter, setupDeadLetter } from "../../src/dead-letter.js";
import { consumeEvents } from "../../src/consumer.js";
import type { ClientState } from "../../src/client-state.js";

function fixture(maxRetries = 3) {
  const confirms: Array<(cause?: unknown) => void> = [];
  const publisher = Object.assign(new EventEmitter(), {
    publish: mock((_exchange: string, _key: string, _content: Buffer, _properties: Options.Publish, confirm: (cause?: unknown) => void) => {
      confirms.push(confirm); return true;
    }),
    close: mock(async () => {}),
  });
  const channel = {
    assertQueue: mock(async (queue: string, _options: Options.AssertQueue) => ({ queue })),
    ack: mock((_message: ConsumeMessage) => {}),
  };
  const reports: BaseError[] = [];
  const state = {
    options: { uri: "amqp://test", exchangeName: "events", exchangeType: "topic", subscribe: {
      queueName: "orders", deadLetter: { maxRetries, retryDelayMs: 100 }, subscriptions: [],
    } },
    channel, failureChannel: null,
    connection: { createConfirmChannel: async () => publisher },
    reportError(cause: unknown) {
      const error = cause instanceof BaseError ? cause : new BaseError(String(cause), undefined, cause);
      reports.push(error); return error;
    },
  } as unknown as ClientState;
  return { publisher, channel, state, confirms, reports };
}

function message(retries = 0): ConsumeMessage {
  return {
    content: Buffer.from('{"id":1}'),
    fields: { routingKey: retries ? "orders" : "order.created" },
    properties: { messageId: "original-id", expiration: "10", headers: {
      custom: "kept",
      ...(retries ? {
        "x-amqpeasy-queue": "orders", "x-amqpeasy-retry-count": retries,
        "x-amqpeasy-routing-key": "order.created",
      } : {}),
    } },
  } as unknown as ConsumeMessage;
}

test("dead-letter is opt-in and validates bounded retry settings", () => {
  expect(resolveDeadLetter("", false)).toBeUndefined();
  expect(resolveDeadLetter("orders", true)).toEqual({ maxRetries: 3, retryDelayMs: 5000 });
  expect(resolveDeadLetter("orders", { maxRetries: 0 })).toEqual({ maxRetries: 0, retryDelayMs: 5000 });
  expect(() => resolveDeadLetter("", true)).toThrow(BaseError);
  expect(() => resolveDeadLetter("orders", { maxRetries: -1 })).toThrow(BaseError);
  expect(() => resolveDeadLetter("orders", { retryDelayMs: 0 })).toThrow(BaseError);
  expect(() => resolveDeadLetter("orders", { maxRetries: Infinity })).toThrow(BaseError);
  expect(() => resolveDeadLetter("å".repeat(130), true)).toThrow(BaseError);
});

test("setup creates durable quorum queues and safe delayed forwarding", async () => {
  const f = fixture();
  await setupDeadLetter(f.state, f.channel as unknown as Channel);
  expect(f.channel.assertQueue).toHaveBeenCalledWith("orders.dead", expect.objectContaining({ durable: true, exclusive: false }));
  expect(f.channel.assertQueue).toHaveBeenCalledWith("orders.retry", expect.objectContaining({ arguments: expect.objectContaining({
    "x-queue-type": "quorum", "x-message-ttl": 100,
    "x-dead-letter-routing-key": "orders", "x-dead-letter-strategy": "at-least-once",
  }) }));
});

test("retry is confirmed before acknowledging and preserves the original message", async () => {
  const f = fixture();
  const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
  const original = message();
  const moved = handle(original, new BaseError("Temporary error"), true);
  expect(f.channel.ack).not.toHaveBeenCalled();
  const call = f.publisher.publish.mock.calls[0]!;
  expect(call[0]).toBe(""); expect(call[1]).toBe("orders.retry"); expect(call[2]).toBe(original.content);
  expect(call[3]).toMatchObject({ messageId: "original-id", persistent: true, mandatory: true, headers: {
    custom: "kept", "x-amqpeasy-retry-count": 1, "x-amqpeasy-routing-key": "order.created",
  } });
  expect(call[3].expiration).toBeUndefined();
  f.confirms[0]!(); await moved;
  expect(f.channel.ack).toHaveBeenCalledWith(original);
});

test("permanent failures and exhausted retries go straight to the dead queue", async () => {
  for (const [retryable, count] of [[false, 0], [true, 3]] as const) {
    const f = fixture();
    const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
    const moved = handle(message(count), new BaseError("Failure"), retryable);
    expect(f.publisher.publish.mock.calls[0]![1]).toBe("orders.dead");
    f.confirms[0]!(); await moved;
  }
});

test("zero retries creates no retry queue and sends handler failures to dead-letter", async () => {
  const f = fixture(0);
  const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
  expect(f.channel.assertQueue.mock.calls.map(c => c[0])).toEqual(["orders.dead", "orders"]);
  const moved = handle(message(), new BaseError("Failure"), true);
  expect(f.publisher.publish.mock.calls[0]![1]).toBe("orders.dead");
  f.confirms[0]!(); await moved;
});

test("a negative confirm never acknowledges the original", async () => {
  const f = fixture();
  const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
  const moved = handle(message(), new BaseError("Failure"), true);
  queueMicrotask(() => f.confirms[0]!(new Error("Broker nack")));
  await expect(moved).rejects.toBeInstanceOf(FailureTransferError);
  expect(f.channel.ack).not.toHaveBeenCalled();
});

test("a mandatory return rejects the transfer even if the broker also confirms", async () => {
  const f = fixture();
  const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
  const moved = handle(message(), new BaseError("Failure"), false);
  const properties = f.publisher.publish.mock.calls[0]![3];
  queueMicrotask(() => { f.publisher.emit("return", { properties }); f.confirms[0]!(); });
  await expect(moved).rejects.toBeInstanceOf(FailureTransferError);
  expect(f.channel.ack).not.toHaveBeenCalled();
});

test("a closed confirm channel rejects pending transfers", async () => {
  const f = fixture();
  const handle = await setupDeadLetter(f.state, f.channel as unknown as Channel);
  const moved = handle(message(), new BaseError("Failure"), true);
  queueMicrotask(() => f.publisher.emit("close"));
  await expect(moved).rejects.toBeInstanceOf(FailureTransferError);
  expect(f.channel.ack).not.toHaveBeenCalled();
});

test("retry routing keys are only restored for their own queue", () => {
  expect(messageRoutingKey(message(1), "orders")).toBe("order.created");
  expect(messageRoutingKey(message(1), "other")).toBe("orders");
});

test("consumer classifies bad JSON, schema failures, and handler failures", async () => {
  for (const scenario of ["json", "schema", "handler"] as const) {
    const f = fixture();
    let consume!: (message: ConsumeMessage | null) => Promise<void>;
    f.state.options.subscribe!.subscriptions = [{ routingKey: "order.created", async handle() {
      if (scenario === "schema") throw new ValidationError();
      if (scenario === "handler") throw new Error("Temporary error");
    } }];
    const channel = Object.assign(f.channel, { consume: async (_queue: string, handler: typeof consume) => { consume = handler; } });
    const failure = mock(async () => {});
    await consumeEvents(f.state, channel as unknown as Channel, "orders", failure);
    const delivery = message(); if (scenario === "json") delivery.content = Buffer.from("not json");
    await consume(delivery);
    expect(failure).toHaveBeenCalledWith(delivery, expect.any(BaseError), scenario === "handler");
    expect(f.channel.ack).not.toHaveBeenCalled();
  }
});
