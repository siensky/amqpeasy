import { expect, test } from "bun:test";
import net from "node:net";
import amqp from "amqplib";
import { z } from "zod";
import easy from "../../src/public.js";
import { NoRouteError, ShutdownTimeoutError } from "../../src/error.js";

const uri = process.env.RABBITMQ_URL;
const integration = test.skipIf(!uri);
const unique = () => `amqpeasy.reliability.${crypto.randomUUID()}`;
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(r => { resolve = r; });
  return { promise, resolve };
}
async function until(predicate: () => boolean) {
  const deadline = Date.now() + 5000;
  while (!predicate()) { if (Date.now() > deadline) throw Error("Timed out"); await sleep(10); }
}

integration("mandatory publishing rejects a missing route and stores routed messages persistently", async () => {
  const name = unique(); const admin = await amqp.connect(uri!); const channel = await admin.createChannel();
  const client = await easy({ uri: uri!, exchangeName: name, publish: { mandatory: true, persistent: true } }, () => {});
  try {
    await client.connect();
    await expect(client.publish("job", 1)).rejects.toBeInstanceOf(NoRouteError);
    await channel.assertQueue(name, { durable: true }); await channel.bindQueue(name, name, "job");
    await client.publish("job", 2);
    const message = await channel.get(name, { noAck: true });
    expect(message && message.properties.deliveryMode).toBe(2);
    expect(message && message.content.toString()).toBe("2");
  } finally {
    await client.disconnect(); await channel.deleteQueue(name); await channel.deleteExchange(name); await admin.close();
  }
});

integration("prefetch limits work to one message and shutdown waits for the active handler", async () => {
  const name = unique(); const admin = await amqp.connect(uri!); const channel = await admin.createChannel();
  const gate = deferred(); let calls = 0; let finished = false;
  const client = await easy({ uri: uri!, exchangeName: name, publish: { mandatory: true }, subscribe: {
    queueName: name, durable: true, prefetch: 1,
    subscriptions: [{ routingKey: "job", schema: z.number(), handler: async () => { calls++; await gate.promise; finished = true; } }],
  } }, () => {});
  try {
    await client.connect(); await client.publish("job", 1); await client.publish("job", 2);
    await until(() => calls === 1); await sleep(100); expect(calls).toBe(1);
    let closed = false; const closing = client.disconnect().then(() => { closed = true; });
    await sleep(100); expect(closed).toBe(false); expect(finished).toBe(false);
    gate.resolve(); await closing;
    expect(finished).toBe(true); expect(calls).toBe(1);
    const remaining = await channel.get(name, { noAck: true });
    expect(remaining && remaining.content.toString()).toBe("2");
  } finally {
    gate.resolve(); await client.disconnect(); await channel.deleteQueue(name); await channel.deleteExchange(name); await admin.close();
  }
});

integration("shutdown timeout leaves unfinished work available for redelivery", async () => {
  const name = unique(); const gate = deferred(); let started = false; let signal!: AbortSignal;
  const admin = await amqp.connect(uri!); const channel = await admin.createChannel();
  const client = await easy({ uri: uri!, exchangeName: name, publish: { mandatory: true, persistent: true }, subscribe: {
    queueName: name, durable: true, prefetch: 1,
    subscriptions: [{ routingKey: "job", schema: z.number(), handler: async (_value, context) => {
      signal = context.signal; started = true; await gate.promise;
    } }],
  } }, () => {});
  try {
    await client.connect(); await client.publish("job", 1); await until(() => started);
    await expect(client.disconnect({ timeoutMs: 50 })).rejects.toBeInstanceOf(ShutdownTimeoutError);
    expect(signal.aborted).toBe(true);
    let delivery: amqp.GetMessage | false = false;
    for (let i = 0; i < 100 && !delivery; i++) { delivery = await channel.get(name, { noAck: true }); if (!delivery) await sleep(20); }
    expect(delivery && delivery.fields.redelivered).toBe(true);
    gate.resolve(); await sleep(20);
  } finally {
    gate.resolve(); await client.disconnect(); await channel.deleteQueue(name); await channel.deleteExchange(name); await admin.close();
  }
});

integration("a connection dropped at basic.publish rejects instead of claiming success", async () => {
  const target = new URL(uri!); const sockets = new Set<net.Socket>(); let dropped = false;
  const proxy = net.createServer(client => {
    const upstream = net.connect(Number(target.port || 5672), target.hostname);
    sockets.add(client); sockets.add(upstream);
    client.on("error", () => {}); upstream.on("error", () => {}); upstream.pipe(client);
    let buffer = Buffer.alloc(0), handshake = true;
    client.on("data", data => {
      buffer = Buffer.concat([buffer, Buffer.from(data)]);
      if (handshake) {
        if (buffer.length < 8) return;
        upstream.write(buffer.subarray(0, 8)); buffer = buffer.subarray(8); handshake = false;
      }
      while (buffer.length >= 7) {
        const size = buffer.readUInt32BE(3) + 8; if (buffer.length < size) return;
        const frame = buffer.subarray(0, size); buffer = buffer.subarray(size);
        if (frame[0] === 1 && size >= 12 && frame.readUInt16BE(7) === 60 && frame.readUInt16BE(9) === 40) {
          dropped = true; client.destroy(); upstream.destroy(); return;
        }
        upstream.write(frame);
      }
    });
  });
  await new Promise<void>(r => proxy.listen(0, "127.0.0.1", r));
  const address = proxy.address() as net.AddressInfo;
  const proxied = new URL(uri!); proxied.hostname = "127.0.0.1"; proxied.port = String(address.port);
  const name = unique(); const client = await easy({ uri: proxied.toString(), exchangeName: name, publish: { confirms: true } }, () => {});
  try {
    await client.connect(); await expect(client.publish("job", 1)).rejects.toBeInstanceOf(Error);
    expect(dropped).toBe(true); expect(client.status).toBe("disconnected");
  } finally {
    await client.disconnect(); sockets.forEach(socket => socket.destroy());
    await new Promise<void>(r => proxy.close(() => r()));
    const admin = await amqp.connect(uri!); const channel = await admin.createChannel(); await channel.deleteExchange(name); await admin.close();
  }
});

// Explicit opt-in: this restarts only the dedicated container named by the test runner.
test.skipIf(!uri || !process.env.RABBITMQ_TEST_CONTAINER)("broker restart preserves waiting jobs and manual reconnect restores one consumer and prefetch", async () => {
  const name = unique(); const gate = deferred(); let calls = 0;
  let admin = await amqp.connect(uri!); let channel = await admin.createChannel();
  admin.on("error", () => {}); channel.on("error", () => {});
  // Queue jobs before starting a consumer, so both must survive the restart.
  await channel.assertExchange(name, "topic", { durable: true });
  await channel.assertQueue(name, { durable: true }); await channel.bindQueue(name, name, "job");
  const publisher = await easy({ uri: uri!, exchangeName: name, publish: { confirms: true, mandatory: true, persistent: true } }, () => {});
  const consumer = await easy({ uri: uri!, exchangeName: name, subscribe: { queueName: name, durable: true, prefetch: 1,
    subscriptions: [{ routingKey: "job", schema: z.number(), handler: async () => { calls++; await gate.promise; } }],
  } }, () => {});
  try {
    await publisher.connect(); await publisher.publish("job", 1); await publisher.publish("job", 2);
    const restart = Bun.spawn(["docker", "restart", process.env.RABBITMQ_TEST_CONTAINER!], { stdout: "ignore", stderr: "inherit" });
    expect(await restart.exited).toBe(0);
    await until(() => publisher.status === "disconnected");
    let ready = false;
    for (let i = 0; i < 150; i++) {
      try { admin = await amqp.connect(uri!); ready = true; break; } catch { await sleep(200); }
    }
    expect(ready).toBe(true); channel = await admin.createChannel();
    expect((await channel.checkQueue(name)).messageCount).toBe(2);
    await Promise.all([publisher.connect(), publisher.connect()]);
    await Promise.all([consumer.connect(), consumer.connect()]);
    await until(() => calls === 1); await sleep(100); expect(calls).toBe(1);
    expect((await channel.checkQueue(name)).consumerCount).toBe(1);
    gate.resolve(); await until(() => calls === 2);
    await publisher.publish("job", 3); await until(() => calls === 3);
    // A second restart checks restoration of an already active consumer.
    const again = Bun.spawn(["docker", "restart", process.env.RABBITMQ_TEST_CONTAINER!], { stdout: "ignore", stderr: "inherit" });
    admin.on("error", () => {}); channel.on("error", () => {}); expect(await again.exited).toBe(0);
    await until(() => consumer.status === "disconnected");
    for (let i = 0; i < 150; i++) { try { admin = await amqp.connect(uri!); break; } catch { await sleep(200); } }
    channel = await admin.createChannel();
    await Promise.all([consumer.connect(), consumer.connect()]); await publisher.connect();
    expect((await channel.checkQueue(name)).consumerCount).toBe(1);
    await publisher.publish("job", 4); await until(() => calls === 4);
  } finally {
    gate.resolve(); await consumer.disconnect(); await publisher.disconnect();
    await channel.deleteQueue(name); await channel.deleteExchange(name); await admin.close();
  }
}, 60000);
