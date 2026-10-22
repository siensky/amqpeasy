# amqpEasy

Use RabbitMQ without setting up channels or binding queues yourself. amqpEasy handles that setup for you, so you can focus on sending messages and deciding what happens when they arrive.

Create a client, add your subscriptions, and call `connect()`. You can then send messages with `publish()`.

## Install

```sh
bun add amqpeasy
```

Or with npm:

```sh
npm install amqpeasy
```

Zod is included as a dependency. Import it from `amqpeasy/zod` to define your message schemas; no separate installation is needed.

You need **Node.js 22+ or Bun**, and a running **RabbitMQ** server. This package uses ESM imports and includes TypeScript types. For Node.js projects, set `"type": "module"` in your `package.json`.

The examples below use `amqp://localhost`. Replace it with your RabbitMQ connection URL, including credentials if needed.

## Send your first message

The examples use TypeScript files run with Bun. `receiver.ts` receives messages, and `sender.ts` sends them. Start the receiver first so its queue exists before you send a message.

### 1. Receive messages

Save this as `receiver.ts`:

```ts
import amqpEasy from "amqpeasy";
import { z } from "amqpeasy/zod";

const client = await amqpEasy({
  uri: "amqp://localhost",
  exchangeName: "orders",
  subscribe: {
    queueName: "orders-worker",
    subscriptions: [
      {
        routingKey: "order.created",
        schema: z.object({
          id: z.string(),
          total: z.number().nonnegative(),
        }),
        async handler(order) {
          console.log("Order received:", order.id, order.total);
        },
      },
    ],
  },
});

await client.connect();
console.log("Waiting for orders...");
```

Run it and leave the terminal open:

```sh
bun receiver.ts
```

### 2. Send a message

Save this as `sender.ts`:

```ts
import amqpEasy from "amqpeasy";

const client = await amqpEasy({
  uri: "amqp://localhost",
  exchangeName: "orders",
});

await client.connect();

try {
  await client.publish("order.created", { id: "order-123", total: 99 });
} finally {
  await client.disconnect();
}
```

Run it in a second terminal:

```sh
bun sender.ts
```

The receiver prints:

```text
Order received: order-123 99
```

For a JavaScript project using Node.js, save these two examples as `.mjs` files and run them with `node`. Their code works unchanged.

This example uses a temporary queue that is deleted when the receiver disconnects. To keep the queue and retry failed messages, enable [retries and dead letters](#retries-and-dead-letters).

## Client API

```ts
const client = await amqpEasy(options, onError);
```

Creating a client does not open a connection. Call `connect()` when you are ready.

| Method | What it does |
| --- | --- |
| `await client.connect()` | Opens the connection and starts any configured subscriptions. |
| `await client.publish(routingKey, payload)` | Sends a JSON message to the client's exchange. |
| `await client.disconnect()` | Closes the client's channels and connection. |

One client can both publish and receive messages. You can also create separate clients; disconnecting one does not affect the others. Repeated calls to `connect()` reuse an open connection.

### Options

| Option | Default | Description |
| --- | --- | --- |
| `uri` | Required | RabbitMQ connection URL. |
| `exchangeName` | `"events"` | Exchange used for publishing and subscriptions. |
| `exchangeType` | `"topic"` | `"topic"`, `"direct"`, or `"fanout"`. |
| `subscribe` | Omitted | Configure this when the client should receive messages. |
| `subscribe.queueName` | Required when subscribing | Queue to consume from. Use `""` for a generated name when dead letters are disabled. |
| `subscribe.subscriptions` | Required when subscribing | An array of routing keys, Zod schemas, and handlers. |
| `subscribe.deadLetter` | `false` | Enable retries and a dead-letter queue with `true` or a settings object. |

Subscriptions are configured when you create the client. There is no separate `client.subscribe()` method.

## Validate messages with Zod

Each subscription needs a schema. Incoming JSON is checked against that schema before its handler runs.

In TypeScript, the handler's parameter type is inferred automatically:

```ts
import amqpEasy from "amqpeasy";
import { z } from "amqpeasy/zod";

const Order = z.object({
  id: z.string(),
  total: z.number().nonnegative(),
});

const client = await amqpEasy({
  uri: "amqp://localhost",
  subscribe: {
    queueName: "orders-worker",
    subscriptions: [
      {
        routingKey: "order.created",
        schema: Order,
        async handler(order) {
          // TypeScript knows id is a string and total is a number.
          console.log(order.id, order.total);
        },
      },
    ],
  },
});
```

If you need the same type elsewhere, derive it from the schema:

```ts
type OrderPayload = z.infer<typeof Order>;
```

TypeScript types disappear at runtime, so a type alone cannot validate a message received from another application. The schema gives you both runtime validation and TypeScript types.

Async refinements and transformations are supported. The handler receives the schema's output, including any transformations. Use `z.unknown()` if you deliberately want to accept any JSON value.

Validation happens when receiving. `publish()` accepts JSON-serializable data and does not validate it against your subscriptions.

## Choose how messages are routed

An **exchange** routes messages to **queues**. A **routing key** describes the message, such as `order.created`. Each subscription tells amqpEasy which messages its handler should process.

| Exchange type | Subscription key | Receives |
| --- | --- | --- |
| `topic` (default) | `order.*` | `order.created` or `order.cancelled`, but not `order.eu.created`. |
| `topic` | `order.#` | `order`, `order.created`, and `order.eu.created`. |
| `direct` | `order.created` | Only the exact key `order.created`. |
| `fanout` | Not needed | Every message sent to the exchange. |

For topics, `*` matches one dot-separated word; `#` matches zero or more words. For direct exchanges, these characters have no special meaning.

Set `exchangeType` on both the sender and receiver. Clients using the same exchange name must agree on its type. Exchanges are declared as durable.

### Broadcast with fanout

```ts
const client = await amqpEasy({
  uri: "amqp://localhost",
  exchangeName: "announcements",
  exchangeType: "fanout",
  subscribe: {
    queueName: "announcements-email",
    subscriptions: [
      {
        schema: z.object({ text: z.string() }),
        handler(message) {
          console.log(message.text);
        },
      },
    ],
  },
});

await client.connect();
await client.publish("", { text: "Maintenance starts at 22:00." });
```

This snippet uses the `amqpEasy` and `z` imports shown above. With fanout, omit the subscription's routing key and pass `""` when publishing.

Use **different queue names** when each receiver needs its own copy. With dead letters enabled, workers can use the **same queue name** to share the work; each message goes to one worker on that queue. Workers sharing a queue should use the same subscriptions and settings.

Without dead letters, a queue is exclusive to its connection and cannot be shared by multiple clients.

## Retries and dead letters

Enable automatic retries by adding `deadLetter: true` inside `subscribe`:

```ts
subscribe: {
  queueName: "orders-worker",
  deadLetter: true,
  subscriptions: [/* your subscriptions */],
}
```

No manual queue setup is needed. On connection, amqpEasy creates:

| Queue | Purpose |
| --- | --- |
| `orders-worker` | Receives messages for your handlers. |
| `orders-worker.retry` | Holds failed messages until their next attempt. |
| `orders-worker.dead` | Stores messages that cannot be processed. |

By default, a handler gets **three retries, five seconds apart**: up to four attempts including the first one.

To change that, use an object instead of `true`:

```ts
deadLetter: {
  maxRetries: 3,
  retryDelayMs: 5000,
}
```

`maxRetries` must be a non-negative integer. `retryDelayMs` must be a positive integer in milliseconds. Both are limited to 2,147,483,647. Set `maxRetries: 0` to send handler failures straight to `.dead`; no retry queue is created.

### What happens when processing fails?

| Failure | With dead letters enabled |
| --- | --- |
| Invalid JSON | Sent straight to `.dead`. |
| Payload fails its Zod schema | Sent straight to `.dead`; that handler is not called. |
| No subscription matches | Sent straight to `.dead`. |
| Handler throws or rejects | Retried, then sent to `.dead` if every attempt fails. |

Return normally when processing succeeds. Throw an error, or let your async function reject, when processing should be retried.

Retries return only to the queue that failed, including when using fanout. The delay is managed by RabbitMQ, so a scheduled retry survives the client disconnecting. Messages in `.dead` stay there for you to inspect or replay; amqpEasy does not consume them automatically.

Failed messages retain their original body. Reserved `x-amqpeasy-*` headers record the original routing key, retry count, error details, and failure time. Failure copies are persistent; the original expiration and user ID are cleared.

### Queue requirements

This mode creates **durable quorum queues** and requires RabbitMQ support for at-least-once dead lettering. The integration tests use RabbitMQ 4. Queue names must be stable and non-empty, and queues remain after `disconnect()`.

An existing temporary queue cannot be converted to a quorum queue in place. Use a new queue name when enabling this mode for an existing application. Changing the delay on an existing retry queue also requires a queue migration. amqpEasy never deletes existing queues for you.

If dead letters are disabled, failed messages are reported through `onError` and left unacknowledged. They are not automatically retried, and the exclusive queue is deleted when its connection closes.

## Handle errors

Pass an error callback as the second argument to `amqpEasy`. Import error classes from `amqpeasy/errors`:

```ts
import amqpEasy from "amqpeasy";
import { AccessRefusedError, ValidationError } from "amqpeasy/errors";

const client = await amqpEasy(
  { uri: "amqp://localhost" },
  (error) => {
    if (error instanceof ValidationError) {
      console.error("Invalid message:", error.cause);
    } else if (error instanceof AccessRefusedError) {
      console.error("RabbitMQ denied access:", error.message);
    } else {
      console.error(error.code, error.message);
    }
  },
);

try {
  await client.connect();
} catch (error) {
  // onError has already been notified. Decide how your app should respond.
  console.error("Could not start the client.");
}
```

All library errors extend `BaseError`, which extends `Error`. They expose `message`, `code` when available, and `cause` when an underlying error is available.

The callback reports failures; it does not swallow them. Failed `connect()`, `publish()`, and `disconnect()` calls still reject and should be handled by your application. Without a callback, background errors are logged to the console.

`onError` can be async, but the library does not wait for it. Errors thrown by the callback are logged without replacing the original failure.

<details>
<summary>Available error classes</summary>

| Code | Class |
| --- | --- |
| 311 | `ContentTooLargeError` |
| 312 | `NoRouteError` |
| 313 | `NoConsumersError` |
| 320 | `ConnectionForcedError` |
| 402 | `InvalidPathError` |
| 403 | `AccessRefusedError` |
| 404 | `NotFoundError` |
| 405 | `ResourceLockedError` |
| 406 | `PreconditionFailedError` |
| 501 | `FrameError` |
| 502 | `AmqpSyntaxError` |
| 503 | `CommandInvalidError` |
| 504 | `ChannelError` |
| 505 | `UnexpectedFrameError` |
| 506 | `ResourceError` |
| 530 | `NotAllowedError` |
| 540 | `NotImplementedError` |
| 541 | `InternalError` |
| `VALIDATION_ERROR` | `ValidationError` |
| `FAILURE_TRANSFER_ERROR` | `FailureTransferError` |

AMQP classes are selected when the underlying error provides a matching numeric code. Other failures, including system errors such as `ECONNREFUSED`, use `BaseError`.

</details>

## Delivery and shutdown

Keep these behaviors in mind when using the library:

- **Acknowledgements are automatic.** A message is acknowledged after all matching handlers succeed. Matching handlers run in subscription order for each message; separate messages may be processed concurrently.
- **Handlers may run more than once.** Make repeated processing safe, for example by recording an order ID before charging for an order. If several handlers match and a later one fails, a retry runs all matching handlers again.
- **Publishing does not confirm delivery.** Normal `publish()` calls do not use publisher confirms, persistent messages, or mandatory routing. A resolved promise does not prove that RabbitMQ stored the message or that a consumer processed it. Messages without a matching queue may be discarded.
- **Failure transfers are confirmed.** Retry and dead-letter copies use publisher confirms and mandatory routing before the original is acknowledged. A failed transfer reports `FailureTransferError` and leaves the original unacknowledged. A connection failure between copying and acknowledging can still produce duplicates.
- **Reconnection is manual.** The client does not reconnect automatically after a lost connection.
- **Disconnect closes the connection.** Call `await client.disconnect()` during application shutdown. It does not wait for running handlers to finish, so coordinate ongoing work before disconnecting.

Durable queues alone do not make normal published messages persistent across broker restarts.

## Development

The repository uses Bun for development:

```sh
bun install --frozen-lockfile
bun run typecheck
bun run test
bun run build
bun run test:package
```

For integration tests, point `RABBITMQ_URL` at a dedicated test broker:

```sh
RABBITMQ_URL=amqp://user:password@localhost:5672 bun run test:integration
RABBITMQ_URL=amqp://user:password@localhost:5672 bun run test:package
```

Tests cover routing, validation, isolated clients, errors, retries, and dead letters. The package check installs a local tarball into a temporary project and checks its imports and types in Node and Bun. With `RABBITMQ_URL`, it also tests message delivery through the installed package. It does not publish anything.

GitHub Actions runs checks on pull requests and pushes to `main` or `master`, using Node 22 and 24, Bun, and RabbitMQ. The release workflow runs on `v*` tags, verifies the version, and publishes the tested tarball after CI passes. It requires npm Trusted Publishing to be configured for `siensky/amqpeasy`, workflow `release.yml`, and environment `npm`. See [npm's setup instructions](https://docs.npmjs.com/trusted-publishers/).

## License

[MIT](./LICENSE) © 2026 siensky
