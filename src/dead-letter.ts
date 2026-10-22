import { randomUUID } from "node:crypto";
import type { Channel, ConsumeMessage, Message, Options } from "amqplib";
import { BaseError, FailureTransferError } from "./error.js";
import type { ClientState } from "./client-state.js";
import type { DeadLetterOptions } from "./types.js";

const headerPrefix = "x-amqpeasy-";
const transferHeader = `${headerPrefix}transfer-id`;
const queueHeader = `${headerPrefix}queue`;
const retryHeader = `${headerPrefix}retry-count`;
const routingHeader = `${headerPrefix}routing-key`;

/** Validates the opt-in settings before any RabbitMQ resources are created. */
export function resolveDeadLetter(
  queueName: string,
  value?: boolean | DeadLetterOptions,
): Required<DeadLetterOptions> | undefined {
  if (value === undefined || value === false) return undefined;
  if (value !== true && (typeof value !== "object" || value === null)) {
    throw new BaseError("deadLetter must be true, false, or retry settings.");
  }
  if (!queueName || !queueName.trim() || Buffer.byteLength(`${queueName}.retry`) > 255) {
    throw new BaseError("deadLetter needs a stable queueName with room for the .retry suffix (255 bytes total).");
  }
  const options = value === true ? {} : value;
  const maxRetries = options.maxRetries ?? 3;
  const retryDelayMs = options.retryDelayMs ?? 5000;
  if (!Number.isSafeInteger(maxRetries) || maxRetries < 0 || maxRetries > 2147483647) {
    throw new BaseError("maxRetries must be an integer between 0 and 2147483647.");
  }
  if (!Number.isSafeInteger(retryDelayMs) || retryDelayMs < 1 || retryDelayMs > 2147483647) {
    throw new BaseError("retryDelayMs must be an integer between 1 and 2147483647.");
  }
  return { maxRetries, retryDelayMs };
}

function retryCount(message: ConsumeMessage, queueName: string): number {
  const headers = message.properties?.headers;
  if (headers?.[queueHeader] !== queueName) return 0;
  const count: unknown = headers[retryHeader];
  return typeof count === "number" && Number.isSafeInteger(count) && count >= 0 ? count : 0;
}

/** Retry deliveries use the original routing key, even for fanout exchanges. */
export function messageRoutingKey(message: ConsumeMessage, queueName: string): string {
  const headers = message.properties?.headers;
  const routingKey: unknown = headers?.[routingHeader];
  return headers?.[queueHeader] === queueName && typeof routingKey === "string"
    ? routingKey
    : message.fields.routingKey;
}

/** Uses quorum queues so broker-side delayed forwarding waits for acceptance. */
function queueOptions(target?: string, ttl?: number): Options.AssertQueue {
  return {
    durable: true,
    exclusive: false,
    autoDelete: false,
    arguments: {
      "x-queue-type": "quorum",
      ...(target === undefined ? {} : {
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": target,
        "x-dead-letter-strategy": "at-least-once",
        "x-overflow": "reject-publish",
      }),
      ...(ttl === undefined ? {} : { "x-message-ttl": ttl }),
    },
  };
}

export type FailureHandler = (
  message: ConsumeMessage,
  error: BaseError,
  retryable: boolean,
) => Promise<void>;

/** Creates durable work, retry, and dead-letter queues for one consumer. */
export async function setupDeadLetter(state: ClientState, channel: Channel): Promise<FailureHandler> {
  const subscription = state.options.subscribe;
  const settings = subscription?.deadLetter;
  if (!subscription || !settings || !state.connection) throw new BaseError("Dead-letter settings require a connected consumer.");
  const { queueName } = subscription;
  const deadQueue = `${queueName}.dead`;
  const retryQueue = `${queueName}.retry`;
  await channel.assertQueue(deadQueue, queueOptions());
  await channel.assertQueue(queueName, queueOptions(deadQueue));
  if (settings.maxRetries > 0) {
    await channel.assertQueue(retryQueue, queueOptions(queueName, settings.retryDelayMs));
  }

  const publisher = await state.connection.createConfirmChannel();
  state.failureChannel = publisher;
  const pending = new Map<string, (cause?: unknown) => void>();
  publisher.on("error", (error) => { state.reportError(error, true); });
  publisher.on("close", () => {
    if (state.failureChannel === publisher) state.failureChannel = null;
    for (const finish of [...pending.values()]) finish(new Error("The failure-publishing channel closed."));
  });
  publisher.on("return", (returned: Message) => {
    const id: unknown = returned.properties?.headers?.[transferHeader];
    if (typeof id === "string") pending.get(id)?.(new Error("The retry or dead-letter queue could not be routed to."));
  });

  function move(message: ConsumeMessage, destination: string, error: BaseError, count: number): Promise<void> {
    return new Promise((resolve, reject) => {
      const id = randomUUID();
      const timer = setTimeout(() => finish(new Error("Timed out waiting for a failed-message publisher confirm.")), 30000);
      function finish(cause?: unknown) {
        if (!pending.delete(id)) return;
        clearTimeout(timer);
        if (cause) reject(new FailureTransferError(cause));
        else resolve();
      }
      pending.set(id, finish);
      const properties: Options.Publish = {
        ...message.properties,
        // Expiry and validated userId must not accidentally discard/reject the copy.
        expiration: undefined,
        userId: undefined,
        persistent: true,
        mandatory: true,
        headers: {
          ...message.properties?.headers,
          [transferHeader]: id,
          [queueHeader]: queueName,
          [retryHeader]: count,
          [routingHeader]: messageRoutingKey(message, queueName),
          [`${headerPrefix}error-name`]: error.name,
          [`${headerPrefix}error-message`]: error.message,
          [`${headerPrefix}error-code`]: error.code ?? "",
          [`${headerPrefix}failed-at`]: new Date().toISOString(),
        },
      };
      try {
        publisher.publish("", destination, message.content, properties, cause => finish(cause));
      } catch (cause) {
        finish(cause);
      }
    });
  }

  return async (message, error, retryable) => {
    const count = retryCount(message, queueName);
    const retry = retryable && count < settings.maxRetries;
    await move(message, retry ? retryQueue : deadQueue, error, retry ? count + 1 : count);
    // Only release the original after the broker confirms the routed copy.
    channel.ack(message);
  };
}
