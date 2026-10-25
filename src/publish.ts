import { randomUUID } from "node:crypto";
import type { ConfirmChannel } from "amqplib";
import { BaseError, PublishTimeoutError } from "./error.js";
import type { ClientState } from "./client-state.js";

export const publicationHeader = "x-amqpeasy-publication-id";

/** Sends JSON and, when enabled, waits for a routed broker confirmation. Never retries. */
export default async function publish(state: ClientState, routingKey: string, payload: unknown): Promise<void> {
  try {
    if (state.status !== "connected") throw new BaseError("Call connect() before publish(); the client must be connected.");
    const { exchangeName, exchangeType, publish: options } = state.options;
    const channel = options.confirms ? state.publisher : state.channel;
    if (!channel) throw new BaseError("The publishing channel is closed.");
    const json = JSON.stringify(payload);
    if (json === undefined) throw new BaseError("The payload must be JSON serializable.");
    const pending = state.pendingPublications;
    const publications = state.publications;
    const id = randomUUID();
    const operation = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => finish(new PublishTimeoutError()), options.timeoutMs);
      function finish(cause?: unknown) {
        if (!pending.delete(id)) return;
        clearTimeout(timer);
        if (cause) reject(cause); else resolve();
      }
      pending.set(id, finish);
      // Include exchange setup in the timeout, and never send after the deadline.
      void (async () => {
        await channel.assertExchange(exchangeName, exchangeType, { durable: true });
        if (!pending.has(id)) return;
        const key = exchangeType === "fanout" ? "" : routingKey;
        const body = Buffer.from(json);
        if (options.confirms) {
          (channel as ConfirmChannel).publish(exchangeName, key, body, {
            persistent: options.persistent, mandatory: options.mandatory,
            headers: { [publicationHeader]: id },
          }, finish);
        } else {
          if (options.persistent) channel.publish(exchangeName, key, body, { persistent: true });
          else channel.publish(exchangeName, key, body);
          finish();
        }
      })().catch(finish);
    });
    publications.add(operation);
    try { await operation; } finally { publications.delete(operation); }
  } catch (cause) {
    throw state.reportError(cause);
  }
}
