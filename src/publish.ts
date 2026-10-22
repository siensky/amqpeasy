import { BaseError } from "./error.js";
import type { ClientState } from "./client-state.js";

/** Sends a JSON message using this client's exchange settings. */
export default async function publish(state: ClientState, routingKey: string, payload: unknown): Promise<void> {
  try {
    const channel = state.channel;
    if (!channel) throw new BaseError("Call connect() before publish()");
    const { exchangeName, exchangeType } = state.options;
    await channel.assertExchange(exchangeName, exchangeType, { durable: true });
    const json = JSON.stringify(payload);
    if (json === undefined) throw new BaseError("The payload must be JSON serializable.");
    channel.publish(exchangeName, exchangeType === "fanout" ? "" : routingKey, Buffer.from(json));
  } catch (cause) {
    throw state.reportError(cause);
  }
}
