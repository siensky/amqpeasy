import amqp from "amqplib";
import startConsumer from "./consumer.js";
import type { BaseError } from "./error.js";
import type { ClientState } from "./client-state.js";

/** Creates connection functions for one client. */
export function createConnection(state: ClientState) {
  let connecting: Promise<void> | undefined;
  let disconnecting: Promise<void> | undefined;

  async function closeResources(): Promise<void> {
    const { channel, connection, failureChannel } = state;
    state.channel = null;
    state.connection = null;
    state.failureChannel = null;
    let failure: BaseError | undefined;
    try {
      if (channel) await channel.close();
    } catch (cause) {
      failure = state.reportError(cause);
    } finally {
      try {
        if (failureChannel) await failureChannel.close();
      } catch (cause) {
        const error = state.reportError(cause);
        failure ??= error;
      }
      try {
        if (connection) await connection.close();
      } catch (cause) {
        const error = state.reportError(cause);
        failure ??= error;
      }
    }
    if (failure) throw failure;
  }

  async function open(): Promise<void> {
    try {
      const connection = await amqp.connect(state.options.uri);
      state.connection = connection;
      connection.on("error", (error) => { state.reportError(error, true); });
      connection.on("close", () => {
        if (state.connection === connection) {
          state.connection = null;
          state.channel = null;
          state.failureChannel = null;
        }
      });
      const channel = await connection.createChannel();
      state.channel = channel;
      channel.on("error", (error) => { state.reportError(error, true); });
      channel.on("close", () => {
        if (state.channel === channel) state.channel = null;
      });
      channel.on("return", (message) => {
        const fields = message.fields;
        const replyText = "replyText" in fields ? String(fields.replyText) : "Message returned by RabbitMQ";
        state.reportError(Object.assign(new Error(replyText), {
          code: "replyCode" in fields ? fields.replyCode : undefined,
        }), true);
      });
      if (state.options.subscribe) await startConsumer(state, channel);
    } catch (cause) {
      const error = state.reportError(cause);
      try {
        await closeResources();
      } catch {
        // Cleanup errors have already been reported.
      }
      throw error;
    }
  }

  /** Connects this client and starts receiving subscribed messages. */
  async function connect(): Promise<void> {
    if (disconnecting) await disconnecting;
    if (connecting) return connecting;
    if (state.channel) return;
    connecting = (async () => {
      // A channel may have closed while its connection is still open.
      if (state.connection) await closeResources();
      await open();
    })();
    try {
      await connecting;
    } finally {
      connecting = undefined;
    }
  }

  /** Closes only this client's channel and connection. */
  async function disconnect(): Promise<void> {
    if (disconnecting) return disconnecting;
    disconnecting = (async () => {
      if (connecting) await connecting.catch(() => {});
      await closeResources();
    })();
    try {
      await disconnecting;
    } finally {
      disconnecting = undefined;
    }
  }

  return { connect, disconnect };
}
