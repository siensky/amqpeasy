import amqp, { type Channel, type ChannelModel } from "amqplib";
import startConsumer from "./consumer.js";
import { BaseError, ShutdownTimeoutError } from "./error.js";
import { publicationHeader } from "./publish.js";
import type { ClientState } from "./client-state.js";
import type { DisconnectOptions } from "./types.js";

/** amqplib has no public force-close API. Used only when graceful close times out. */
function forceClose(connection: ChannelModel | null) {
  const transport = connection?.connection as { stream?: { destroy(): void } } | undefined;
  transport?.stream?.destroy();
}

function returnedError(fields: object): Error {
  return Object.assign(new Error("replyText" in fields ? String(fields.replyText) : "Message returned by RabbitMQ"), {
    code: "replyCode" in fields ? fields.replyCode : undefined,
  });
}

/** Creates connection functions for one client. */
export function createConnection(state: ClientState) {
  let connecting: Promise<void> | undefined;
  let disconnecting: Promise<void> | undefined;
  let closingConnection: ChannelModel | null = null;

  async function closeResources(): Promise<void> {
    const { channel, connection, failureChannel, publisher } = state;
    state.channel = null;
    state.connection = null;
    state.failureChannel = null;
    state.publisher = null;
    state.consumerTag = undefined;
    closingConnection = connection;
    let failure: BaseError | undefined;
    for (const resource of [channel, publisher, failureChannel, connection]) {
      if (!resource) continue;
      try { await resource.close(); }
      catch (cause) {
        const error = state.reportError(cause);
        failure ??= error;
        if (resource === connection) forceClose(connection);
      }
    }
    if (closingConnection === connection) closingConnection = null;
    if (failure) throw failure;
  }

  async function open(lifecycle: AbortController): Promise<void> {
    const { signal } = lifecycle;
    const pending = state.pendingPublications;
    signal.addEventListener("abort", () => {
      for (const finish of [...pending.values()]) finish(signal.reason);
    }, { once: true });
    try {
      const connection = await amqp.connect(state.options.uri);
      // A connection attempt can complete after a shutdown deadline.
      if (signal.aborted) {
        connection.on("error", () => {});
        forceClose(connection);
        void connection.close().catch(() => {});
        signal.throwIfAborted();
      }
      state.connection = connection;
      let connectionError: BaseError | undefined;
      connection.on("error", error => { connectionError = state.reportError(error, true); });
      connection.on("close", () => {
        if (state.connection !== connection) return;
        state.connection = null;
        state.channel = null;
        state.failureChannel = null;
        state.publisher = null;
        state.consumerTag = undefined;
        state.invalidate(connectionError ?? new BaseError("The RabbitMQ connection closed.", "CONNECTION_CLOSED"));
      });
      function watch(channel: Channel) {
        let channelError: BaseError | undefined;
        channel.on("error", error => { channelError = state.reportError(error, true); });
        channel.on("close", () => {
          if (state.connection !== connection || state.lifecycle !== lifecycle) return;
          if (state.channel === channel) state.channel = null;
          if (state.publisher === channel) state.publisher = null;
          if (state.failureChannel === channel) state.failureChannel = null;
          state.invalidate(channelError ?? new BaseError("A RabbitMQ channel closed.", "CHANNEL_CLOSED"));
        });
      }
      const channel = await connection.createChannel();
      signal.throwIfAborted();
      state.channel = channel;
      watch(channel);
      channel.on("return", message => {
        state.reportError(returnedError(message.fields), true);
      });
      if (state.options.publish.confirms) {
        const publisher = await connection.createConfirmChannel();
        signal.throwIfAborted();
        state.publisher = publisher;
        watch(publisher);
        publisher.on("return", message => {
          const id: unknown = message.properties.headers?.[publicationHeader];
          if (typeof id === "string") pending.get(id)?.(returnedError(message.fields));
        });
      }
      if (state.options.subscribe) await startConsumer(state, channel);
      if (state.options.subscribe?.deadLetter && !state.failureChannel) {
        throw new BaseError("The failure-publishing channel closed during setup.");
      }
      if (state.failureChannel) watch(state.failureChannel);
      signal.throwIfAborted();
      if (state.status !== "disconnecting") state.setStatus("connected");
    } catch (cause) {
      const error = state.reportError(cause);
      if (state.lifecycle === lifecycle) {
        state.invalidate(error);
        try { await closeResources(); } catch { /* Already reported. */ }
      }
      throw error;
    }
  }

  async function connect(): Promise<void> {
    if (disconnecting) await disconnecting;
    if (connecting) return connecting;
    if (state.status === "connected") return;
    state.lifecycle.abort(new BaseError("The connection is being replaced."));
    const lifecycle = new AbortController();
    state.lifecycle = lifecycle;
    state.handlers = new Set();
    state.publications = new Set();
    state.pendingPublications = new Map();
    state.setStatus("connecting");
    const operation = (async () => {
      try {
        if (state.connection) await closeResources();
        lifecycle.signal.throwIfAborted();
        await open(lifecycle);
      } catch (cause) {
        if (state.lifecycle === lifecycle) state.invalidate(state.reportError(cause));
        throw cause;
      }
    })();
    connecting = operation;
    try { await operation; }
    finally { if (connecting === operation) connecting = undefined; }
  }

  /** Stop deliveries, drain work, then close, all within one deadline. */
  async function disconnect(options: DisconnectOptions = {}): Promise<void> {
    if (disconnecting) return disconnecting;
    const timeoutMs = options.timeoutMs ?? 30000;
    if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 2147483647) {
      throw state.reportError(new BaseError("disconnect.timeoutMs must be an integer between 1 and 2147483647."));
    }
    if (!connecting && !state.connection) return;
    state.setStatus("disconnecting");
    const lifecycle = state.lifecycle;
    const opening = connecting;
    let timer: ReturnType<typeof setTimeout>;
    const deadline = new Promise<never>((_, reject) => {
      timer = setTimeout(() => {
        const error = new ShutdownTimeoutError();
        lifecycle.abort(error);
        forceClose(state.connection);
        forceClose(closingConnection);
        state.connection = null;
        state.channel = null;
        state.publisher = null;
        state.failureChannel = null;
        state.consumerTag = undefined;
        if (connecting === opening) connecting = undefined;
        reject(error);
      }, timeoutMs);
    });
    const drain = (async () => {
      if (opening) await opening.catch(() => {});
      if (lifecycle.signal.aborted) {
        if (state.lifecycle === lifecycle) await closeResources();
        return;
      }
      lifecycle.signal.throwIfAborted();
      if (state.channel && state.consumerTag) {
        await state.channel.cancel(state.consumerTag);
        lifecycle.signal.throwIfAborted();
        state.consumerTag = undefined;
      }
      lifecycle.signal.throwIfAborted();
      // Cancel-ok is a barrier: no new deliveries arrive after it.
      await Promise.allSettled([...state.handlers, ...state.publications]);
      lifecycle.signal.throwIfAborted();
      await closeResources();
      lifecycle.abort(new BaseError("The client disconnected."));
    })();
    const operation = (async () => {
      try { await Promise.race([drain, deadline]); }
      catch (cause) {
        lifecycle.abort(cause);
        // A failed cancel or drain must still initiate resource cleanup.
        if (!(cause instanceof ShutdownTimeoutError)) {
          forceClose(state.connection);
          forceClose(closingConnection);
        }
        if (state.connection) void closeResources().catch(() => {});
        throw state.reportError(cause);
      } finally {
        clearTimeout(timer!);
        state.setStatus("disconnected");
      }
    })();
    disconnecting = operation;
    try { await operation; }
    finally { if (disconnecting === operation) disconnecting = undefined; }
  }

  return { connect, disconnect };
}
