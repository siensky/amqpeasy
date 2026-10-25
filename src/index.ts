import type { z } from "zod";
import { createConnection } from "./connection.js";
import publish from "./publish.js";
import { BaseError, ValidationError } from "./error.js";
import { createErrorReporter } from "./error-handler.js";
import { resolveDeadLetter } from "./dead-letter.js";
import type { ClientState, RuntimeSubscription } from "./client-state.js";
import type { AmqpEasyClient, AmqpEasyOptions, AmqpEasySubscription, OnError, OnStatusChange } from "./types.js";

function prepareSubscription<Schema extends z.ZodType>(
  subscription: AmqpEasySubscription<Schema>,
): RuntimeSubscription {
  const { schema, handler, routingKey = "" } = subscription;
  return {
    routingKey,
    async handle(payload, context) {
      const result = await schema.safeParseAsync(payload);
      if (!result.success) throw new ValidationError(result.error);
      if (!context.signal.aborted) await handler(result.data, context);
    },
  };
}

/** Creates a separate client. Call connect() to start using RabbitMQ.
 * @param onError Receives errors from this client only.
 */
export default async function amqpEasy<const Schemas extends readonly z.ZodType[]>(
  options: AmqpEasyOptions<Schemas>,
  onError?: OnError,
): Promise<AmqpEasyClient> {
  const reportError = createErrorReporter(onError);
  try {
    const exchangeType = options.exchangeType ?? "topic";
    if (!["topic", "direct", "fanout"].includes(exchangeType)) {
      throw new BaseError("exchangeType must be topic, direct, or fanout.");
    }
    for (const subscription of options.subscribe?.subscriptions ?? []) {
      if (!subscription.schema || typeof subscription.schema.safeParseAsync !== "function") {
        throw new BaseError("Each subscription needs a Zod schema.");
      }
      if (exchangeType !== "fanout" && typeof subscription.routingKey !== "string") {
        throw new BaseError("A routingKey is required for topic and direct subscriptions.");
      }
    }

    const publishOptions = {
      confirms: options.publish?.confirms ?? options.publish?.mandatory ?? false,
      persistent: options.publish?.persistent ?? false,
      mandatory: options.publish?.mandatory ?? false,
      timeoutMs: options.publish?.timeoutMs ?? 10000,
    };
    for (const value of [publishOptions.confirms, publishOptions.persistent, publishOptions.mandatory, options.subscribe?.durable ?? false]) {
      if (typeof value !== "boolean") throw new BaseError("Publishing flags and durable must be booleans.");
    }
    if (publishOptions.mandatory && !publishOptions.confirms) throw new BaseError("mandatory requires publisher confirms.");
    if (!Number.isSafeInteger(publishOptions.timeoutMs) || publishOptions.timeoutMs < 1 || publishOptions.timeoutMs > 2147483647) {
      throw new BaseError("publish.timeoutMs must be an integer between 1 and 2147483647.");
    }
    const prefetch = options.subscribe?.prefetch;
    if (prefetch !== undefined && (!Number.isSafeInteger(prefetch) || prefetch < 0 || prefetch > 65535)) {
      throw new BaseError("prefetch must be an integer between 0 and 65535.");
    }
    if (options.subscribe?.durable && !options.subscribe.queueName.trim()) throw new BaseError("A durable queue needs a stable queueName.");
    if (options.subscribe?.durable === false && options.subscribe.deadLetter) throw new BaseError("Dead-letter queues must be durable.");
    const listeners = new Set<OnStatusChange>();
    const state: ClientState = {
      options: {
        uri: options.uri,
        exchangeName: options.exchangeName || "events",
        exchangeType,
        publish: publishOptions,
        subscribe: options.subscribe ? {
          queueName: options.subscribe.queueName,
          durable: options.subscribe.durable,
          prefetch,
          deadLetter: resolveDeadLetter(options.subscribe.queueName, options.subscribe.deadLetter),
          subscriptions: options.subscribe.subscriptions.map(prepareSubscription),
        } : undefined,
      },
      connection: null,
      channel: null,
      failureChannel: null,
      publisher: null,
      status: "disconnected",
      lifecycle: new AbortController(),
      handlers: new Set(),
      publications: new Set(),
      pendingPublications: new Map(),
      setStatus(status, error) {
        if (state.status === status) return;
        state.status = status;
        for (const listener of listeners) {
          // Defer callbacks so reentrant calls see the current connection operation.
          Promise.resolve().then(() => listener({ status, error })).catch(cause => { reportError(cause, true); });
        }
      },
      invalidate(error) {
        state.lifecycle.abort(error);
        if (state.status !== "disconnecting") state.setStatus("disconnected", error);
      },
      reportError,
    };
    const { connect, disconnect } = createConnection(state);
    return {
      connect, disconnect,
      get status() { return state.status; },
      onStatusChange(listener) { listeners.add(listener); return () => { listeners.delete(listener); }; },
      publish: (routingKey, payload) => publish(state, routingKey, payload),
    };
  } catch (cause) {
    throw reportError(cause);
  }
}
