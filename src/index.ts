import type { z } from "zod";
import { createConnection } from "./connection.js";
import publish from "./publish.js";
import { BaseError, ValidationError } from "./error.js";
import { createErrorReporter } from "./error-handler.js";
import { resolveDeadLetter } from "./dead-letter.js";
import type { ClientState, RuntimeSubscription } from "./client-state.js";
import type { AmqpEasyClient, AmqpEasyOptions, AmqpEasySubscription, OnError } from "./types.js";

function prepareSubscription<Schema extends z.ZodType>(
  subscription: AmqpEasySubscription<Schema>,
): RuntimeSubscription {
  const { schema, handler, routingKey = "" } = subscription;
  return {
    routingKey,
    async handle(payload) {
      const result = await schema.safeParseAsync(payload);
      if (!result.success) throw new ValidationError(result.error);
      await handler(result.data);
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

    const state: ClientState = {
      options: {
        uri: options.uri,
        exchangeName: options.exchangeName || "events",
        exchangeType,
        subscribe: options.subscribe ? {
          queueName: options.subscribe.queueName,
          deadLetter: resolveDeadLetter(options.subscribe.queueName, options.subscribe.deadLetter),
          subscriptions: options.subscribe.subscriptions.map(prepareSubscription),
        } : undefined,
      },
      connection: null,
      channel: null,
      failureChannel: null,
      reportError,
    };
    const { connect, disconnect } = createConnection(state);
    return { connect, disconnect, publish: (routingKey, payload) => publish(state, routingKey, payload) };
  } catch (cause) {
    throw reportError(cause);
  }
}
