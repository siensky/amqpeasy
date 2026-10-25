import type { Channel, ConsumeMessage } from "amqplib";
import { BaseError, ValidationError } from "./error.js";
import type { ClientState } from "./client-state.js";
import { messageRoutingKey, setupDeadLetter, type FailureHandler } from "./dead-letter.js";

/** Matches topic words: * matches one word, # matches zero or more. */
export function matchesTopic(pattern: string, routingKey: string): boolean {
  const patternWords = pattern === "" ? [] : pattern.split(".");
  const keyWords = routingKey === "" ? [] : routingKey.split(".");
  const memo = new Map<string, boolean>();
  function matches(p: number, k: number): boolean {
    const id = `${p}:${k}`;
    if (memo.has(id)) return memo.get(id)!;
    const word = patternWords[p];
    const result = p === patternWords.length
      ? k === keyWords.length
      : word === "#"
        ? matches(p + 1, k) || (k < keyWords.length && matches(p, k + 1))
        : k < keyWords.length && (word === "*" || word === keyWords[k]) && matches(p + 1, k + 1);
    memo.set(id, result);
    return result;
  }
  return matches(0, 0);
}

export async function registerQueues(state: ClientState, channel: Channel): Promise<string> {
  const { subscribe, exchangeName, exchangeType } = state.options;
  if (!subscribe) throw new BaseError("No subscriptions configured.");
  await channel.assertExchange(exchangeName, exchangeType, { durable: true });
  const queue = subscribe.deadLetter
    ? { queue: subscribe.queueName }
    : await channel.assertQueue(subscribe.queueName, subscribe.durable
      ? { durable: true, exclusive: false, autoDelete: false }
      : { exclusive: true });
  for (const subscription of subscribe.subscriptions) {
    await channel.bindQueue(queue.queue, exchangeName, exchangeType === "fanout" ? "" : subscription.routingKey);
  }
  return queue.queue;
}

export async function consumeEvents(state: ClientState, channel: Channel, queueName: string, handleFailure?: FailureHandler): Promise<void> {
  const { subscribe, exchangeType } = state.options;
  if (!subscribe) return;
  const { signal } = state.lifecycle;
  const handlers = state.handlers;
  const processMessage = async (message: ConsumeMessage) => {
    let retryable = false;
    let processingComplete = false;
    try {
      const payload: unknown = JSON.parse(message.content.toString());
      const routingKey = subscribe.deadLetter ? messageRoutingKey(message, queueName) : message.fields.routingKey;
      const subscriptions = subscribe.subscriptions.filter((subscription) =>
        exchangeType === "fanout" || (exchangeType === "topic"
          ? matchesTopic(subscription.routingKey, routingKey)
          : subscription.routingKey === routingKey),
      );
      if (subscriptions.length === 0) {
        throw new BaseError(`No subscription matches routing key "${routingKey}".`);
      }
      retryable = true;
      for (const subscription of subscriptions) {
        if (signal.aborted) return;
        await subscription.handle(payload, { signal });
      }
      retryable = false;
      processingComplete = true;
      if (!signal.aborted) channel.ack(message);
    } catch (cause) {
      const error = state.reportError(cause, true);
      if (handleFailure && !processingComplete && !signal.aborted) {
        try {
          await handleFailure(message, error, retryable && !(cause instanceof ValidationError));
        } catch (transferError) {
          // Do not acknowledge or loop on a failed transfer.
          state.reportError(transferError, true);
        }
      }
    }
  }
  const consumer = await channel.consume(queueName, (message: ConsumeMessage | null) => {
    if (signal.aborted) return;
    if (!message) {
      if (state.status !== "disconnecting") {
        state.invalidate(state.reportError(new BaseError("RabbitMQ cancelled the consumer.", "CONSUMER_CANCELLED"), true));
      }
      return;
    }
    const task = processMessage(message);
    handlers.add(task);
    void task.finally(() => handlers.delete(task));
    return task;
  });
  if (!signal.aborted) state.consumerTag = consumer.consumerTag;
}

export default async function startConsumer(state: ClientState, channel: Channel): Promise<void> {
  const { signal } = state.lifecycle;
  const handleFailure = state.options.subscribe?.deadLetter ? await setupDeadLetter(state, channel) : undefined;
  signal.throwIfAborted();
  const queueName = await registerQueues(state, channel);
  signal.throwIfAborted();
  const prefetch = state.options.subscribe?.prefetch;
  if (prefetch !== undefined) await channel.prefetch(prefetch);
  signal.throwIfAborted();
  await consumeEvents(state, channel, queueName, handleFailure);
}
