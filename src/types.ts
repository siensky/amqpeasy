import type { z } from "zod";
import type { BaseError } from "./error.js";

/** Receives errors reported by amqpEasy. */
export type OnError = (error: BaseError) => void | Promise<void>;

export type ConnectionStatus = "disconnected" | "connecting" | "connected" | "disconnecting";
export type StatusChange = { status: ConnectionStatus; error?: BaseError };
export type OnStatusChange = (change: StatusChange) => void | Promise<void>;

export type PublishOptions = {
  /** Wait for broker acceptance. Defaults to false (legacy behavior). */
  confirms?: boolean;
  /** Store messages persistently. Defaults to false. */
  persistent?: boolean;
  /** Reject unroutable messages. Enables confirms unless explicitly disabled. */
  mandatory?: boolean;
  /** Maximum publication time, including exchange setup. Defaults to 10000. */
  timeoutMs?: number;
};

export type DisconnectOptions = {
  /** Maximum time to drain and close. Defaults to 30000. */
  timeoutMs?: number;
};

export type HandlerContext = {
  /** Aborted on connection loss or forced shutdown. */
  signal: AbortSignal;
};

/** Settings for the RabbitMQ connection and message subscriptions. */
export type AmqpEasyOptions<Schemas extends readonly z.ZodType[] = readonly z.ZodType[]> = {
  /** The RabbitMQ connection URL. */
  uri: string;
  publish?: PublishOptions;
  /** Optional settings for receiving messages. */
  subscribe?: {
    /** The name of the queue to receive messages from. */
    queueName: string;
    /** Keep a named, non-exclusive queue after disconnect. Defaults to false. */
    durable?: boolean;
    /** Unacknowledged messages per consumer. 0 means unlimited; omitted uses broker default. */
    prefetch?: number;
    /** Creates durable retry and dead-letter queues. True uses 3 retries, 5 seconds apart. */
    deadLetter?: boolean | DeadLetterOptions;
    /** The routing keys to listen for and their message handlers. */
    subscriptions: { [K in keyof Schemas]: AmqpEasySubscription<Schemas[K]> };
  };
  /** The exchange used to send and receive messages. Defaults to "events". */
  exchangeName?: string;
  /** How messages are routed. Defaults to "topic". */
  exchangeType?: ExchangeType;
};

/** Controls automatic retries before a message goes to the dead-letter queue. */
export type DeadLetterOptions = {
  /** Retries after the first failed attempt. Defaults to 3. */
  maxRetries?: number;
  /** Delay between retries in milliseconds. Defaults to 5000. */
  retryDelayMs?: number;
};

/** The supported RabbitMQ exchange types. */
export type ExchangeType = "topic" | "direct" | "fanout";

/** A schema and handler for matching messages. */
export type AmqpEasySubscription<Schema extends z.ZodType = z.ZodType> = {
  /** A topic pattern or direct routing key. Optional for fanout. */
  routingKey?: string;
  /** Validates each message before the handler runs. */
  schema: Schema;
  /** Receives the validated payload, including schema transformations. */
  handler: (payload: z.output<Schema>, context: HandlerContext) => void | Promise<void>;
};

/** A separate RabbitMQ client with its own connection and error handler. */
export type AmqpEasyClient = {
  readonly status: ConnectionStatus;
  /** Subscribe to future changes. Returns an unsubscribe function. */
  onStatusChange(listener: OnStatusChange): () => void;
  /** Connects to RabbitMQ and starts receiving subscribed messages. */
  connect(): Promise<void>;
  /** Closes this client's channel and connection. */
  disconnect(options?: DisconnectOptions): Promise<void>;
  /** Sends a JSON message. Use an empty routing key for fanout. */
  publish(routingKey: string, payload: unknown): Promise<void>;
};
