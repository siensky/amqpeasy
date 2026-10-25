import type { Channel, ChannelModel, ConfirmChannel } from "amqplib";
import type { BaseError } from "./error.js";
import type { ConnectionStatus, DeadLetterOptions, ExchangeType, HandlerContext, PublishOptions } from "./types.js";

export type RuntimeSubscription = {
  routingKey: string;
  handle(payload: unknown, context: HandlerContext): Promise<void>;
};

export type ClientState = {
  options: {
    uri: string;
    exchangeName: string;
    exchangeType: ExchangeType;
    publish: Required<PublishOptions>;
    subscribe?: {
      queueName: string;
      durable?: boolean;
      prefetch?: number;
      deadLetter?: Required<DeadLetterOptions>;
      subscriptions: RuntimeSubscription[];
    };
  };
  connection: ChannelModel | null;
  channel: Channel | null;
  failureChannel: ConfirmChannel | null;
  publisher: ConfirmChannel | null;
  status: ConnectionStatus;
  lifecycle: AbortController;
  consumerTag?: string;
  handlers: Set<Promise<void>>;
  publications: Set<Promise<void>>;
  pendingPublications: Map<string, (cause?: unknown) => void>;
  setStatus(status: ConnectionStatus, error?: BaseError): void;
  invalidate(error: BaseError): void;
  reportError(cause: unknown, background?: boolean): BaseError;
};
