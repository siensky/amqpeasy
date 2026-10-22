import type { Channel, ChannelModel, ConfirmChannel } from "amqplib";
import type { BaseError } from "./error.js";
import type { DeadLetterOptions, ExchangeType } from "./types.js";

export type RuntimeSubscription = {
  routingKey: string;
  handle(payload: unknown): Promise<void>;
};

export type ClientState = {
  options: {
    uri: string;
    exchangeName: string;
    exchangeType: ExchangeType;
    subscribe?: {
      queueName: string;
      deadLetter?: Required<DeadLetterOptions>;
      subscriptions: RuntimeSubscription[];
    };
  };
  connection: ChannelModel | null;
  channel: Channel | null;
  failureChannel: ConfirmChannel | null;
  reportError(cause: unknown, background?: boolean): BaseError;
};
