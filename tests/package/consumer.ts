import amqpEasy, { type AmqpEasyClient, type AmqpEasySubscription } from "amqpeasy";
import { ValidationError } from "amqpeasy/errors";
import { z } from "zod";

const Order = z.object({ id: z.number() });
const subscriber: AmqpEasySubscription<typeof Order> = {
  routingKey: "order.*", schema: Order,
  handler(payload) { const id: number = payload.id; },
};
const client: AmqpEasyClient = await amqpEasy({
  uri: "amqp://localhost", subscribe: { queueName: "test", deadLetter: { maxRetries: 3, retryDelayMs: 5000 }, subscriptions: [
    { schema: Order, routingKey: "order.*", handler(payload) {
      const id: number = payload.id;
      // @ts-expect-error Schema properties must retain their inferred types.
      const invalid: string = payload.id;
    } },
    { schema: z.string(), routingKey: "text", handler(payload) {
      const text: string = payload;
      // @ts-expect-error Different subscriptions must not share a payload type.
      const invalid: number = payload;
    } },
  ] },
}, error => {
  if (error instanceof ValidationError) console.error(error.cause);
});
