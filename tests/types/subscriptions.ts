import amqpEasy, { type AmqpEasySubscription } from "../../src/public.js";
import { z } from "zod";

const Order = z.object({ id: z.number() });
const User = z.object({ name: z.string() });

void amqpEasy({
  uri: "amqp://localhost",
  subscribe: {
    queueName: "types",
    subscriptions: [
      { routingKey: "order.*", schema: Order, handler(payload) {
        const id: number = payload.id;
        // @ts-expect-error The schema infers a number, not a string.
        const wrong: string = payload.id;
        // @ts-expect-error Unknown payload properties are not allowed.
        payload.missing;
      } },
      { routingKey: "user.*", schema: User, handler(payload) {
        const name: string = payload.name;
        // @ts-expect-error Each subscription has its own inferred type.
        payload.id;
      } },
      { routingKey: "text", schema: z.string().transform(s => s.length), handler(payload) {
        const length: number = payload;
        // @ts-expect-error The handler receives the schema output, not its input.
        const text: string = payload;
      } },
    ],
  },
});

const subscription: AmqpEasySubscription<typeof Order> = {
  schema: Order,
  routingKey: "order.created",
  handler(payload) { const id: number = payload.id; },
};
