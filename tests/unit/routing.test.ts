import { expect, test } from "bun:test";
import { matchesTopic } from "../../src/consumer.js";

test.each([
  ["order.*", "order.created", true],
  ["order.*", "order.eu.created", false],
  ["order.*", "order", false],
  ["order.#", "order", true],
  ["order.#", "order.eu.created", true],
  ["#.created", "created", true],
  ["#.created", "order.eu.created", true],
  ["order.#.created", "order.created", true],
  ["order.#.created", "order.eu.created", true],
  ["#", "", true],
  ["*", "", false],
  ["", "", true],
  ["a.#.#.b", "a.x.y.b", true],
  ["literal+value", "literalvalue", false],
])("topic %s matches %s: %s", (pattern, key, expected) => {
  expect(matchesTopic(pattern, key)).toBe(expected);
});
