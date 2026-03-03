import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { parseEventUid, parseMarketUid } from "./query-service.js";

describe("UID parsing", () => {
  test("parseMarketUid parses provider and marketRef", () => {
    assert.deepEqual(parseMarketUid("kalshi:KXTOPMODEL-26MAR07-GPT"), {
      providerCode: "kalshi",
      marketRef: "KXTOPMODEL-26MAR07-GPT"
    });
  });

  test("parseEventUid parses provider and eventRef", () => {
    assert.deepEqual(parseEventUid("kalshi:KXTOPMODEL-26MAR07"), {
      providerCode: "kalshi",
      eventRef: "KXTOPMODEL-26MAR07"
    });
  });

  test("parseEventUid returns null for invalid scoped id", () => {
    assert.equal(parseEventUid("kalshi"), null);
    assert.equal(parseEventUid(":KXTOPMODEL-26MAR07"), null);
  });
});
