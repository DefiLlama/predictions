import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { parseEpochToDate } from "./time.js";

describe("parseEpochToDate", () => {
  test("parses epoch seconds", () => {
    const date = parseEpochToDate(1_700_000_000);
    assert.equal(date.toISOString(), "2023-11-14T22:13:20.000Z");
  });

  test("parses epoch milliseconds", () => {
    const date = parseEpochToDate(1_700_000_000_000);
    assert.equal(date.toISOString(), "2023-11-14T22:13:20.000Z");
  });
});
