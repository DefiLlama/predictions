import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { env } from "../config/env.js";
import { resolveRelinkBatchSize, resolveRelinkRunLimit } from "./ingestion-service.js";

describe("relink limit helpers", () => {
  test("resolveRelinkRunLimit supports explicit unbounded mode", () => {
    assert.equal(resolveRelinkRunLimit(null), null);
  });

  test("resolveRelinkRunLimit uses configured default for invalid input", () => {
    assert.equal(resolveRelinkRunLimit(undefined), env.MARKET_RELINK_MAX_MARKETS_PER_RUN);
    assert.equal(resolveRelinkRunLimit(0), env.MARKET_RELINK_MAX_MARKETS_PER_RUN);
    assert.equal(resolveRelinkRunLimit(-1), env.MARKET_RELINK_MAX_MARKETS_PER_RUN);
  });

  test("resolveRelinkRunLimit preserves bounded numeric mode", () => {
    assert.equal(resolveRelinkRunLimit(17.9), 17);
  });

  test("resolveRelinkBatchSize respects both configured batch size and run limit", () => {
    const configuredBatchSize = Math.max(1, env.MARKET_RELINK_MAX_MARKETS_PER_RUN);
    assert.equal(resolveRelinkBatchSize(null), configuredBatchSize);
    assert.equal(resolveRelinkBatchSize(7), 7);
    assert.equal(resolveRelinkBatchSize(configuredBatchSize + 100), configuredBatchSize);
  });
});
