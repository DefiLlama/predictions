import assert from "node:assert/strict";
import { describe, test } from "node:test";

import {
  calculateEventTradesMetrics,
  parseEventUid,
  parseMarketUid,
  resolveEventTradesLimit,
  normalizeTradeSideForMetrics
} from "./query-service.js";

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

describe("event trades helpers", () => {
  test("normalizeTradeSideForMetrics supports buy/sell and yes/no", () => {
    assert.equal(normalizeTradeSideForMetrics("buy"), "buy");
    assert.equal(normalizeTradeSideForMetrics("  YES  "), "buy");
    assert.equal(normalizeTradeSideForMetrics("sell"), "sell");
    assert.equal(normalizeTradeSideForMetrics("No"), "sell");
    assert.equal(normalizeTradeSideForMetrics("maker"), null);
    assert.equal(normalizeTradeSideForMetrics(null), null);
  });

  test("resolveEventTradesLimit applies defaults and bounds", () => {
    assert.equal(resolveEventTradesLimit(undefined), 50);
    assert.equal(resolveEventTradesLimit(null), 50);
    assert.equal(resolveEventTradesLimit(0), 1);
    assert.equal(resolveEventTradesLimit(-3), 1);
    assert.equal(resolveEventTradesLimit(120), 100);
    assert.equal(resolveEventTradesLimit(17.9), 17);
  });

  test("calculateEventTradesMetrics aggregates trades slice", () => {
    const metrics = calculateEventTradesMetrics([
      {
        ts: new Date("2026-03-03T10:00:00.000Z"),
        side: "buy",
        notionalUsd: "100.250000"
      },
      {
        ts: new Date("2026-03-03T10:02:00.000Z"),
        side: "NO",
        notionalUsd: "20.750000"
      },
      {
        ts: new Date("2026-03-03T10:01:00.000Z"),
        side: "unknown",
        notionalUsd: null
      }
    ]);

    assert.deepEqual(metrics, {
      tradesCount: 3,
      totalTrades: 3,
      windowStartTs: "2026-03-03T10:00:00.000Z",
      windowEndTs: "2026-03-03T10:02:00.000Z",
      totalNotionalUsd: "121",
      buyTrades: 1,
      sellTrades: 1
    });
  });

  test("calculateEventTradesMetrics supports explicit total trade count", () => {
    const metrics = calculateEventTradesMetrics(
      [
        {
          ts: new Date("2026-03-03T10:00:00.000Z"),
          side: "buy",
          notionalUsd: "10"
        }
      ],
      120
    );

    assert.equal(metrics.tradesCount, 1);
    assert.equal(metrics.totalTrades, 120);
  });
});
