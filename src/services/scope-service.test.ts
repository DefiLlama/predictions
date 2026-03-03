import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { rankExpandedScopeMarketIds } from "./scope-service.js";

describe("rankExpandedScopeMarketIds", () => {
  test("orders by seed-event priority and keeps seeds before siblings", () => {
    const ranked = rankExpandedScopeMarketIds(
      [
        { id: 101, eventId: 11 },
        { id: 201, eventId: 22 },
        { id: 102, eventId: 11 }
      ],
      [
        { id: 101, eventId: 11, primaryRankValue: 500, secondaryRankValue: 10 },
        { id: 102, eventId: 11, primaryRankValue: 300, secondaryRankValue: 15 },
        { id: 103, eventId: 11, primaryRankValue: 999, secondaryRankValue: 99 },
        { id: 104, eventId: 11, primaryRankValue: 450, secondaryRankValue: 12 },
        { id: 201, eventId: 22, primaryRankValue: 400, secondaryRankValue: 20 },
        { id: 202, eventId: 22, primaryRankValue: 800, secondaryRankValue: 30 }
      ]
    );

    assert.deepEqual(ranked, [101, 102, 103, 104, 201, 202]);
  });

  test("deduplicates candidates and keeps null-event seeds", () => {
    const ranked = rankExpandedScopeMarketIds(
      [
        { id: 10, eventId: 1 },
        { id: 20, eventId: null }
      ],
      [
        { id: 10, eventId: 1, primaryRankValue: 100, secondaryRankValue: 5 },
        { id: 11, eventId: 1, primaryRankValue: 90, secondaryRankValue: 7 },
        { id: 11, eventId: 1, primaryRankValue: 95, secondaryRankValue: 1 }
      ]
    );

    assert.deepEqual(ranked, [10, 11, 20]);
  });
});
