import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";
import { describe, test } from "node:test";

import { JOB_NAMES } from "../jobs/names.js";
import { migrateFullCatalogResumeStateV1ToV2, runProvidersWithConcurrency, summarizePipeline } from "./pipelines.js";

describe("cron provider concurrency", () => {
  test("starts provider flows in parallel when concurrency > 1", async () => {
    const providers = ["polymarket", "kalshi"] as const;
    const started: string[] = [];
    let resolveBothStarted: (() => void) | null = null;
    const bothStarted = new Promise<void>((resolve) => {
      resolveBothStarted = resolve;
    });

    const runPromise = runProvidersWithConcurrency([...providers], 2, async (providerCode) => {
      started.push(providerCode);
      if (started.length === providers.length) {
        resolveBothStarted?.();
      }
      await delay(300);
      return { rowsUpserted: 0, rowsSkipped: 0, errors: 0, partials: 0 };
    });

    await Promise.race([
      bothStarted,
      delay(120).then(() => {
        throw new Error("Provider flows did not start in parallel");
      })
    ]);

    await runPromise;
    assert.equal(started.length, 2);
  });

  test("returns results in provider input order", async () => {
    const results = await runProvidersWithConcurrency(["polymarket", "kalshi"], 2, async (providerCode) => {
      if (providerCode === "polymarket") {
        await delay(120);
        return { rowsUpserted: 1, rowsSkipped: 0, errors: 0, partials: 0 };
      }

      await delay(10);
      return { rowsUpserted: 2, rowsSkipped: 0, errors: 0, partials: 0 };
    });

    assert.deepEqual(
      results.map((row) => row.rowsUpserted),
      [1, 2]
    );
  });
});

describe("summarizePipeline", () => {
  test("prioritizes provider failures over partial step counts", () => {
    const summary = summarizePipeline([
      { rowsUpserted: 10, rowsSkipped: 1, errors: 1, partials: 2 },
      { rowsUpserted: 5, rowsSkipped: 3, errors: 0, partials: 1 }
    ]);

    assert.equal(summary.rowsUpserted, 15);
    assert.equal(summary.rowsSkipped, 4);
    assert.equal(summary.partialReason, "1 provider run(s) failed");
  });

  test("reports partial reason when there are no provider failures", () => {
    const summary = summarizePipeline([
      { rowsUpserted: 7, rowsSkipped: 0, errors: 0, partials: 2 },
      { rowsUpserted: 4, rowsSkipped: 1, errors: 0, partials: 0 }
    ]);

    assert.equal(summary.partialReason, "2 step(s) returned partial_success");
  });
});

describe("full catalog resume state migration", () => {
  test("migrates legacy v1 checkpoint to v2 provider states", () => {
    const migrated = migrateFullCatalogResumeStateV1ToV2({
      version: 1,
      cycleId: "cycle-1",
      cycleStartedAt: "2026-03-03T00:00:00.000Z",
      cycleExpiresAt: "2026-03-04T00:00:00.000Z",
      providerIndex: 1,
      stepIndex: 3,
      stepRetries: 2,
      currentProvider: "kalshi",
      currentStep: JOB_NAMES.KALSHI_SYNC_PRICES_FULL_CATALOG,
      lastRequestId: "req-1",
      lastStepStartedAt: "2026-03-03T00:10:00.000Z",
      lastStepFinishedAt: "2026-03-03T00:11:00.000Z",
      updatedAt: "2026-03-03T00:11:00.000Z"
    });

    assert.equal(migrated.version, 2);
    assert.equal(migrated.providers.polymarket.completed, true);
    assert.equal(migrated.providers.kalshi.stepIndex, 3);
    assert.equal(migrated.providers.kalshi.stepRetries, 2);
    assert.equal(migrated.providers.kalshi.currentStep, JOB_NAMES.KALSHI_SYNC_PRICES_FULL_CATALOG);
    assert.equal(migrated.providers.kalshi.lastRequestId, "req-1");
  });
});
