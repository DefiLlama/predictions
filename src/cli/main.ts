import { randomUUID } from "node:crypto";

import { startServer } from "../app/server.js";
import { env } from "../config/env.js";
import { closeDb } from "../db/client.js";
import { JOB_NAMES } from "../jobs/names.js";
import { runTopNLivePipeline, runFullCatalogPipeline, runFullCatalogResumePipeline } from "../cron/pipelines.js";
import { rebuildMarketCategoryAssignments, refreshProviderCategory1hRollup } from "../services/category-service.js";
import { releaseLock, tryAcquireLock } from "../services/cron-lock-service.js";
import {
  relinkMarketEvents,
  syncKalshiMetadata,
  syncKalshiOpenInterest,
  syncKalshiOrderbook,
  syncKalshiPrices,
  syncKalshiTrades,
  syncPolymarketMetadata,
  syncPolymarketMetadataBackfill,
  syncPolymarketOpenInterest,
  syncPolymarketOrderbook,
  syncPolymarketPrices,
  syncPolymarketTrades
} from "../services/ingestion-service.js";
import { recoverStaleRunningJobRuns, runLoggedJob, type JobRunContext, type JobRunResult } from "../services/job-log-service.js";
import { getVerifySummary } from "../services/query-service.js";
import { refreshMarketLiquidity1hRollup, refreshMarketPrice1hRollup } from "../services/rollup-service.js";
import { rebuildScopeTopN } from "../services/scope-service.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";
import { runWithHttpMetrics } from "../utils/http-metrics.js";

const COMMAND = process.argv[2];
const CLI_FLAGS = new Set(process.argv.slice(3));

function printUsage(): void {
  console.log(`Usage: npm run <script>

Commands:
  server
  cron:topn-live
  cron:full-catalog
  cron:full-catalog:resume
  ingest:metadata
  ingest:metadata:backfill
  ingest:prices
  ingest:orderbook
  ingest:trades
  ingest:oi
  ingest:kalshi:metadata
  ingest:kalshi:prices
  ingest:kalshi:orderbook
  ingest:kalshi:trades
  ingest:kalshi:oi
  ingest:relink-events
  ingest:categories
  ingest:categories:backfill
  ingest:rollups
  ingest:all
  verify:summary [--strict]`);
}

function formatDate(value: Date | string | null): string {
  if (value === null) {
    return "null";
  }

  if (value instanceof Date) {
    return value.toISOString();
  }

  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toISOString();
}

function ensureStrict(summary: Awaited<ReturnType<typeof getVerifySummary>>): void {
  const issues: string[] = [];

  if (summary.scopedCount < 100) {
    issues.push(`scoped markets too low (${summary.scopedCount} < 100)`);
  }

  if (summary.fillRates.priceMarketFillRate < 0.5) {
    issues.push(`price market fill rate too low (${summary.fillRates.priceMarketFillRate.toFixed(3)} < 0.5)`);
  }

  if (summary.fillRates.orderbookBboMarketFillRate < 0.3) {
    issues.push(`orderbook BBO market fill rate too low (${summary.fillRates.orderbookBboMarketFillRate.toFixed(3)} < 0.3)`);
  }

  const failedLatest = summary.latestRuns.filter((row) => row.status === "failed");
  if (failedLatest.length > 0) {
    issues.push(`latest failed jobs: ${failedLatest.map((row) => `${row.providerCode}:${row.jobName}`).join(", ")}`);
  }

  const qualityByProvider = new Map(summary.categoryQuality.map((row) => [row.providerCode, row]));
  const polymarketQuality = qualityByProvider.get("polymarket");
  const kalshiQuality = qualityByProvider.get("kalshi");

  if (polymarketQuality && polymarketQuality.scopedUnknownRate > 0.3) {
    issues.push(`polymarket scoped unknown rate too high (${polymarketQuality.scopedUnknownRate.toFixed(3)} > 0.300)`);
  }

  if (kalshiQuality && kalshiQuality.scopedUnknownRate > 0.15) {
    issues.push(`kalshi scoped unknown rate too high (${kalshiQuality.scopedUnknownRate.toFixed(3)} > 0.150)`);
  }

  if (issues.length > 0) {
    throw new Error(`Strict verify failed: ${issues.join(" | ")}`);
  }
}

async function runVerifySummary(strictMode: boolean): Promise<void> {
  const summary = await getVerifySummary();

  console.log("Latest job runs:");
  for (const row of summary.latestRuns) {
    console.log(
      `- ${row.providerCode} ${row.jobName}: ${row.status} upserted=${row.rowsUpserted} skipped=${row.rowsSkipped} started=${formatDate(
        row.startedAt
      )} finished=${formatDate(row.finishedAt)}${row.errorText ? ` note=${row.errorText}` : ""}`
    );
  }

  console.log("\nTable counts:");
  for (const [tableName, count] of Object.entries(summary.tableCounts)) {
    console.log(`- ${tableName}: ${count}`);
  }

  console.log(`\nIn-scope markets: ${summary.scopedCount}`);
  console.log("Scope by status:");
  for (const [status, count] of Object.entries(summary.scopedByStatus)) {
    console.log(`- ${status}: ${count}`);
  }

  console.log("\nFill rates:");
  console.log(`- priceMarketFillRate(6h): ${summary.fillRates.priceMarketFillRate.toFixed(4)}`);
  console.log(`- orderbookBboMarketFillRate: ${summary.fillRates.orderbookBboMarketFillRate.toFixed(4)}`);

  console.log("\nCategory quality:");
  for (const row of summary.categoryQuality) {
    console.log(
      `- ${row.providerCode}: scopedUnknownRate=${row.scopedUnknownRate.toFixed(4)} globalUnknownRate=${row.globalUnknownRate.toFixed(
        4
      )}`
    );
  }

  console.log("\nSample markets:");
  for (const sample of summary.samples) {
    console.log(
      `- ${sample.marketUid}: latestPriceTs=${formatDate(sample.latestPriceTs)} latestOrderbookTs=${formatDate(sample.latestOrderbookTs)}`
    );
  }

  if (strictMode) {
    ensureStrict(summary);
    console.log("\nStrict verify: PASSED");
  }
}

async function runLoggedStep(context: JobRunContext, callback: () => Promise<JobRunResult>): Promise<JobRunResult> {
  const { result, metrics } = await runWithHttpMetrics(context.requestId ?? null, () => runLoggedJob(context, callback));

  logger.info(
    {
      providerCode: context.providerCode,
      jobName: context.jobName,
      requestId: context.requestId,
      rowsUpserted: result.rowsUpserted,
      rowsSkipped: result.rowsSkipped,
      httpMetrics: metrics,
      status: result.partialReason ? "partial_success" : "success",
      note: result.partialReason ?? null
    },
    "Ingestion step finished"
  );

  return result;
}

async function runPolymarketMetadataFlow(requestId: string, forceBackfill = false): Promise<void> {
  const useBackfill = forceBackfill || env.POLYMARKET_METADATA_MODE === "backfill";

  await runLoggedStep(
    {
      providerCode: "polymarket",
      jobName: useBackfill ? JOB_NAMES.POLYMARKET_SYNC_METADATA_BACKFILL_FULL : JOB_NAMES.POLYMARKET_SYNC_METADATA,
      requestId
    },
    () =>
      useBackfill
        ? syncPolymarketMetadataBackfill({ requestId })
        : syncPolymarketMetadata({ requestId })
  );

  await runLoggedStep(
    {
      providerCode: "polymarket",
      jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
      requestId
    },
    () => relinkMarketEvents("polymarket", { requestId, maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN })
  );

  await runLoggedStep(
    {
      providerCode: "polymarket",
      jobName: JOB_NAMES.SCOPE_REBUILD,
      requestId
    },
    async () => ({ rowsUpserted: await rebuildScopeTopN("polymarket"), rowsSkipped: 0 })
  );
}

async function runKalshiMetadataFlow(requestId: string): Promise<void> {
  await runLoggedStep(
    {
      providerCode: "kalshi",
      jobName: JOB_NAMES.KALSHI_SYNC_METADATA,
      requestId
    },
    () => syncKalshiMetadata({ requestId })
  );

  await runLoggedStep(
    {
      providerCode: "kalshi",
      jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
      requestId
    },
    () => relinkMarketEvents("kalshi", { requestId, maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN })
  );

  await runLoggedStep(
    {
      providerCode: "kalshi",
      jobName: JOB_NAMES.SCOPE_REBUILD,
      requestId
    },
    async () => ({ rowsUpserted: await rebuildScopeTopN("kalshi"), rowsSkipped: 0 })
  );
}

async function runCronSkippedLog(jobName: string, requestId: string): Promise<void> {
  for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
    await runLoggedJob(
      {
        providerCode,
        jobName,
        requestId
      },
      async () => ({
        rowsUpserted: 0,
        rowsSkipped: 0,
        partialReason: "skipped_lock_held"
      })
    );
  }
}

async function runCronCommand(params: {
  jobName: string;
  mode: "topN_live" | "full_catalog" | "full_catalog_resume";
  lockKey: bigint;
  runPipeline: (requestId: string) => Promise<JobRunResult>;
}): Promise<void> {
  const requestId = randomUUID();
  const lock = await tryAcquireLock(params.lockKey, { timeoutMs: env.CRON_LOCK_TIMEOUT_MS });

  if (!lock) {
    logger.warn(
      {
        pipeline: params.mode,
        requestId,
        status: "skipped",
        reason: "skipped_lock_held"
      },
      "Cron invocation skipped because lock is already held"
    );
    await runCronSkippedLog(params.jobName, requestId);
    return;
  }

  try {
    const startedAt = Date.now();
    const result = await params.runPipeline(requestId);

    logger.info(
      {
        pipeline: params.mode,
        requestId,
        durationMs: Date.now() - startedAt,
        rowsUpserted: result.rowsUpserted,
        rowsSkipped: result.rowsSkipped,
        status: result.partialReason ? "partial_success" : "success",
        note: result.partialReason ?? null
      },
      "Cron command completed"
    );
  } finally {
    await releaseLock(lock);
  }
}

async function recoverStaleJobRunsBestEffort(): Promise<void> {
  try {
    const recovered = await recoverStaleRunningJobRuns(env.JOB_RUN_STALE_AFTER_MS);
    if (recovered > 0) {
      logger.warn({ recovered, staleAfterMs: env.JOB_RUN_STALE_AFTER_MS }, "Recovered stale running job runs");
    }
  } catch (error) {
    logger.warn({ error }, "Failed to recover stale running job runs");
  }
}

async function main(): Promise<void> {
  await recoverStaleJobRunsBestEffort();

  switch (COMMAND) {
    case "server": {
      await startServer();
      return;
    }

    case "cron:topn-live": {
      await runCronCommand({
        jobName: JOB_NAMES.CRON_TOPN_LIVE,
        mode: "topN_live",
        lockKey: env.CRON_TOPN_LOCK_KEY,
        runPipeline: runTopNLivePipeline
      });
      return;
    }

    case "cron:full-catalog": {
      await runCronCommand({
        jobName: JOB_NAMES.CRON_FULL_CATALOG,
        mode: "full_catalog",
        lockKey: env.CRON_FULLCAT_LOCK_KEY,
        runPipeline: runFullCatalogPipeline
      });
      return;
    }

    case "cron:full-catalog:resume": {
      await runCronCommand({
        jobName: JOB_NAMES.CRON_FULL_CATALOG_RESUME,
        mode: "full_catalog_resume",
        lockKey: env.CRON_FULLCAT_LOCK_KEY,
        runPipeline: runFullCatalogResumePipeline
      });
      return;
    }

    case "ingest:metadata": {
      await runPolymarketMetadataFlow(randomUUID());
      return;
    }

    case "ingest:metadata:backfill": {
      await runPolymarketMetadataFlow(randomUUID(), true);
      return;
    }

    case "ingest:prices": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES, requestId },
        () => syncPolymarketPrices({ requestId })
      );
      return;
    }

    case "ingest:orderbook": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK, requestId },
        () => syncPolymarketOrderbook({ requestId })
      );
      return;
    }

    case "ingest:trades": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES, requestId },
        () => syncPolymarketTrades({ requestId })
      );
      return;
    }

    case "ingest:oi": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_OI, requestId },
        () => syncPolymarketOpenInterest({ requestId })
      );
      return;
    }

    case "ingest:kalshi:metadata": {
      await runKalshiMetadataFlow(randomUUID());
      return;
    }

    case "ingest:kalshi:prices": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_PRICES, requestId },
        () => syncKalshiPrices({ requestId })
      );
      return;
    }

    case "ingest:kalshi:orderbook": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK, requestId },
        () => syncKalshiOrderbook({ requestId })
      );
      return;
    }

    case "ingest:kalshi:trades": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_TRADES, requestId },
        () => syncKalshiTrades({ requestId })
      );
      return;
    }

    case "ingest:kalshi:oi": {
      const requestId = randomUUID();
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_OI, requestId },
        () => syncKalshiOpenInterest({ requestId })
      );
      return;
    }

    case "ingest:relink-events": {
      const requestId = randomUUID();
      for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.MARKET_RELINK_EVENTS, requestId },
          () => relinkMarketEvents(providerCode, { requestId, maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN })
        );
      }
      return;
    }

    case "ingest:categories": {
      const requestId = randomUUID();
      for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS, requestId },
          () => rebuildMarketCategoryAssignments(providerCode, { requestId, target: "scope" })
        );
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H, requestId },
          () => refreshProviderCategory1hRollup(providerCode)
        );
      }
      return;
    }

    case "ingest:categories:backfill": {
      const requestId = randomUUID();
      for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS, requestId },
          () =>
            rebuildMarketCategoryAssignments(providerCode, {
              requestId,
              target: "all",
              maxMarkets: env.CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN
            })
        );
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H, requestId },
          () => refreshProviderCategory1hRollup(providerCode)
        );
      }
      return;
    }

    case "ingest:rollups": {
      const requestId = randomUUID();
      for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS, requestId },
          () => rebuildMarketCategoryAssignments(providerCode, { requestId, target: "scope" })
        );
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H, requestId },
          () => refreshProviderCategory1hRollup(providerCode)
        );
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H, requestId },
          () => refreshMarketPrice1hRollup(providerCode)
        );
        await runLoggedStep(
          { providerCode, jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H, requestId },
          () => refreshMarketLiquidity1hRollup(providerCode)
        );
      }
      return;
    }

    case "ingest:all": {
      const requestId = randomUUID();

      await runPolymarketMetadataFlow(requestId);
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS, requestId },
        () => rebuildMarketCategoryAssignments("polymarket", { requestId, target: "scope" })
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES, requestId },
        () => syncPolymarketPrices({ requestId })
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK, requestId },
        () => syncPolymarketOrderbook({ requestId })
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES, requestId },
        () => syncPolymarketTrades({ requestId })
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.POLYMARKET_SYNC_OI, requestId },
        () => syncPolymarketOpenInterest({ requestId })
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H, requestId },
        () => refreshProviderCategory1hRollup("polymarket")
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H, requestId },
        () => refreshMarketPrice1hRollup("polymarket")
      );
      await runLoggedStep(
        { providerCode: "polymarket", jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H, requestId },
        () => refreshMarketLiquidity1hRollup("polymarket")
      );

      await runKalshiMetadataFlow(requestId);
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS, requestId },
        () => rebuildMarketCategoryAssignments("kalshi", { requestId, target: "scope" })
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_PRICES, requestId },
        () => syncKalshiPrices({ requestId })
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK, requestId },
        () => syncKalshiOrderbook({ requestId })
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_TRADES, requestId },
        () => syncKalshiTrades({ requestId })
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.KALSHI_SYNC_OI, requestId },
        () => syncKalshiOpenInterest({ requestId })
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H, requestId },
        () => refreshProviderCategory1hRollup("kalshi")
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H, requestId },
        () => refreshMarketPrice1hRollup("kalshi")
      );
      await runLoggedStep(
        { providerCode: "kalshi", jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H, requestId },
        () => refreshMarketLiquidity1hRollup("kalshi")
      );

      return;
    }

    case "verify:summary": {
      await runVerifySummary(CLI_FLAGS.has("--strict"));
      return;
    }

    default: {
      printUsage();
      process.exitCode = 1;
    }
  }
}

async function finalizeCli(exitCode: number): Promise<never> {
  if (COMMAND !== "server") {
    try {
      await closeDb();
    } catch (error) {
      logger.error({ error }, "Failed to close DB during CLI shutdown");
      exitCode = 1;
    }
  }

  process.exit(exitCode);
}

main()
  .then(() => finalizeCli(0))
  .catch((error) => {
    logger.error({ error }, "CLI command failed");
    return finalizeCli(1);
  });
