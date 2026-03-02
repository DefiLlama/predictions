import { randomUUID } from "node:crypto";

import { and, desc, eq } from "drizzle-orm";

import { startServer } from "../app/server.js";
import { env } from "../config/env.js";
import { closeDb, db } from "../db/client.js";
import { jobRunLog } from "../db/schema.js";
import { enqueueJob, startWorkerRuntime } from "../jobs/boss.js";
import { JOB_NAMES, type JobName } from "../jobs/names.js";
import { getVerifySummary } from "../services/query-service.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";

interface QueueCommand {
  jobName: JobName;
  providerCode: ProviderCode;
  payload?: Record<string, unknown>;
}

const COMMAND = process.argv[2];
const CLI_FLAGS = new Set(process.argv.slice(3));

function printUsage(): void {
  console.log(`Usage: npm run <script>

Commands:
  server
  worker
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

async function waitForJobLog(params: {
  providerCode: ProviderCode;
  jobName: string;
  requestId: string;
  timeoutMs?: number;
}): Promise<{
  status: string;
  rowsUpserted: number;
  rowsSkipped: number;
  errorText: string | null;
}> {
  const timeoutMs = params.timeoutMs ?? 60 * 60 * 1000;
  const startedAt = Date.now();

  while (Date.now() - startedAt < timeoutMs) {
    const rows = await db
      .select({
        status: jobRunLog.status,
        rowsUpserted: jobRunLog.rowsUpserted,
        rowsSkipped: jobRunLog.rowsSkipped,
        errorText: jobRunLog.errorText
      })
      .from(jobRunLog)
      .where(
        and(
          eq(jobRunLog.providerCode, params.providerCode),
          eq(jobRunLog.jobName, params.jobName),
          eq(jobRunLog.requestId, params.requestId)
        )
      )
      .orderBy(desc(jobRunLog.startedAt))
      .limit(1);

    const row = rows[0];
    if (row && row.status !== "running") {
      return row;
    }

    await new Promise((resolve) => setTimeout(resolve, 1000));
  }

  throw new Error(`Timed out waiting for ${params.jobName} (${params.requestId})`);
}

async function runQueueCommands(commands: QueueCommand[]): Promise<void> {
  const runtime = await startWorkerRuntime({ enableScheduler: false });

  try {
    for (const command of commands) {
      const requestId = randomUUID();
      const payload = {
        ...(command.payload ?? {}),
        requestId
      };

      await enqueueJob(runtime.boss, command.jobName, payload);

      const result = await waitForJobLog({
        providerCode: command.providerCode,
        jobName: command.jobName,
        requestId
      });

      if (result.status !== "success" && result.status !== "partial_success") {
        throw new Error(`${command.jobName} failed: ${result.errorText ?? "unknown error"}`);
      }

      logger.info(
        {
          jobName: command.jobName,
          requestId,
          status: result.status,
          rowsUpserted: result.rowsUpserted,
          rowsSkipped: result.rowsSkipped,
          note: result.errorText
        },
        "Job completed"
      );
    }
  } finally {
    await runtime.stop();
  }
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

async function runWorker(): Promise<void> {
  const runtime = await startWorkerRuntime({ enableScheduler: true });
  logger.info("Worker started with scheduler enabled");

  const shutdown = async () => {
    await runtime.stop();
    await closeDb();
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void shutdown();
  });
  process.on("SIGTERM", () => {
    void shutdown();
  });

  await new Promise<void>(() => {
    // Keep process alive while pg-boss workers run.
  });
}

async function main(): Promise<void> {
  switch (COMMAND) {
    case "server": {
      await startServer();
      return;
    }

    case "worker": {
      await runWorker();
      return;
    }

    case "ingest:metadata": {
      const metadataJobName =
        env.POLYMARKET_METADATA_MODE === "backfill"
          ? JOB_NAMES.POLYMARKET_SYNC_METADATA_BACKFILL_FULL
          : JOB_NAMES.POLYMARKET_SYNC_METADATA;

      await runQueueCommands([
        { jobName: metadataJobName, providerCode: "polymarket" },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        { jobName: JOB_NAMES.SCOPE_REBUILD, providerCode: "polymarket", payload: { providerCode: "polymarket" } }
      ]);
      return;
    }

    case "ingest:metadata:backfill": {
      await runQueueCommands([
        { jobName: JOB_NAMES.POLYMARKET_SYNC_METADATA_BACKFILL_FULL, providerCode: "polymarket" },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        { jobName: JOB_NAMES.SCOPE_REBUILD, providerCode: "polymarket", payload: { providerCode: "polymarket" } }
      ]);
      return;
    }

    case "ingest:prices": {
      await runQueueCommands([{ jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES, providerCode: "polymarket" }]);
      return;
    }

    case "ingest:orderbook": {
      await runQueueCommands([{ jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK, providerCode: "polymarket" }]);
      return;
    }

    case "ingest:trades": {
      await runQueueCommands([{ jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES, providerCode: "polymarket" }]);
      return;
    }

    case "ingest:oi": {
      await runQueueCommands([{ jobName: JOB_NAMES.POLYMARKET_SYNC_OI, providerCode: "polymarket" }]);
      return;
    }

    case "ingest:kalshi:metadata": {
      await runQueueCommands([
        { jobName: JOB_NAMES.KALSHI_SYNC_METADATA, providerCode: "kalshi" },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        { jobName: JOB_NAMES.SCOPE_REBUILD, providerCode: "kalshi", payload: { providerCode: "kalshi" } }
      ]);
      return;
    }

    case "ingest:kalshi:prices": {
      await runQueueCommands([{ jobName: JOB_NAMES.KALSHI_SYNC_PRICES, providerCode: "kalshi" }]);
      return;
    }

    case "ingest:kalshi:orderbook": {
      await runQueueCommands([{ jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK, providerCode: "kalshi" }]);
      return;
    }

    case "ingest:kalshi:trades": {
      await runQueueCommands([{ jobName: JOB_NAMES.KALSHI_SYNC_TRADES, providerCode: "kalshi" }]);
      return;
    }

    case "ingest:kalshi:oi": {
      await runQueueCommands([{ jobName: JOB_NAMES.KALSHI_SYNC_OI, providerCode: "kalshi" }]);
      return;
    }

    case "ingest:relink-events": {
      await runQueueCommands([
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        }
      ]);
      return;
    }

    case "ingest:categories": {
      await runQueueCommands([
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        }
      ]);
      return;
    }

    case "ingest:categories:backfill": {
      await runQueueCommands([
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "polymarket",
          payload: {
            providerCode: "polymarket",
            target: "all",
            maxMarkets: env.CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN
          }
        },
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "kalshi",
          payload: {
            providerCode: "kalshi",
            target: "all",
            maxMarkets: env.CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN
          }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        }
      ]);
      return;
    }

    case "ingest:rollups": {
      await runQueueCommands([
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        }
      ]);
      return;
    }

    case "ingest:all": {
      await runQueueCommands([
        { jobName: JOB_NAMES.POLYMARKET_SYNC_METADATA, providerCode: "polymarket" },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        { jobName: JOB_NAMES.SCOPE_REBUILD, providerCode: "polymarket", payload: { providerCode: "polymarket" } },
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        { jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES, providerCode: "polymarket" },
        { jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK, providerCode: "polymarket" },
        { jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES, providerCode: "polymarket" },
        { jobName: JOB_NAMES.POLYMARKET_SYNC_OI, providerCode: "polymarket" },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
          providerCode: "polymarket",
          payload: { providerCode: "polymarket" }
        },
        { jobName: JOB_NAMES.KALSHI_SYNC_METADATA, providerCode: "kalshi" },
        {
          jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi", maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN }
        },
        { jobName: JOB_NAMES.SCOPE_REBUILD, providerCode: "kalshi", payload: { providerCode: "kalshi" } },
        {
          jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        { jobName: JOB_NAMES.KALSHI_SYNC_PRICES, providerCode: "kalshi" },
        { jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK, providerCode: "kalshi" },
        { jobName: JOB_NAMES.KALSHI_SYNC_TRADES, providerCode: "kalshi" },
        { jobName: JOB_NAMES.KALSHI_SYNC_OI, providerCode: "kalshi" },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        },
        {
          jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
          providerCode: "kalshi",
          payload: { providerCode: "kalshi" }
        }
      ]);
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

main()
  .then(async () => {
    if (COMMAND !== "server" && COMMAND !== "worker") {
      await closeDb();
    }
  })
  .catch(async (error) => {
    logger.error({ error }, "CLI command failed");
    await closeDb();
    process.exitCode = 1;
  });
