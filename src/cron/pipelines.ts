import { randomUUID } from "node:crypto";

import { rebuildMarketCategoryAssignments, refreshProviderCategory1hRollup } from "../services/category-service.js";
import type { JobRunResult } from "../services/job-log-service.js";
import { runLoggedJob } from "../services/job-log-service.js";
import {
  relinkMarketEvents,
  syncKalshiMetadataFullCatalog,
  syncKalshiOpenInterest,
  syncKalshiOrderbook,
  syncKalshiPrices,
  syncKalshiTrades,
  syncPolymarketMetadataFullCatalog,
  syncPolymarketOpenInterest,
  syncPolymarketOrderbook,
  syncPolymarketPrices,
  syncPolymarketTrades
} from "../services/ingestion-service.js";
import { refreshMarketLiquidity1hRollup, refreshMarketPrice1hRollup } from "../services/rollup-service.js";
import { rebuildScopeTopN } from "../services/scope-service.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";
import { env } from "../config/env.js";
import { JOB_NAMES } from "../jobs/names.js";
import { getHttpMetricsFromError, runWithHttpMetrics } from "../utils/http-metrics.js";

interface StepDefinition {
  providerCode: ProviderCode;
  step: string;
  mode: "topN_live" | "full_catalog";
  run: () => Promise<JobRunResult>;
}

interface PipelineCounters {
  rowsUpserted: number;
  rowsSkipped: number;
  errors: number;
  partials: number;
}

const STEP_DB_RETRY_MAX_ATTEMPTS = 2;
const STEP_DB_RETRY_BACKOFF_MS = 1_500;

const RETRYABLE_DB_ERROR_CODES = new Set([
  "08000",
  "08001",
  "08003",
  "08006",
  "57P01",
  "57P02",
  "57P03"
]);

const RETRYABLE_DB_ERROR_PATTERNS = [
  "connection terminated unexpectedly",
  "connection ended unexpectedly",
  "connection reset by peer",
  "terminating connection due to administrator command",
  "the database system is shutting down",
  "econnreset",
  "econnrefused",
  "etimedout"
];

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeMessage(value: unknown): string {
  return typeof value === "string" ? value.toLowerCase() : "";
}

function isRetryableDbError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const pgCode = (error as Error & { code?: unknown }).code;
  if (typeof pgCode === "string" && RETRYABLE_DB_ERROR_CODES.has(pgCode)) {
    return true;
  }

  const message = normalizeMessage(error.message);
  if (RETRYABLE_DB_ERROR_PATTERNS.some((pattern) => message.includes(pattern))) {
    return true;
  }

  const cause = (error as Error & { cause?: unknown }).cause;
  if (cause instanceof Error) {
    return isRetryableDbError(cause);
  }

  return false;
}

function withTimeout<T>(label: string, callback: () => Promise<T>): Promise<T> {
  const timeoutMs = env.CRON_STEP_TIMEOUT_MS;
  if (timeoutMs <= 0) {
    return callback();
  }

  return Promise.race([
    callback(),
    new Promise<never>((_, reject) => {
      setTimeout(() => {
        reject(new Error(`${label} exceeded ${timeoutMs}ms timeout`));
      }, timeoutMs);
    })
  ]);
}

async function runPipelineStep(requestId: string, pipeline: string, definition: StepDefinition): Promise<JobRunResult> {
  const startedAt = Date.now();
  logger.info(
    {
      pipeline,
      provider: definition.providerCode,
      step: definition.step,
      requestId,
      mode: definition.mode,
      status: "started"
    },
    "Cron pipeline step started"
  );

  let attempt = 0;
  while (true) {
    try {
      const { result, metrics } = await runWithHttpMetrics(requestId, () =>
        runLoggedJob(
          {
            providerCode: definition.providerCode,
            jobName: definition.step,
            requestId
          },
          () => withTimeout(`${pipeline}:${definition.providerCode}:${definition.step}`, definition.run)
        )
      );

      const durationMs = Date.now() - startedAt;
      logger.info(
        {
          pipeline,
          provider: definition.providerCode,
          step: definition.step,
          requestId,
          mode: definition.mode,
          durationMs,
          attempt: attempt + 1,
          rowsUpserted: result.rowsUpserted,
          rowsSkipped: result.rowsSkipped,
          httpMetrics: metrics,
          status: result.partialReason ? "partial_success" : "success"
        },
        "Cron pipeline step finished"
      );

      return result;
    } catch (error) {
      const retryable = isRetryableDbError(error) && attempt < STEP_DB_RETRY_MAX_ATTEMPTS;
      if (retryable) {
        attempt += 1;
        const backoffMs = STEP_DB_RETRY_BACKOFF_MS * attempt;

        logger.warn(
          {
            pipeline,
            provider: definition.providerCode,
            step: definition.step,
            requestId,
            mode: definition.mode,
            attempt,
            backoffMs,
            error
          },
          "Cron pipeline step hit transient DB error, retrying"
        );

        await sleep(backoffMs);
        continue;
      }

      const durationMs = Date.now() - startedAt;
      logger.error(
        {
          pipeline,
          provider: definition.providerCode,
          step: definition.step,
          requestId,
          mode: definition.mode,
          durationMs,
          attempt: attempt + 1,
          httpMetrics: getHttpMetricsFromError(error),
          status: "failed",
          error
        },
        "Cron pipeline step failed"
      );

      throw error;
    }
  }
}

async function runProviderSteps(
  requestId: string,
  pipeline: string,
  providerCode: ProviderCode,
  steps: StepDefinition[]
): Promise<PipelineCounters> {
  const counters: PipelineCounters = {
    rowsUpserted: 0,
    rowsSkipped: 0,
    errors: 0,
    partials: 0
  };

  for (const step of steps) {
    try {
      const result = await runPipelineStep(requestId, pipeline, step);
      counters.rowsUpserted += result.rowsUpserted;
      counters.rowsSkipped += result.rowsSkipped;
      if (result.partialReason) {
        counters.partials += 1;
      }
    } catch {
      counters.errors += 1;
      break;
    }
  }

  logger.info(
    {
      pipeline,
      provider: providerCode,
      requestId,
      rowsUpserted: counters.rowsUpserted,
      rowsSkipped: counters.rowsSkipped,
      partials: counters.partials,
      errors: counters.errors,
      status: counters.errors > 0 ? "failed" : counters.partials > 0 ? "partial_success" : "success"
    },
    "Cron provider summary"
  );

  return counters;
}

function buildTopNLiveSteps(providerCode: ProviderCode, requestId: string): StepDefinition[] {
  if (providerCode === "polymarket") {
    return [
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_PRICES,
        mode: "topN_live",
        run: () => syncPolymarketPrices({ requestId, scopeStatus: "active", mode: "topN_live" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK,
        mode: "topN_live",
        run: () => syncPolymarketOrderbook({ requestId, scopeStatus: "active", mode: "topN_live" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_TRADES,
        mode: "topN_live",
        run: () => syncPolymarketTrades({ requestId, scopeStatus: "active", mode: "topN_live" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_OI,
        mode: "topN_live",
        run: () => syncPolymarketOpenInterest({ requestId, scopeStatus: "active", mode: "topN_live" })
      }
    ];
  }

  return [
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_PRICES,
      mode: "topN_live",
      run: () => syncKalshiPrices({ requestId, scopeStatus: "active", mode: "topN_live" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_ORDERBOOK,
      mode: "topN_live",
      run: () => syncKalshiOrderbook({ requestId, scopeStatus: "active", mode: "topN_live" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_TRADES,
      mode: "topN_live",
      run: () => syncKalshiTrades({ requestId, scopeStatus: "active", mode: "topN_live" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_OI,
      mode: "topN_live",
      run: () => syncKalshiOpenInterest({ requestId, scopeStatus: "active", mode: "topN_live" })
    }
  ];
}

function buildFullCatalogSteps(providerCode: ProviderCode, requestId: string): StepDefinition[] {
  if (providerCode === "polymarket") {
    return [
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_METADATA,
        mode: "full_catalog",
        run: () => syncPolymarketMetadataFullCatalog({ requestId, mode: "full_catalog" })
      },
      {
        providerCode,
        step: JOB_NAMES.MARKET_RELINK_EVENTS,
        mode: "full_catalog",
        run: () => relinkMarketEvents(providerCode, { requestId, maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN })
      },
      {
        providerCode,
        step: JOB_NAMES.SCOPE_REBUILD,
        mode: "full_catalog",
        run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_PRICES_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketPrices({ requestId, scopeStatus: "active", mode: "full_catalog" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketOrderbook({ requestId, scopeStatus: "active", mode: "full_catalog" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_TRADES_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketTrades({ requestId, scopeStatus: "active", mode: "full_catalog" })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_OI_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketOpenInterest({ requestId, scopeStatus: "active", mode: "full_catalog" })
      },
      {
        providerCode,
        step: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
        mode: "full_catalog",
        run: () =>
          rebuildMarketCategoryAssignments(providerCode, {
            requestId,
            target: "all",
            maxMarkets: env.CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN
          })
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
        mode: "full_catalog",
        run: () => refreshProviderCategory1hRollup(providerCode)
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
        mode: "full_catalog",
        run: () => refreshMarketPrice1hRollup(providerCode)
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
        mode: "full_catalog",
        run: () => refreshMarketLiquidity1hRollup(providerCode)
      }
    ];
  }

  return [
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_METADATA,
      mode: "full_catalog",
      run: () => syncKalshiMetadataFullCatalog({ requestId, mode: "full_catalog" })
    },
    {
      providerCode,
      step: JOB_NAMES.MARKET_RELINK_EVENTS,
      mode: "full_catalog",
      run: () => relinkMarketEvents(providerCode, { requestId, maxMarkets: env.MARKET_RELINK_MAX_MARKETS_PER_RUN })
    },
    {
      providerCode,
      step: JOB_NAMES.SCOPE_REBUILD,
      mode: "full_catalog",
      run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_PRICES_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiPrices({ requestId, scopeStatus: "active", mode: "full_catalog" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_ORDERBOOK_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiOrderbook({ requestId, scopeStatus: "active", mode: "full_catalog" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_TRADES_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiTrades({ requestId, scopeStatus: "active", mode: "full_catalog" })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_OI_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiOpenInterest({ requestId, scopeStatus: "active", mode: "full_catalog" })
    },
    {
      providerCode,
      step: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
      mode: "full_catalog",
      run: () =>
        rebuildMarketCategoryAssignments(providerCode, {
          requestId,
          target: "all",
          maxMarkets: env.CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN
        })
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
      mode: "full_catalog",
      run: () => refreshProviderCategory1hRollup(providerCode)
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
      mode: "full_catalog",
      run: () => refreshMarketPrice1hRollup(providerCode)
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
      mode: "full_catalog",
      run: () => refreshMarketLiquidity1hRollup(providerCode)
    }
  ];
}

function summarizePipeline(counters: PipelineCounters[]): JobRunResult {
  const rowsUpserted = counters.reduce((sum, item) => sum + item.rowsUpserted, 0);
  const rowsSkipped = counters.reduce((sum, item) => sum + item.rowsSkipped, 0);
  const errors = counters.reduce((sum, item) => sum + item.errors, 0);
  const partials = counters.reduce((sum, item) => sum + item.partials, 0);

  return {
    rowsUpserted,
    rowsSkipped,
    partialReason: errors > 0 ? `${errors} provider run(s) failed` : partials > 0 ? `${partials} step(s) returned partial_success` : null
  };
}

export async function runTopNLivePipeline(requestIdInput?: string): Promise<JobRunResult> {
  const requestId = requestIdInput ?? randomUUID();
  const pipeline = "topN_live";
  const startedAt = Date.now();

  const counters = [];
  for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
    const providerCounters = await runProviderSteps(requestId, pipeline, providerCode, buildTopNLiveSteps(providerCode, requestId));
    counters.push(providerCounters);
  }

  const summary = summarizePipeline(counters);

  logger.info(
    {
      pipeline,
      requestId,
      mode: "topN_live",
      durationMs: Date.now() - startedAt,
      rowsUpserted: summary.rowsUpserted,
      rowsSkipped: summary.rowsSkipped,
      status: summary.partialReason ? "partial_success" : "success"
    },
    "Cron pipeline summary"
  );

  return summary;
}

export async function runFullCatalogPipeline(requestIdInput?: string): Promise<JobRunResult> {
  const requestId = requestIdInput ?? randomUUID();
  const pipeline = "full_catalog";
  const startedAt = Date.now();

  const counters = [];
  for (const providerCode of ["polymarket", "kalshi"] satisfies ProviderCode[]) {
    const providerCounters = await runProviderSteps(requestId, pipeline, providerCode, buildFullCatalogSteps(providerCode, requestId));
    counters.push(providerCounters);
  }

  const summary = summarizePipeline(counters);

  logger.info(
    {
      pipeline,
      requestId,
      mode: "full_catalog",
      durationMs: Date.now() - startedAt,
      rowsUpserted: summary.rowsUpserted,
      rowsSkipped: summary.rowsSkipped,
      status: summary.partialReason ? "partial_success" : "success"
    },
    "Cron pipeline summary"
  );

  return summary;
}
