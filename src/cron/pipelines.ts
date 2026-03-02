import { randomUUID } from "node:crypto";

import { rebuildMarketCategoryAssignments, refreshProviderCategory1hRollup } from "../services/category-service.js";
import { getCheckpoint, setCheckpoint } from "../services/checkpoint-service.js";
import type { JobRunResult } from "../services/job-log-service.js";
import { runLoggedJob } from "../services/job-log-service.js";
import {
  relinkMarketEvents,
  syncKalshiMetadata,
  syncKalshiMetadataFullCatalog,
  syncKalshiOpenInterest,
  syncKalshiOrderbook,
  syncKalshiPrices,
  syncKalshiTrades,
  syncPolymarketMetadata,
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

interface FullCatalogResumeState {
  version: 1;
  cycleId: string;
  cycleStartedAt: string;
  cycleExpiresAt: string;
  providerIndex: number;
  stepIndex: number;
  stepRetries: number;
  currentProvider: ProviderCode;
  currentStep: string;
  lastRequestId: string | null;
  lastStepStartedAt: string | null;
  lastStepFinishedAt: string | null;
  updatedAt: string;
}

const STEP_DB_RETRY_MAX_ATTEMPTS = 2;
const STEP_DB_RETRY_BACKOFF_MS = 1_500;
const FULL_CATALOG_RESUME_CHECKPOINT_PROVIDER = "system";
const FULL_CATALOG_RESUME_CHECKPOINT_KEY = "cron:full-catalog:resume:v1";
const FULL_CATALOG_PROVIDERS = ["polymarket", "kalshi"] satisfies ProviderCode[];

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

function parseFiniteNumber(value: unknown, fallback = 0): number {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }

  return fallback;
}

function parseIso(value: unknown): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return new Date(parsed).toISOString();
}

function clampProviderIndex(index: number): number {
  if (index < 0) {
    return 0;
  }

  if (index >= FULL_CATALOG_PROVIDERS.length) {
    return FULL_CATALOG_PROVIDERS.length - 1;
  }

  return index;
}

function createFullCatalogResumeState(now: Date, requestId?: string): FullCatalogResumeState {
  const cycleStartedAt = now.toISOString();
  const cycleExpiresAt = new Date(now.getTime() + env.FULL_CATALOG_CYCLE_TTL_MS).toISOString();

  return {
    version: 1,
    cycleId: randomUUID(),
    cycleStartedAt,
    cycleExpiresAt,
    providerIndex: 0,
    stepIndex: 0,
    stepRetries: 0,
    currentProvider: FULL_CATALOG_PROVIDERS[0],
    currentStep: JOB_NAMES.POLYMARKET_SYNC_METADATA,
    lastRequestId: requestId ?? null,
    lastStepStartedAt: null,
    lastStepFinishedAt: null,
    updatedAt: cycleStartedAt
  };
}

function parseFullCatalogResumeState(raw: Record<string, unknown> | null): FullCatalogResumeState | null {
  if (!raw || raw.version !== 1) {
    return null;
  }

  const cycleId = typeof raw.cycleId === "string" && raw.cycleId.trim().length > 0 ? raw.cycleId : null;
  const cycleStartedAt = parseIso(raw.cycleStartedAt);
  const cycleExpiresAt = parseIso(raw.cycleExpiresAt);

  if (!cycleId || !cycleStartedAt || !cycleExpiresAt) {
    return null;
  }

  const providerIndex = clampProviderIndex(Math.floor(parseFiniteNumber(raw.providerIndex, 0)));
  const currentProvider = FULL_CATALOG_PROVIDERS[providerIndex];

  return {
    version: 1,
    cycleId,
    cycleStartedAt,
    cycleExpiresAt,
    providerIndex,
    stepIndex: Math.max(0, Math.floor(parseFiniteNumber(raw.stepIndex, 0))),
    stepRetries: Math.max(0, Math.floor(parseFiniteNumber(raw.stepRetries, 0))),
    currentProvider,
    currentStep: typeof raw.currentStep === "string" && raw.currentStep.length > 0 ? raw.currentStep : JOB_NAMES.POLYMARKET_SYNC_METADATA,
    lastRequestId: typeof raw.lastRequestId === "string" ? raw.lastRequestId : null,
    lastStepStartedAt: parseIso(raw.lastStepStartedAt),
    lastStepFinishedAt: parseIso(raw.lastStepFinishedAt),
    updatedAt: parseIso(raw.updatedAt) ?? cycleStartedAt
  };
}

async function saveFullCatalogResumeState(state: FullCatalogResumeState): Promise<void> {
  await setCheckpoint(
    FULL_CATALOG_RESUME_CHECKPOINT_PROVIDER,
    FULL_CATALOG_RESUME_CHECKPOINT_KEY,
    state as unknown as Record<string, unknown>
  );
}

async function loadFullCatalogResumeState(requestId: string): Promise<FullCatalogResumeState> {
  const now = new Date();
  const raw = await getCheckpoint(FULL_CATALOG_RESUME_CHECKPOINT_PROVIDER, FULL_CATALOG_RESUME_CHECKPOINT_KEY);
  const parsed = parseFullCatalogResumeState(raw);
  const nowMs = now.getTime();

  if (!parsed || Date.parse(parsed.cycleExpiresAt) <= nowMs) {
    const initialized = createFullCatalogResumeState(now, requestId);
    await saveFullCatalogResumeState(initialized);
    return initialized;
  }

  return parsed;
}

function resolveCurrentFullCatalogStep(state: FullCatalogResumeState, requestId: string): StepDefinition | null {
  while (state.providerIndex < FULL_CATALOG_PROVIDERS.length) {
    const providerCode = FULL_CATALOG_PROVIDERS[state.providerIndex]!;
    const steps = buildFullCatalogSteps(providerCode, requestId);
    if (state.stepIndex < steps.length) {
      const currentStep = steps[state.stepIndex]!;
      state.currentProvider = providerCode;
      state.currentStep = currentStep.step;
      return currentStep;
    }

    state.providerIndex += 1;
    state.stepIndex = 0;
    state.stepRetries = 0;
  }

  return null;
}

function buildTopNLiveSteps(providerCode: ProviderCode, requestId: string): StepDefinition[] {
  if (providerCode === "polymarket") {
    return [
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_METADATA,
        mode: "topN_live",
        run: () => syncPolymarketMetadata({ requestId })
      },
      {
        providerCode,
        step: JOB_NAMES.SCOPE_REBUILD,
        mode: "topN_live",
        run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
      },
      {
        providerCode,
        step: JOB_NAMES.MARKET_RELINK_EVENTS,
        mode: "topN_live",
        run: () =>
          relinkMarketEvents(providerCode, {
            requestId,
            target: "scope"
          })
      },
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
      },
      {
        providerCode,
        step: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
        mode: "topN_live",
        run: () =>
          rebuildMarketCategoryAssignments(providerCode, {
            requestId,
            target: "scope",
            maxMarkets: env.POLYMARKET_SCOPE_TOP_N
          })
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
        mode: "topN_live",
        run: () => refreshProviderCategory1hRollup(providerCode, { target: "scope" })
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
        mode: "topN_live",
        run: () => refreshMarketPrice1hRollup(providerCode, { target: "scope" })
      },
      {
        providerCode,
        step: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
        mode: "topN_live",
        run: () => refreshMarketLiquidity1hRollup(providerCode, { target: "scope" })
      }
    ];
  }

  return [
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_METADATA,
      mode: "topN_live",
      run: () => syncKalshiMetadata({ requestId })
    },
    {
      providerCode,
      step: JOB_NAMES.SCOPE_REBUILD,
      mode: "topN_live",
      run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
    },
    {
      providerCode,
      step: JOB_NAMES.MARKET_RELINK_EVENTS,
      mode: "topN_live",
      run: () =>
        relinkMarketEvents(providerCode, {
          requestId,
          target: "scope"
        })
    },
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
    },
    {
      providerCode,
      step: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
      mode: "topN_live",
      run: () =>
        rebuildMarketCategoryAssignments(providerCode, {
          requestId,
          target: "scope",
          maxMarkets: env.POLYMARKET_SCOPE_TOP_N
        })
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
      mode: "topN_live",
      run: () => refreshProviderCategory1hRollup(providerCode, { target: "scope" })
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
      mode: "topN_live",
      run: () => refreshMarketPrice1hRollup(providerCode, { target: "scope" })
    },
    {
      providerCode,
      step: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
      mode: "topN_live",
      run: () => refreshMarketLiquidity1hRollup(providerCode, { target: "scope" })
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

export async function runFullCatalogResumePipeline(requestIdInput?: string): Promise<JobRunResult> {
  const requestId = requestIdInput ?? randomUUID();
  const pipeline = "full_catalog_resume";
  const startedAt = Date.now();
  const invocationBudgetMs = env.FULL_CATALOG_RESUME_INVOCATION_BUDGET_MS;
  const deadline = startedAt + invocationBudgetMs;

  let rowsUpserted = 0;
  let rowsSkipped = 0;
  let partials = 0;
  let executedSteps = 0;

  let state = await loadFullCatalogResumeState(requestId);
  const maxSteps = env.FULL_CATALOG_MAX_STEPS_PER_INVOCATION;

  while (executedSteps < maxSteps && Date.now() < deadline) {
    const step = resolveCurrentFullCatalogStep(state, requestId);
    if (!step) {
      logger.info(
        {
          pipeline,
          requestId,
          cycleId: state.cycleId,
          status: "completed"
        },
        "Full catalog cycle completed, resetting resume state"
      );

      state = createFullCatalogResumeState(new Date(), requestId);
      await saveFullCatalogResumeState(state);
      break;
    }

    state.lastRequestId = requestId;
    state.lastStepStartedAt = new Date().toISOString();
    state.lastStepFinishedAt = null;
    state.updatedAt = state.lastStepStartedAt;
    await saveFullCatalogResumeState(state);

    try {
      const result = await runPipelineStep(requestId, pipeline, step);
      rowsUpserted += result.rowsUpserted;
      rowsSkipped += result.rowsSkipped;
      if (result.partialReason) {
        partials += 1;
      }

      state.stepRetries = 0;
      state.stepIndex += 1;
      state.lastStepFinishedAt = new Date().toISOString();
      state.updatedAt = state.lastStepFinishedAt;
      await saveFullCatalogResumeState(state);
      executedSteps += 1;
    } catch (error) {
      state.stepRetries += 1;
      state.lastStepFinishedAt = new Date().toISOString();
      state.updatedAt = state.lastStepFinishedAt;
      await saveFullCatalogResumeState(state);

      if (state.stepRetries >= env.FULL_CATALOG_MAX_STEP_RETRIES) {
        logger.error(
          {
            pipeline,
            requestId,
            cycleId: state.cycleId,
            provider: state.currentProvider,
            step: state.currentStep,
            stepRetries: state.stepRetries,
            maxStepRetries: env.FULL_CATALOG_MAX_STEP_RETRIES
          },
          "Full catalog resume reached step retry cap, skipping step"
        );

        partials += 1;
        state.stepRetries = 0;
        state.stepIndex += 1;
        state.updatedAt = new Date().toISOString();
        await saveFullCatalogResumeState(state);
        executedSteps += 1;
        continue;
      }

      throw error;
    }
  }

  const timedOut = Date.now() >= deadline;
  const partialReasonParts: string[] = [];
  if (partials > 0) {
    partialReasonParts.push(`${partials} step(s) returned partial_success or were skipped after retry cap`);
  }
  if (timedOut) {
    partialReasonParts.push("invocation budget reached");
  }

  const summary: JobRunResult = {
    rowsUpserted,
    rowsSkipped,
    partialReason: partialReasonParts.length > 0 ? partialReasonParts.join("; ") : null
  };

  const nextStep = resolveCurrentFullCatalogStep(state, requestId);

  logger.info(
    {
      pipeline,
      requestId,
      durationMs: Date.now() - startedAt,
      invocationBudgetMs,
      executedSteps,
      rowsUpserted,
      rowsSkipped,
      partials,
      nextProvider: nextStep?.providerCode ?? null,
      nextStep: nextStep?.step ?? null,
      status: summary.partialReason ? "partial_success" : "success"
    },
    "Cron pipeline summary"
  );

  return summary;
}
