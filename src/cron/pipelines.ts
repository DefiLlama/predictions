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

interface FullCatalogResumeStateV1 {
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

interface FullCatalogProviderResumeState {
  stepIndex: number;
  stepRetries: number;
  currentStep: string;
  lastRequestId: string | null;
  lastStepStartedAt: string | null;
  lastStepFinishedAt: string | null;
  completed: boolean;
}

interface FullCatalogResumeState {
  version: 2;
  cycleId: string;
  cycleStartedAt: string;
  cycleExpiresAt: string;
  providers: Record<ProviderCode, FullCatalogProviderResumeState>;
  updatedAt: string;
}

const STEP_DB_RETRY_MAX_ATTEMPTS = 2;
const STEP_DB_RETRY_BACKOFF_MS = 1_500;
const FULL_CATALOG_RESUME_CHECKPOINT_PROVIDER = "system";
const FULL_CATALOG_RESUME_CHECKPOINT_KEY = "cron:full-catalog:resume:v1";
const PIPELINE_PROVIDERS = ["polymarket", "kalshi"] satisfies ProviderCode[];

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
          status: result.partialReason || result.continueSameStep ? "partial_success" : "success"
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
      if (result.partialReason || result.continueSameStep) {
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

function getFullCatalogStepNames(providerCode: ProviderCode): string[] {
  if (providerCode === "polymarket") {
    return [
      JOB_NAMES.POLYMARKET_SYNC_METADATA,
      JOB_NAMES.MARKET_RELINK_EVENTS,
      JOB_NAMES.SCOPE_REBUILD,
      JOB_NAMES.POLYMARKET_SYNC_PRICES_FULL_CATALOG,
      JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK_FULL_CATALOG,
      JOB_NAMES.POLYMARKET_SYNC_TRADES_FULL_CATALOG,
      JOB_NAMES.POLYMARKET_SYNC_OI_FULL_CATALOG,
      JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
      JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
      JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
      JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H
    ];
  }

  return [
    JOB_NAMES.KALSHI_SYNC_METADATA,
    JOB_NAMES.MARKET_RELINK_EVENTS,
    JOB_NAMES.SCOPE_REBUILD,
    JOB_NAMES.KALSHI_SYNC_PRICES_FULL_CATALOG,
    JOB_NAMES.KALSHI_SYNC_ORDERBOOK_FULL_CATALOG,
    JOB_NAMES.KALSHI_SYNC_TRADES_FULL_CATALOG,
    JOB_NAMES.KALSHI_SYNC_OI_FULL_CATALOG,
    JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
    JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
    JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
    JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H
  ];
}

function createInitialProviderResumeState(providerCode: ProviderCode, requestId?: string): FullCatalogProviderResumeState {
  return {
    stepIndex: 0,
    stepRetries: 0,
    currentStep: getFullCatalogStepNames(providerCode)[0]!,
    lastRequestId: requestId ?? null,
    lastStepStartedAt: null,
    lastStepFinishedAt: null,
    completed: false
  };
}

function createFullCatalogResumeState(now: Date, requestId?: string): FullCatalogResumeState {
  const cycleStartedAt = now.toISOString();
  const cycleExpiresAt = new Date(now.getTime() + env.FULL_CATALOG_CYCLE_TTL_MS).toISOString();

  return {
    version: 2,
    cycleId: randomUUID(),
    cycleStartedAt,
    cycleExpiresAt,
    providers: {
      polymarket: createInitialProviderResumeState("polymarket", requestId),
      kalshi: createInitialProviderResumeState("kalshi", requestId)
    },
    updatedAt: cycleStartedAt
  };
}

function parseFullCatalogResumeStateV1(raw: Record<string, unknown> | null): FullCatalogResumeStateV1 | null {
  if (!raw || raw.version !== 1) {
    return null;
  }

  const cycleId = typeof raw.cycleId === "string" && raw.cycleId.trim().length > 0 ? raw.cycleId : null;
  const cycleStartedAt = parseIso(raw.cycleStartedAt);
  const cycleExpiresAt = parseIso(raw.cycleExpiresAt);

  if (!cycleId || !cycleStartedAt || !cycleExpiresAt) {
    return null;
  }

  const providerIndex = Math.max(0, Math.min(Math.floor(parseFiniteNumber(raw.providerIndex, 0)), PIPELINE_PROVIDERS.length - 1));

  return {
    version: 1,
    cycleId,
    cycleStartedAt,
    cycleExpiresAt,
    providerIndex,
    stepIndex: Math.max(0, Math.floor(parseFiniteNumber(raw.stepIndex, 0))),
    stepRetries: Math.max(0, Math.floor(parseFiniteNumber(raw.stepRetries, 0))),
    currentProvider: PIPELINE_PROVIDERS[providerIndex]!,
    currentStep: typeof raw.currentStep === "string" && raw.currentStep.length > 0 ? raw.currentStep : JOB_NAMES.POLYMARKET_SYNC_METADATA,
    lastRequestId: typeof raw.lastRequestId === "string" ? raw.lastRequestId : null,
    lastStepStartedAt: parseIso(raw.lastStepStartedAt),
    lastStepFinishedAt: parseIso(raw.lastStepFinishedAt),
    updatedAt: parseIso(raw.updatedAt) ?? cycleStartedAt
  };
}

function parseProviderResumeState(providerCode: ProviderCode, raw: unknown): FullCatalogProviderResumeState {
  const base = createInitialProviderResumeState(providerCode);
  const stepNames = getFullCatalogStepNames(providerCode);

  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return base;
  }

  const value = raw as Record<string, unknown>;
  const stepIndex = Math.max(0, Math.floor(parseFiniteNumber(value.stepIndex, 0)));
  const completed = typeof value.completed === "boolean" ? value.completed : stepIndex >= stepNames.length;
  const normalizedStepIndex = completed ? Math.max(stepNames.length, stepIndex) : Math.min(stepIndex, stepNames.length - 1);

  return {
    stepIndex: normalizedStepIndex,
    stepRetries: Math.max(0, Math.floor(parseFiniteNumber(value.stepRetries, 0))),
    currentStep:
      typeof value.currentStep === "string" && value.currentStep.length > 0
        ? value.currentStep
        : completed
          ? stepNames[stepNames.length - 1]!
          : stepNames[normalizedStepIndex]!,
    lastRequestId: typeof value.lastRequestId === "string" ? value.lastRequestId : null,
    lastStepStartedAt: parseIso(value.lastStepStartedAt),
    lastStepFinishedAt: parseIso(value.lastStepFinishedAt),
    completed
  };
}

function parseFullCatalogResumeStateV2(raw: Record<string, unknown> | null): FullCatalogResumeState | null {
  if (!raw || raw.version !== 2) {
    return null;
  }

  const cycleId = typeof raw.cycleId === "string" && raw.cycleId.trim().length > 0 ? raw.cycleId : null;
  const cycleStartedAt = parseIso(raw.cycleStartedAt);
  const cycleExpiresAt = parseIso(raw.cycleExpiresAt);
  if (!cycleId || !cycleStartedAt || !cycleExpiresAt) {
    return null;
  }

  const providersRaw = raw.providers;
  if (!providersRaw || typeof providersRaw !== "object" || Array.isArray(providersRaw)) {
    return null;
  }

  const providersRecord = providersRaw as Record<string, unknown>;

  return {
    version: 2,
    cycleId,
    cycleStartedAt,
    cycleExpiresAt,
    providers: {
      polymarket: parseProviderResumeState("polymarket", providersRecord.polymarket),
      kalshi: parseProviderResumeState("kalshi", providersRecord.kalshi)
    },
    updatedAt: parseIso(raw.updatedAt) ?? cycleStartedAt
  };
}

export function migrateFullCatalogResumeStateV1ToV2(stateV1: FullCatalogResumeStateV1): FullCatalogResumeState {
  const migrated = createFullCatalogResumeState(new Date(stateV1.cycleStartedAt), stateV1.lastRequestId ?? undefined);
  migrated.cycleId = stateV1.cycleId;
  migrated.cycleStartedAt = stateV1.cycleStartedAt;
  migrated.cycleExpiresAt = stateV1.cycleExpiresAt;
  migrated.updatedAt = stateV1.updatedAt;

  const providerState = migrated.providers[stateV1.currentProvider];
  providerState.stepIndex = stateV1.stepIndex;
  providerState.stepRetries = stateV1.stepRetries;
  providerState.currentStep = stateV1.currentStep;
  providerState.lastRequestId = stateV1.lastRequestId;
  providerState.lastStepStartedAt = stateV1.lastStepStartedAt;
  providerState.lastStepFinishedAt = stateV1.lastStepFinishedAt;
  providerState.completed = false;

  const currentProviderIdx = PIPELINE_PROVIDERS.indexOf(stateV1.currentProvider);
  for (const providerCode of PIPELINE_PROVIDERS) {
    const idx = PIPELINE_PROVIDERS.indexOf(providerCode);
    const state = migrated.providers[providerCode];
    const stepNames = getFullCatalogStepNames(providerCode);

    if (idx < currentProviderIdx) {
      state.stepIndex = stepNames.length;
      state.stepRetries = 0;
      state.currentStep = stepNames[stepNames.length - 1]!;
      state.completed = true;
    } else if (idx > currentProviderIdx) {
      state.stepIndex = 0;
      state.stepRetries = 0;
      state.currentStep = stepNames[0]!;
      state.completed = false;
    }
  }

  return migrated;
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
  const parsedV2 = parseFullCatalogResumeStateV2(raw);
  const nowMs = now.getTime();

  if (parsedV2 && Date.parse(parsedV2.cycleExpiresAt) > nowMs) {
    return parsedV2;
  }

  const parsedV1 = parseFullCatalogResumeStateV1(raw);
  if (parsedV1 && Date.parse(parsedV1.cycleExpiresAt) > nowMs) {
    const migrated = migrateFullCatalogResumeStateV1ToV2(parsedV1);
    await saveFullCatalogResumeState(migrated);
    return migrated;
  }

  const initialized = createFullCatalogResumeState(now, requestId);
  await saveFullCatalogResumeState(initialized);
  return initialized;
}

function resolveCurrentFullCatalogStep(
  state: FullCatalogResumeState,
  providerCode: ProviderCode,
  requestId: string,
  resumeIntraStep = false
): StepDefinition | null {
  const providerState = state.providers[providerCode];
  if (providerState.completed) {
    return null;
  }

  const steps = buildFullCatalogSteps(providerCode, requestId, resumeIntraStep);
  if (providerState.stepIndex >= steps.length) {
    providerState.completed = true;
    providerState.stepRetries = 0;
    providerState.currentStep = steps[steps.length - 1]?.step ?? providerState.currentStep;
    return null;
  }

  const currentStep = steps[providerState.stepIndex]!;
  providerState.currentStep = currentStep.step;
  return currentStep;
}

function isFullCatalogCycleCompleted(state: FullCatalogResumeState): boolean {
  return PIPELINE_PROVIDERS.every((providerCode) => state.providers[providerCode].completed);
}

function getNextFullCatalogStepName(state: FullCatalogResumeState, providerCode: ProviderCode): string | null {
  const providerState = state.providers[providerCode];
  const stepNames = getFullCatalogStepNames(providerCode);
  if (providerState.completed || providerState.stepIndex >= stepNames.length) {
    return null;
  }

  return stepNames[providerState.stepIndex]!;
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
        step: JOB_NAMES.MARKET_RELINK_EVENTS,
        mode: "topN_live",
        run: () =>
          relinkMarketEvents(providerCode, {
            requestId,
            target: "all",
            maxMarkets: null
          })
      },
      {
        providerCode,
        step: JOB_NAMES.SCOPE_REBUILD,
        mode: "topN_live",
        run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
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
        run: () => rebuildMarketCategoryAssignments(providerCode, { requestId, target: "scope" })
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
      step: JOB_NAMES.MARKET_RELINK_EVENTS,
      mode: "topN_live",
      run: () =>
        relinkMarketEvents(providerCode, {
          requestId,
          target: "all",
          maxMarkets: null
        })
    },
    {
      providerCode,
      step: JOB_NAMES.SCOPE_REBUILD,
      mode: "topN_live",
      run: async () => ({ rowsUpserted: await rebuildScopeTopN(providerCode), rowsSkipped: 0 })
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
      run: () => rebuildMarketCategoryAssignments(providerCode, { requestId, target: "scope" })
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

function buildFullCatalogSteps(providerCode: ProviderCode, requestId: string, resumeIntraStep = false): StepDefinition[] {
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
        run: () => relinkMarketEvents(providerCode, { requestId, target: "all", maxMarkets: null })
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
        run: () => syncPolymarketPrices({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketOrderbook({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_TRADES_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketTrades({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
      },
      {
        providerCode,
        step: JOB_NAMES.POLYMARKET_SYNC_OI_FULL_CATALOG,
        mode: "full_catalog",
        run: () => syncPolymarketOpenInterest({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
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
      run: () => relinkMarketEvents(providerCode, { requestId, target: "all", maxMarkets: null })
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
      run: () => syncKalshiPrices({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_ORDERBOOK_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiOrderbook({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_TRADES_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiTrades({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
    },
    {
      providerCode,
      step: JOB_NAMES.KALSHI_SYNC_OI_FULL_CATALOG,
      mode: "full_catalog",
      run: () => syncKalshiOpenInterest({ requestId, scopeStatus: "active", mode: "full_catalog", resumeIntraStep })
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

export function summarizePipeline(counters: PipelineCounters[]): JobRunResult {
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

export async function runProvidersWithConcurrency(
  providers: ProviderCode[],
  providerConcurrency: number,
  runner: (providerCode: ProviderCode) => Promise<PipelineCounters>
): Promise<PipelineCounters[]> {
  if (providers.length === 0) {
    return [];
  }

  const workerCount = Math.max(1, Math.min(Math.floor(providerConcurrency), providers.length));
  const results: PipelineCounters[] = new Array(providers.length);
  let nextIndex = 0;

  const runWorker = async (): Promise<void> => {
    while (true) {
      const index = nextIndex;
      nextIndex += 1;
      if (index >= providers.length) {
        return;
      }

      const providerCode = providers[index]!;
      results[index] = await runner(providerCode);
    }
  };

  await Promise.all(Array.from({ length: workerCount }, () => runWorker()));
  return results;
}

export async function runTopNLivePipeline(requestIdInput?: string): Promise<JobRunResult> {
  const requestId = requestIdInput ?? randomUUID();
  const pipeline = "topN_live";
  const startedAt = Date.now();
  const providers = [...PIPELINE_PROVIDERS];
  const providerConcurrency = providers.length;

  logger.info({ pipeline, requestId, providerConcurrency, providers }, "Cron provider scheduler started");

  const counters = await runProvidersWithConcurrency(
    providers,
    providerConcurrency,
    async (providerCode) => runProviderSteps(requestId, pipeline, providerCode, buildTopNLiveSteps(providerCode, requestId))
  );

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
  const providers = [...PIPELINE_PROVIDERS];
  const providerConcurrency = Math.max(1, Math.min(env.CRON_FULLCAT_PROVIDER_CONCURRENCY, providers.length));

  logger.info({ pipeline, requestId, providerConcurrency, providers }, "Cron provider scheduler started");

  const counters = await runProvidersWithConcurrency(
    providers,
    providerConcurrency,
    async (providerCode) => runProviderSteps(requestId, pipeline, providerCode, buildFullCatalogSteps(providerCode, requestId))
  );

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
  let state = await loadFullCatalogResumeState(requestId);
  if (isFullCatalogCycleCompleted(state)) {
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
  }

  const providers = [...PIPELINE_PROVIDERS];
  const providerConcurrency = Math.max(1, Math.min(env.CRON_FULLCAT_PROVIDER_CONCURRENCY, providers.length));
  const maxStepsPerProvider = Math.max(
    1,
    typeof process.env.FULL_CATALOG_MAX_STEPS_PER_PROVIDER_PER_INVOCATION === "string"
      ? env.FULL_CATALOG_MAX_STEPS_PER_PROVIDER_PER_INVOCATION
      : env.FULL_CATALOG_MAX_STEPS_PER_INVOCATION
  );

  let stateSaveChain = Promise.resolve();
  const updateResumeState = async (mutator: (draft: FullCatalogResumeState) => void): Promise<void> => {
    stateSaveChain = stateSaveChain.then(async () => {
      mutator(state);
      state.updatedAt = new Date().toISOString();
      await saveFullCatalogResumeState(state);
    });

    await stateSaveChain;
  };

  const executedStepsByProvider: Record<ProviderCode, number> = {
    polymarket: 0,
    kalshi: 0
  };

  const counters = await runProvidersWithConcurrency(providers, providerConcurrency, async (providerCode) => {
    const providerCounters: PipelineCounters = {
      rowsUpserted: 0,
      rowsSkipped: 0,
      errors: 0,
      partials: 0
    };

    let executedSteps = 0;
    while (executedSteps < maxStepsPerProvider && Date.now() < deadline) {
      const step = resolveCurrentFullCatalogStep(state, providerCode, requestId, true);
      if (!step) {
        await updateResumeState((draft) => {
          const providerState = draft.providers[providerCode];
          const stepNames = getFullCatalogStepNames(providerCode);
          providerState.completed = true;
          providerState.stepRetries = 0;
          providerState.stepIndex = Math.max(providerState.stepIndex, stepNames.length);
          providerState.currentStep = stepNames[stepNames.length - 1] ?? providerState.currentStep;
        });
        break;
      }

      await updateResumeState((draft) => {
        const providerState = draft.providers[providerCode];
        providerState.lastRequestId = requestId;
        providerState.lastStepStartedAt = new Date().toISOString();
        providerState.lastStepFinishedAt = null;
        providerState.currentStep = step.step;
      });

      try {
        const result = await runPipelineStep(requestId, pipeline, step);
        providerCounters.rowsUpserted += result.rowsUpserted;
        providerCounters.rowsSkipped += result.rowsSkipped;
        if (result.partialReason || result.continueSameStep) {
          providerCounters.partials += 1;
        }

        await updateResumeState((draft) => {
          const providerState = draft.providers[providerCode];
          providerState.stepRetries = 0;
          providerState.lastStepFinishedAt = new Date().toISOString();

          if (!result.continueSameStep) {
            providerState.stepIndex += 1;
          }

          const stepNames = getFullCatalogStepNames(providerCode);
          if (providerState.stepIndex >= stepNames.length) {
            providerState.completed = true;
            providerState.stepIndex = stepNames.length;
            providerState.currentStep = stepNames[stepNames.length - 1]!;
          } else {
            providerState.completed = false;
            providerState.currentStep = stepNames[providerState.stepIndex]!;
          }
        });

        executedSteps += 1;
        executedStepsByProvider[providerCode] += 1;
      } catch (error) {
        let skipStep = false;
        let stepRetries = 0;
        let skippedStepName: string | null = null;

        await updateResumeState((draft) => {
          const providerState = draft.providers[providerCode];
          providerState.stepRetries += 1;
          providerState.lastStepFinishedAt = new Date().toISOString();
          stepRetries = providerState.stepRetries;
          skipStep = providerState.stepRetries >= env.FULL_CATALOG_MAX_STEP_RETRIES;

          if (skipStep) {
            const stepNames = getFullCatalogStepNames(providerCode);
            skippedStepName = providerState.currentStep;
            providerState.stepRetries = 0;
            providerState.stepIndex += 1;
            if (providerState.stepIndex >= stepNames.length) {
              providerState.completed = true;
              providerState.stepIndex = stepNames.length;
              providerState.currentStep = stepNames[stepNames.length - 1]!;
            } else {
              providerState.completed = false;
              providerState.currentStep = stepNames[providerState.stepIndex]!;
            }
          }
        });

        if (skipStep) {
          providerCounters.partials += 1;
          executedSteps += 1;
          executedStepsByProvider[providerCode] += 1;
          logger.error(
            {
              pipeline,
              requestId,
              cycleId: state.cycleId,
              provider: providerCode,
              step: skippedStepName ?? step.step,
              stepRetries,
              maxStepRetries: env.FULL_CATALOG_MAX_STEP_RETRIES
            },
            "Full catalog resume reached step retry cap, skipping step"
          );
          continue;
        }

        providerCounters.errors += 1;
        logger.error(
          {
            pipeline,
            requestId,
            cycleId: state.cycleId,
            provider: providerCode,
            step: step.step,
            stepRetries,
            maxStepRetries: env.FULL_CATALOG_MAX_STEP_RETRIES,
            error
          },
          "Full catalog resume step failed before retry cap"
        );
        break;
      }
    }

    logger.info(
      {
        pipeline,
        requestId,
        cycleId: state.cycleId,
        provider: providerCode,
        rowsUpserted: providerCounters.rowsUpserted,
        rowsSkipped: providerCounters.rowsSkipped,
        partials: providerCounters.partials,
        errors: providerCounters.errors,
        executedSteps: executedStepsByProvider[providerCode],
        status: providerCounters.errors > 0 ? "failed" : providerCounters.partials > 0 ? "partial_success" : "success"
      },
      "Full catalog resume provider summary"
    );

    return providerCounters;
  });

  await stateSaveChain;

  if (isFullCatalogCycleCompleted(state)) {
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
  }

  const baseSummary = summarizePipeline(counters);
  const timedOut = Date.now() >= deadline;
  const partialReasonParts: string[] = [];
  if (baseSummary.partialReason) {
    partialReasonParts.push(baseSummary.partialReason);
  }
  if (timedOut) {
    partialReasonParts.push("invocation budget reached");
  }

  const summary: JobRunResult = {
    rowsUpserted: baseSummary.rowsUpserted,
    rowsSkipped: baseSummary.rowsSkipped,
    partialReason: partialReasonParts.length > 0 ? partialReasonParts.join("; ") : null
  };

  logger.info(
    {
      pipeline,
      requestId,
      durationMs: Date.now() - startedAt,
      invocationBudgetMs,
      providerConcurrency,
      maxStepsPerProvider,
      executedStepsByProvider,
      rowsUpserted: summary.rowsUpserted,
      rowsSkipped: summary.rowsSkipped,
      nextSteps: {
        polymarket: getNextFullCatalogStepName(state, "polymarket"),
        kalshi: getNextFullCatalogStepName(state, "kalshi")
      },
      status: summary.partialReason ? "partial_success" : "success"
    },
    "Cron pipeline summary"
  );

  return summary;
}
