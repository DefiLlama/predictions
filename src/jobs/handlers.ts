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
import { rebuildMarketCategoryAssignments, refreshProviderCategory1hRollup } from "../services/category-service.js";
import { runLoggedJob } from "../services/job-log-service.js";
import { refreshMarketLiquidity1hRollup, refreshMarketPrice1hRollup } from "../services/rollup-service.js";
import { rebuildScopeTopN } from "../services/scope-service.js";
import type { ProviderCode } from "../types/domain.js";
import { JOB_NAMES } from "./names.js";
import type { JobHandlerMap } from "./types.js";

export const jobHandlers: JobHandlerMap = {
  [JOB_NAMES.CATEGORY_ASSIGN_MARKETS]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;
    const target = "target" in payload && (payload.target === "all" || payload.target === "scope") ? payload.target : "scope";
    const maxMarkets =
      "maxMarkets" in payload && typeof payload.maxMarkets === "number" && Number.isFinite(payload.maxMarkets)
        ? payload.maxMarkets
        : undefined;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
        requestId: payload.requestId
      },
      async () =>
        rebuildMarketCategoryAssignments(providerCode, {
          target,
          maxMarkets,
          requestId: payload.requestId
        })
    );
  },

  [JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;
    const lookbackHours = "lookbackHours" in payload ? payload.lookbackHours : undefined;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
        requestId: payload.requestId
      },
      async () => refreshMarketPrice1hRollup(providerCode, { lookbackHours })
    );
  },

  [JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;
    const lookbackHours = "lookbackHours" in payload ? payload.lookbackHours : undefined;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
        requestId: payload.requestId
      },
      async () => refreshMarketLiquidity1hRollup(providerCode, { lookbackHours })
    );
  },

  [JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
        requestId: payload.requestId
      },
      async () => refreshProviderCategory1hRollup(providerCode)
    );
  },

  [JOB_NAMES.SCOPE_REBUILD]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.SCOPE_REBUILD,
        requestId: payload.requestId
      },
      async () => {
        const count = await rebuildScopeTopN(providerCode);
        return { rowsUpserted: count, rowsSkipped: 0 };
      }
    );
  },

  [JOB_NAMES.MARKET_RELINK_EVENTS]: async (payload) => {
    const providerCode = ("providerCode" in payload && payload.providerCode ? payload.providerCode : "polymarket") as ProviderCode;
    const maxMarkets =
      "maxMarkets" in payload && typeof payload.maxMarkets === "number" && Number.isFinite(payload.maxMarkets)
        ? payload.maxMarkets
        : undefined;

    await runLoggedJob(
      {
        providerCode,
        jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
        requestId: payload.requestId
      },
      async () =>
        relinkMarketEvents(providerCode, {
          requestId: payload.requestId,
          maxMarkets
        })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_METADATA]: async (payload) => {
    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_METADATA,
        requestId: payload.requestId
      },
      async () => syncPolymarketMetadata({ requestId: payload.requestId })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_METADATA_BACKFILL_FULL]: async (payload) => {
    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_METADATA_BACKFILL_FULL,
        requestId: payload.requestId
      },
      async () => syncPolymarketMetadataBackfill({ requestId: payload.requestId })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_PRICES]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES,
        requestId: payload.requestId
      },
      async () =>
        syncPolymarketPrices({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK,
        requestId: payload.requestId
      },
      async () =>
        syncPolymarketOrderbook({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_TRADES]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES,
        requestId: payload.requestId
      },
      async () =>
        syncPolymarketTrades({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.POLYMARKET_SYNC_OI]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "polymarket",
        jobName: JOB_NAMES.POLYMARKET_SYNC_OI,
        requestId: payload.requestId
      },
      async () =>
        syncPolymarketOpenInterest({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.KALSHI_SYNC_METADATA]: async (payload) => {
    await runLoggedJob(
      {
        providerCode: "kalshi",
        jobName: JOB_NAMES.KALSHI_SYNC_METADATA,
        requestId: payload.requestId
      },
      async () => syncKalshiMetadata({ requestId: payload.requestId })
    );
  },

  [JOB_NAMES.KALSHI_SYNC_PRICES]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "kalshi",
        jobName: JOB_NAMES.KALSHI_SYNC_PRICES,
        requestId: payload.requestId
      },
      async () =>
        syncKalshiPrices({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.KALSHI_SYNC_ORDERBOOK]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "kalshi",
        jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK,
        requestId: payload.requestId
      },
      async () =>
        syncKalshiOrderbook({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.KALSHI_SYNC_TRADES]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "kalshi",
        jobName: JOB_NAMES.KALSHI_SYNC_TRADES,
        requestId: payload.requestId
      },
      async () =>
        syncKalshiTrades({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  },

  [JOB_NAMES.KALSHI_SYNC_OI]: async (payload) => {
    const scopeStatus = "scopeStatus" in payload ? payload.scopeStatus : "all";

    await runLoggedJob(
      {
        providerCode: "kalshi",
        jobName: JOB_NAMES.KALSHI_SYNC_OI,
        requestId: payload.requestId
      },
      async () =>
        syncKalshiOpenInterest({
          requestId: payload.requestId,
          scopeStatus: scopeStatus ?? "all"
        })
    );
  }
};
