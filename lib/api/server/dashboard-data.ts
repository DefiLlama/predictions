import { unstable_cache } from "next/cache";

import type {
  DashboardBenchmarksData,
  DashboardEvent,
  DashboardInstrument,
  DashboardMainData,
  DashboardMarket,
  TopTrade,
  TopTradesData,
  TreemapEntry,
} from "@/lib/api/types";
import { getDashboardBenchmarks as getDashboardBenchmarksSource } from "@/src/services/defillama-service";
import {
  getDashboardMain as getDashboardMainSource,
  getDashboardTreemap as getDashboardTreemapSource,
  getTopTrades as getTopTradesSource,
} from "@/src/services/query-service";
import type { ProviderCode } from "@/src/types/domain";

function toIsoString(value: Date | string | null): string | null {
  if (value === null) {
    return null;
  }

  if (typeof value === "string") {
    return value;
  }

  return value.toISOString();
}

function serializeDashboardInstrument(
  instrument: Awaited<ReturnType<typeof getDashboardMainSource>>["events"][number]["markets"][number]["instruments"][number],
): DashboardInstrument {
  return {
    instrumentRef: instrument.instrumentRef,
    outcomeLabel: instrument.outcomeLabel,
    outcomeIndex: instrument.outcomeIndex,
    latestPriceTs: toIsoString(instrument.latestPriceTs),
    latestPrice: instrument.latestPrice,
    previousPrice24h: instrument.previousPrice24h,
    delta24h: instrument.delta24h,
    latestOrderbookTs: toIsoString(instrument.latestOrderbookTs),
    bestBid: instrument.bestBid,
    bestAsk: instrument.bestAsk,
    spread: instrument.spread,
    bidDepthTop5: instrument.bidDepthTop5,
    askDepthTop5: instrument.askDepthTop5,
  };
}

function serializeDashboardMarket(
  market: Awaited<ReturnType<typeof getDashboardMainSource>>["events"][number]["markets"][number],
): DashboardMarket {
  return {
    marketUid: market.marketUid,
    providerCode: market.providerCode,
    marketRef: market.marketRef,
    title: market.title,
    displayTitle: market.displayTitle,
    status: market.status,
    closeTime: toIsoString(market.closeTime),
    volume24h: market.volume24h,
    liquidity: market.liquidity,
    instruments: market.instruments.map(serializeDashboardInstrument),
  };
}

function serializeDashboardEvent(
  event: Awaited<ReturnType<typeof getDashboardMainSource>>["events"][number],
): DashboardEvent {
  return {
    eventUid: event.eventUid,
    providerCode: event.providerCode,
    eventRef: event.eventRef,
    title: event.title,
    category: event.category,
    startTime: toIsoString(event.startTime),
    endTime: toIsoString(event.endTime),
    status: event.status,
    marketCount: event.marketCount,
    activeMarketCount: event.activeMarketCount,
    volume24h: event.volume24h,
    liquidity: event.liquidity,
    latestMarketCloseTime: toIsoString(event.latestMarketCloseTime),
    maxAbsDelta24h: event.maxAbsDelta24h,
    markets: event.markets.map(serializeDashboardMarket),
  };
}

function serializeTopTrade(
  trade: Awaited<ReturnType<typeof getTopTradesSource>>["trades"][number],
): TopTrade {
  return {
    tradeRef: trade.tradeRef,
    ts: toIsoString(trade.ts) ?? new Date(0).toISOString(),
    providerCode: trade.providerCode,
    side: trade.side,
    price: trade.price,
    qty: trade.qty,
    notionalUsd: trade.notionalUsd,
    traderRef: trade.traderRef,
    marketUid: trade.marketUid,
    marketRef: trade.marketRef,
    marketTitle: trade.marketTitle,
    eventUid: trade.eventUid,
    eventTitle: trade.eventTitle,
    instrumentRef: trade.instrumentRef,
    outcomeLabel: trade.outcomeLabel,
  };
}

const loadDashboardMainCached = unstable_cache(
  async (
    providerCode: ProviderCode | null,
    limit: number | null,
    marketLimitPerEvent: number | null,
    includeNested: boolean,
  ): Promise<DashboardMainData> => {
    const result = await getDashboardMainSource({
      providerCode: providerCode ?? undefined,
      limit: limit ?? undefined,
      marketLimitPerEvent: marketLimitPerEvent ?? undefined,
      includeNested,
    });

    return {
      kpis: result.kpis,
      events: result.events.map(serializeDashboardEvent),
    };
  },
  ["dashboard-main"],
  { revalidate: 60 },
);

const loadDashboardTreemapCached = unstable_cache(
  async (
    providerCode: ProviderCode | null,
    coverage: "all" | "scope",
  ): Promise<TreemapEntry[]> =>
    getDashboardTreemapSource({
      providerCode: providerCode ?? undefined,
      coverage,
    }),
  ["dashboard-treemap"],
  { revalidate: 60 },
);

const loadTopTradesCached = unstable_cache(
  async (
    window: "24h" | "7d" | "30d",
    providerCode: ProviderCode | null,
    limit: number,
    offset: number,
    summaryOnly: boolean,
  ): Promise<TopTradesData> => {
    const result = await getTopTradesSource({
      window,
      providerCode: providerCode ?? undefined,
      limit,
      offset,
      summaryOnly,
    });

    return {
      summary: result.summary,
      trades: result.trades.map(serializeTopTrade),
      pagination: result.pagination,
    };
  },
  ["top-trades"],
  { revalidate: 30 },
);

const loadDashboardBenchmarksCached = unstable_cache(
  async (providerCode: ProviderCode | null): Promise<DashboardBenchmarksData> =>
    getDashboardBenchmarksSource(providerCode ?? undefined),
  ["dashboard-benchmarks"],
  { revalidate: 300 },
);

export async function getCachedDashboardMain(params?: {
  providerCode?: ProviderCode;
  limit?: number;
  marketLimitPerEvent?: number;
  includeNested?: boolean;
}): Promise<DashboardMainData> {
  return loadDashboardMainCached(
    params?.providerCode ?? null,
    params?.limit ?? null,
    params?.marketLimitPerEvent ?? null,
    params?.includeNested ?? true,
  );
}

export async function getCachedDashboardTreemap(params?: {
  providerCode?: ProviderCode;
  coverage?: "all" | "scope";
}): Promise<TreemapEntry[]> {
  return loadDashboardTreemapCached(
    params?.providerCode ?? null,
    params?.coverage ?? "all",
  );
}

export async function getCachedTopTrades(params: {
  window: "24h" | "7d" | "30d";
  providerCode?: ProviderCode;
  limit: number;
  offset: number;
  summaryOnly?: boolean;
}): Promise<TopTradesData> {
  return loadTopTradesCached(
    params.window,
    params.providerCode ?? null,
    params.limit,
    params.offset,
    params.summaryOnly ?? false,
  );
}

export async function getCachedDashboardBenchmarks(
  providerCode?: ProviderCode,
): Promise<DashboardBenchmarksData> {
  return loadDashboardBenchmarksCached(providerCode ?? null);
}
