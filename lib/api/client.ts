import type {
  ApiEnvelope,
  PaginatedEnvelope,
  DashboardMainData,
  TreemapEntry,
  MarketSummary,
  MarketDetailData,
  EventDetailData,
  EventLatestTradesData,
  TopTradesData,
  PriceHistoryData,
  EventPriceHistoryData,
} from "./types";

const BASE_URL =
  process.env.PREDICTION_API_URL ??
  `http://127.0.0.1:${process.env.PORT ?? "3000"}`;

async function fetchApi<T>(
  path: string,
  params?: Record<string, string | undefined>,
  options?: { revalidate?: number },
): Promise<T> {
  const url = new URL(path, BASE_URL);
  if (params) {
    for (const [k, v] of Object.entries(params)) {
      if (v !== undefined && v !== "") url.searchParams.set(k, v);
    }
  }

  const res = await fetch(url.toString(), {
    next: { revalidate: options?.revalidate ?? 60 },
  });

  if (!res.ok) {
    throw new Error(`API ${res.status}: ${res.statusText} – ${url.pathname}`);
  }

  return res.json() as Promise<T>;
}

/* ── Dashboard ── */

export async function getDashboardMain(provider?: string) {
  return fetchApi<ApiEnvelope<DashboardMainData>>("/v1/dashboard/main", {
    provider,
  });
}

export async function getDashboardTreemap(params?: {
  provider?: string;
  coverage?: string;
}) {
  return fetchApi<ApiEnvelope<TreemapEntry[]>>("/v1/dashboard/treemap", params);
}

/* ── Markets ── */

export async function listMarkets(params?: {
  provider?: string;
  status?: "active" | "all";
  limit?: string;
  offset?: string;
}) {
  return fetchApi<PaginatedEnvelope<MarketSummary[]>>("/v1/markets", params);
}

export async function getMarketDetail(marketUid: string) {
  return fetchApi<ApiEnvelope<MarketDetailData>>(
    `/v1/markets/${encodeURIComponent(marketUid)}`,
  );
}

export async function getMarketPriceHistory(
  marketUid: string,
  params?: { from?: string; to?: string; interval?: string },
) {
  return fetchApi<ApiEnvelope<PriceHistoryData>>(
    `/v1/markets/${encodeURIComponent(marketUid)}/price-history`,
    params,
  );
}

/* ── Top Trades ── */

export async function getTopTrades(params?: {
  window?: string;
  provider?: string;
  limit?: string;
  offset?: string;
}) {
  return fetchApi<ApiEnvelope<TopTradesData>>("/v1/trades/top", params);
}

/* ── Events ── */

export async function getEventDetail(eventUid: string) {
  return fetchApi<ApiEnvelope<EventDetailData>>(
    `/v1/events/${encodeURIComponent(eventUid)}`,
  );
}

export async function getEventLatestTrades(
  eventUid: string,
  params?: { limit?: string },
) {
  return fetchApi<ApiEnvelope<EventLatestTradesData>>(
    `/v1/events/${encodeURIComponent(eventUid)}/trades`,
    params,
  );
}

export async function getEventPriceHistory(
  eventUid: string,
  params?: { from?: string; to?: string; interval?: string },
) {
  return fetchApi<ApiEnvelope<EventPriceHistoryData>>(
    `/v1/events/${encodeURIComponent(eventUid)}/price-history`,
    params,
  );
}
