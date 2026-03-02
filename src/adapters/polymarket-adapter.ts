import { env } from "../config/env.js";
import type {
  AdapterMarketInput,
  AdapterInstrumentInput,
  NormalizedEvent,
  NormalizedInstrument,
  NormalizedMarket,
  NormalizedOpenInterestPoint,
  NormalizedOrderbookTop,
  NormalizedPricePoint,
  NormalizedTradeEvent,
  PriceWindow
} from "../types/domain.js";
import { mapWithConcurrency } from "../utils/async.js";
import { fetchJsonWithRetry } from "../utils/http.js";
import { logger } from "../utils/logger.js";
import { parseEpochToDate } from "../utils/time.js";
import type { ProviderAdapter } from "./types.js";

const DEFAULT_PAGE_LIMIT = 500;

interface GammaMarketRaw {
  conditionId?: unknown;
  eventId?: unknown;
  events?: unknown;
  question?: unknown;
  endDate?: unknown;
  active?: unknown;
  closed?: unknown;
  archived?: unknown;
  volume24hr?: unknown;
  liquidity?: unknown;
  clobTokenIds?: unknown;
  outcomes?: unknown;
  [key: string]: unknown;
}

interface GammaEventRaw {
  id?: unknown;
  title?: unknown;
  category?: unknown;
  startDate?: unknown;
  endDate?: unknown;
  active?: unknown;
  closed?: unknown;
  archived?: unknown;
  [key: string]: unknown;
}

interface PriceHistoryResponse {
  history?: Array<{ t?: unknown; p?: unknown }>;
}

interface OrderLevel {
  price?: unknown;
  size?: unknown;
}

interface OrderbookResponse {
  timestamp?: unknown;
  bids?: OrderLevel[];
  asks?: OrderLevel[];
  [key: string]: unknown;
}

interface ListGammaEntitiesOptions {
  offsetStart?: number;
  maxPages: number;
  queryParams?: Record<string, string>;
}

interface PolymarketTradeRaw {
  transactionHash?: unknown;
  conditionId?: unknown;
  asset?: unknown;
  side?: unknown;
  size?: unknown;
  amount?: unknown;
  usdcSize?: unknown;
  notionalUsd?: unknown;
  notional_usd?: unknown;
  price?: unknown;
  timestamp?: unknown;
  proxyWallet?: unknown;
  [key: string]: unknown;
}

interface PolymarketOpenInterestRaw {
  market?: unknown;
  value?: unknown;
  timestamp?: unknown;
  t?: unknown;
  [key: string]: unknown;
}

export interface PolymarketListState {
  nextOffset: number;
  pagesFetched: number;
  completed: boolean;
  stopReason: "exhausted" | "page_cap" | "repeated_page";
  partialReason: string | null;
}

export interface PolymarketPagedResult<T> {
  items: T[];
  state: PolymarketListState;
}

export interface PolymarketMetadataOptions {
  activeOnly?: boolean;
  offsetStart?: number;
  maxPages?: number;
}

function asString(value: unknown): string | null {
  if (typeof value === "string") {
    const trimmed = value.trim();
    return trimmed.length > 0 ? trimmed : null;
  }
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }
  return null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function asDate(value: unknown): Date | null {
  const raw = asString(value);
  if (!raw) {
    return null;
  }

  const parsed = new Date(raw);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

function asObject(value: unknown): Record<string, unknown> {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function parseJsonArray(value: unknown): unknown[] {
  if (Array.isArray(value)) {
    return value;
  }

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) {
        return parsed;
      }
    } catch {
      return [];
    }
  }

  return [];
}

function clampProbability(rawPrice: number | null): number | null {
  if (rawPrice === null) {
    return null;
  }

  if (rawPrice > 1 && rawPrice <= 100) {
    return rawPrice / 100;
  }

  if (rawPrice >= 0 && rawPrice <= 1) {
    return rawPrice;
  }

  return null;
}

function normalizeStatus(active: unknown, closed: unknown, archived: unknown): "active" | "closed" | "archived" | "unknown" {
  if (archived === true) {
    return "archived";
  }
  if (closed === true) {
    return "closed";
  }
  if (active === true) {
    return "active";
  }
  return "unknown";
}

function buildMetadataQueryParams(activeOnly: boolean): Record<string, string> {
  if (!activeOnly) {
    return {};
  }

  return {
    active: "true",
    closed: "false",
    archived: "false"
  };
}

function extractPolymarketEventRef(raw: GammaMarketRaw): string | null {
  const directEventId = asString(raw.eventId);
  if (directEventId) {
    return directEventId;
  }

  const events = parseJsonArray(raw.events);
  for (const entry of events) {
    const primitiveRef = asString(entry);
    if (primitiveRef) {
      return primitiveRef;
    }

    if (entry && typeof entry === "object" && !Array.isArray(entry)) {
      const eventObj = entry as Record<string, unknown>;
      const nestedRef = asString(eventObj.id) ?? asString(eventObj.eventId) ?? asString(eventObj.event_id);
      if (nestedRef) {
        return nestedRef;
      }
    }
  }

  return null;
}

async function listGammaEntitiesWithState<T extends Record<string, unknown>>(
  baseUrl: string,
  path: string,
  options: ListGammaEntitiesOptions
): Promise<PolymarketPagedResult<T>> {
  const all: T[] = [];
  const seenFirstIds = new Set<string>();

  let nextOffset = options.offsetStart ?? 0;
  let pagesFetched = 0;
  let stopReason: PolymarketListState["stopReason"] = "page_cap";
  let partialReason: string | null = null;
  let completed = false;

  while (pagesFetched < options.maxPages) {
    const url = new URL(path, baseUrl);
    url.searchParams.set("limit", String(DEFAULT_PAGE_LIMIT));
    url.searchParams.set("offset", String(nextOffset));

    for (const [key, value] of Object.entries(options.queryParams ?? {})) {
      url.searchParams.set(key, value);
    }

    const rows = await fetchJsonWithRetry<T[]>(url.toString());
    pagesFetched += 1;

    if (rows.length === 0) {
      stopReason = "exhausted";
      completed = true;
      break;
    }

    const firstId =
      asString(rows[0]?.id) ??
      asString(rows[0]?.conditionId) ??
      asString(rows[0]?.slug) ??
      `offset:${nextOffset}`;

    if (seenFirstIds.has(firstId)) {
      stopReason = "repeated_page";
      completed = true;
      partialReason = `Repeated page detected at offset=${nextOffset}, first_id=${firstId}`;
      logger.warn({ path, nextOffset, firstId }, "Detected repeated page start, stopping pagination safely");
      break;
    }

    seenFirstIds.add(firstId);
    all.push(...rows);
    nextOffset += DEFAULT_PAGE_LIMIT;

    if (rows.length < DEFAULT_PAGE_LIMIT) {
      stopReason = "exhausted";
      completed = true;
      break;
    }
  }

  if (!completed && pagesFetched >= options.maxPages) {
    stopReason = "page_cap";
    partialReason = `Reached page cap (${options.maxPages}) for ${path}`;
    logger.warn({ path, maxPages: options.maxPages }, "Stopped pagination due to configured page cap");
  }

  return {
    items: all,
    state: {
      nextOffset,
      pagesFetched,
      completed,
      stopReason,
      partialReason
    }
  };
}

export class PolymarketAdapter implements ProviderAdapter {
  readonly providerCode = "polymarket" as const;

  async listEventsWithState(options?: PolymarketMetadataOptions): Promise<PolymarketPagedResult<NormalizedEvent>> {
    const activeOnly = options?.activeOnly ?? true;
    const result = await listGammaEntitiesWithState<GammaEventRaw>(env.POLYMARKET_GAMMA_BASE_URL, "/events", {
      offsetStart: options?.offsetStart ?? 0,
      maxPages: options?.maxPages ?? env.POLYMARKET_MAX_PAGES,
      queryParams: buildMetadataQueryParams(activeOnly)
    });

    const normalized: Array<NormalizedEvent | null> = result.items.map((raw) => {
        const eventRef = this.normalizeMarketRef(asString(raw.id) ?? "");
        if (!eventRef) {
          return null;
        }

        return {
          eventRef,
          title: asString(raw.title),
          category: asString(raw.category),
          startTime: asDate(raw.startDate),
          endTime: asDate(raw.endDate),
          status: normalizeStatus(raw.active, raw.closed, raw.archived),
          rawJson: asObject(raw)
        } satisfies NormalizedEvent;
      });

    const items = normalized.filter((item): item is NormalizedEvent => item !== null);

    return {
      items,
      state: result.state
    };
  }

  async listMarketsWithState(options?: PolymarketMetadataOptions): Promise<PolymarketPagedResult<NormalizedMarket>> {
    const activeOnly = options?.activeOnly ?? true;
    const result = await listGammaEntitiesWithState<GammaMarketRaw>(env.POLYMARKET_GAMMA_BASE_URL, "/markets", {
      offsetStart: options?.offsetStart ?? 0,
      maxPages: options?.maxPages ?? env.POLYMARKET_MAX_PAGES,
      queryParams: buildMetadataQueryParams(activeOnly)
    });

    const items = result.items
      .map((raw) => {
        const conditionId = asString(raw.conditionId);
        if (!conditionId) {
          return null;
        }

        return {
          marketRef: this.normalizeMarketRef(conditionId),
          eventRef: (() => {
            const eventRef = extractPolymarketEventRef(raw);
            return eventRef ? this.normalizeMarketRef(eventRef) : null;
          })(),
          title: asString(raw.question),
          status: normalizeStatus(raw.active, raw.closed, raw.archived),
          closeTime: asDate(raw.endDate),
          volume24h: asNumber(raw.volume24hr),
          liquidity: asNumber(raw.liquidity),
          rawJson: asObject(raw)
        } satisfies NormalizedMarket;
      })
      .filter((item): item is NormalizedMarket => item !== null);

    return {
      items,
      state: result.state
    };
  }

  async listEvents(): Promise<NormalizedEvent[]> {
    return (await this.listEventsWithState({ activeOnly: true })).items;
  }

  async listMarkets(): Promise<NormalizedMarket[]> {
    return (await this.listMarketsWithState({ activeOnly: true })).items;
  }

  async listInstruments(markets: NormalizedMarket[]): Promise<NormalizedInstrument[]> {
    const instruments: NormalizedInstrument[] = [];

    for (const market of markets) {
      const tokenIds = parseJsonArray(market.rawJson.clobTokenIds)
        .map((value) => asString(value))
        .filter((value): value is string => value !== null);
      const outcomes = parseJsonArray(market.rawJson.outcomes)
        .map((value) => asString(value))
        .filter((value): value is string => value !== null);

      for (const [index, tokenId] of tokenIds.entries()) {
        instruments.push({
          marketRef: market.marketRef,
          instrumentRef: this.normalizeInstrumentRef(tokenId),
          outcomeLabel: outcomes[index] ?? null,
          outcomeIndex: index,
          isPrimary: index === 0,
          rawJson: {
            tokenId,
            outcomeLabel: outcomes[index] ?? null,
            outcomeIndex: index
          }
        });
      }
    }

    return instruments;
  }

  async listPricePoints(instruments: AdapterInstrumentInput[], window: PriceWindow): Promise<NormalizedPricePoint[]> {
    const responses = await mapWithConcurrency(instruments, 4, async (instrument) => {
      const url = new URL("/prices-history", env.POLYMARKET_CLOB_BASE_URL);
      url.searchParams.set("market", instrument.instrumentRef);
      url.searchParams.set("startTs", String(window.startTs));
      url.searchParams.set("endTs", String(window.endTs));
      url.searchParams.set("fidelity", "5");

      try {
        const body = await fetchJsonWithRetry<PriceHistoryResponse>(url.toString(), undefined, {
          maxAttempts: 2,
          baseDelayMs: 250,
          logRetries: false
        });
        return { instrument, body };
      } catch (error) {
        logger.debug({ instrumentRef: instrument.instrumentRef, error }, "Failed to fetch prices-history");
        return { instrument, body: { history: [] } };
      }
    });

    const points: NormalizedPricePoint[] = [];

    for (const { instrument, body } of responses) {
      for (const point of body.history ?? []) {
        const timestampNumber = asNumber(point.t);
        const price = clampProbability(asNumber(point.p));

        if (timestampNumber === null || price === null) {
          continue;
        }

        points.push({
          instrumentRef: instrument.instrumentRef,
          ts: parseEpochToDate(timestampNumber),
          price,
          source: "polymarket:prices-history"
        });
      }
    }

    return points;
  }

  async listOrderbookTop(instruments: AdapterInstrumentInput[]): Promise<NormalizedOrderbookTop[]> {
    const snapshots = await mapWithConcurrency(instruments, 4, async (instrument) => {
      const url = new URL("/book", env.POLYMARKET_CLOB_BASE_URL);
      url.searchParams.set("token_id", instrument.instrumentRef);

      try {
        const body = await fetchJsonWithRetry<OrderbookResponse>(url.toString(), undefined, {
          maxAttempts: 2,
          baseDelayMs: 250,
          logRetries: false
        });

        const bids = Array.isArray(body.bids) ? body.bids : [];
        const asks = Array.isArray(body.asks) ? body.asks : [];

        const bidPrices = bids.map((level) => asNumber(level.price)).filter((value): value is number => value !== null);
        const askPrices = asks.map((level) => asNumber(level.price)).filter((value): value is number => value !== null);

        const bestBid = bidPrices.length > 0 ? Math.max(...bidPrices) : null;
        const bestAsk = askPrices.length > 0 ? Math.min(...askPrices) : null;
        const spread = bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null;

        const bidDepthTop5 = bids
          .slice(0, 5)
          .map((level) => asNumber(level.size) ?? 0)
          .reduce((acc, value) => acc + value, 0);

        const askDepthTop5 = asks
          .slice(0, 5)
          .map((level) => asNumber(level.size) ?? 0)
          .reduce((acc, value) => acc + value, 0);

        const timestamp = asNumber(body.timestamp);

        return {
          instrumentRef: instrument.instrumentRef,
          ts: timestamp !== null ? parseEpochToDate(timestamp) : new Date(),
          bestBid: clampProbability(bestBid),
          bestAsk: clampProbability(bestAsk),
          spread: spread === null ? null : clampProbability(spread),
          bidDepthTop5,
          askDepthTop5,
          rawJson: asObject(body)
        } satisfies NormalizedOrderbookTop;
      } catch (error) {
        logger.debug({ instrumentRef: instrument.instrumentRef, error }, "Failed to fetch orderbook");

        return {
          instrumentRef: instrument.instrumentRef,
          ts: new Date(),
          bestBid: null,
          bestAsk: null,
          spread: null,
          bidDepthTop5: null,
          askDepthTop5: null,
          rawJson: {
            error: error instanceof Error ? error.message : "unknown"
          }
        } satisfies NormalizedOrderbookTop;
      }
    });

    return snapshots;
  }

  async listTrades(markets: AdapterMarketInput[], window: PriceWindow): Promise<NormalizedTradeEvent[]> {
    const pageLimit = 500;
    const points: NormalizedTradeEvent[] = [];
    const startMs = window.startTs * 1000;
    const endMs = window.endTs * 1000;

    await mapWithConcurrency(markets, 2, async (marketInput) => {
      for (let page = 0; page < env.POLYMARKET_TRADES_MAX_PAGES; page += 1) {
        const offset = page * pageLimit;

        const url = new URL("/trades", env.POLYMARKET_DATA_BASE_URL);
        url.searchParams.set("market", marketInput.marketRef);

        if (env.POLYMARKET_TRADES_FILTER_MODE === "large_cash") {
          url.searchParams.set("filterType", "CASH");
          url.searchParams.set("filterAmount", String(env.POLYMARKET_LARGE_BET_USD_THRESHOLD));
        }

        url.searchParams.set("after", String(window.startTs));
        url.searchParams.set("before", String(window.endTs));
        url.searchParams.set("limit", String(pageLimit));
        url.searchParams.set("offset", String(offset));

        let body: unknown;
        try {
          body = await fetchJsonWithRetry<unknown>(url.toString(), undefined, {
            maxAttempts: 3,
            baseDelayMs: 250,
            logRetries: false
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          logger.warn({ marketRef: marketInput.marketRef, page, error: message }, "Failed to fetch Polymarket trades page");
          break;
        }

        const rows = Array.isArray(body)
          ? (body as PolymarketTradeRaw[])
          : Array.isArray((body as Record<string, unknown>)?.trades)
            ? ((body as Record<string, unknown>).trades as PolymarketTradeRaw[])
            : Array.isArray((body as Record<string, unknown>)?.data)
              ? ((body as Record<string, unknown>).data as PolymarketTradeRaw[])
              : [];

        if (rows.length === 0) {
          break;
        }

        for (const raw of rows) {
          const conditionId = this.normalizeMarketRef(asString(raw.conditionId) ?? marketInput.marketRef);
          if (!conditionId) {
            continue;
          }

          const timestamp = asNumber(raw.timestamp);
          if (timestamp === null) {
            continue;
          }

          const tradeTs = parseEpochToDate(timestamp);
          const tradeMs = tradeTs.getTime();
          if (tradeMs < startMs || tradeMs > endMs) {
            continue;
          }

          const price = clampProbability(asNumber(raw.price));
          const qty = asNumber(raw.size) ?? asNumber(raw.amount);
          const explicitNotionalUsd = asNumber(raw.notionalUsd) ?? asNumber(raw.notional_usd) ?? asNumber(raw.usdcSize);
          const asset = asString(raw.asset);
          const txHash = asString(raw.transactionHash);
          const tradeRefBase = txHash ?? `${conditionId}:${timestamp}:${qty ?? "na"}:${price ?? "na"}`;
          const tradeRef = `${tradeRefBase}:${asset ?? "na"}:${timestamp}`;

          points.push({
            tradeRef,
            marketRef: conditionId,
            instrumentRef: asset ? this.normalizeInstrumentRef(asset) : null,
            ts: tradeTs,
            side: asString(raw.side)?.toLowerCase() ?? null,
            price,
            qty,
            notionalUsd: explicitNotionalUsd ?? (qty !== null && price !== null ? qty * price : null),
            traderRef: asString(raw.proxyWallet),
            rawJson: asObject(raw),
            source: "polymarket:data-trades"
          });
        }

        if (rows.length < pageLimit) {
          break;
        }
      }
    });

    return points;
  }

  async listOpenInterest(markets: AdapterMarketInput[], _window: PriceWindow): Promise<NormalizedOpenInterestPoint[]> {
    const points: NormalizedOpenInterestPoint[] = [];

    await mapWithConcurrency(markets, 4, async (marketInput) => {
      const url = new URL("/oi", env.POLYMARKET_DATA_BASE_URL);
      url.searchParams.set("market", marketInput.marketRef);

      let body: unknown;
      try {
        body = await fetchJsonWithRetry<unknown>(url.toString(), undefined, {
          maxAttempts: 3,
          baseDelayMs: 250,
          logRetries: false
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        logger.warn({ marketRef: marketInput.marketRef, error: message }, "Failed to fetch Polymarket open interest");
        return;
      }

      const rows = Array.isArray(body) ? (body as PolymarketOpenInterestRaw[]) : [body as PolymarketOpenInterestRaw];

      for (const raw of rows) {
        const value = asNumber(raw.value);
        if (value === null) {
          continue;
        }

        const pointMarketRef = this.normalizeMarketRef(asString(raw.market) ?? marketInput.marketRef);
        if (!pointMarketRef) {
          continue;
        }

        const tsRaw = asNumber(raw.timestamp) ?? asNumber(raw.t);
        points.push({
          marketRef: pointMarketRef,
          ts: tsRaw === null ? new Date() : parseEpochToDate(tsRaw),
          value,
          unit: "native",
          rawJson: asObject(raw),
          source: "polymarket:oi"
        });
      }
    });

    return points;
  }

  normalizeMarketRef(raw: string): string {
    return raw.trim().toLowerCase();
  }

  normalizeInstrumentRef(raw: string): string {
    return raw.trim();
  }
}
