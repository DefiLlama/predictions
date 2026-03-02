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
import { chunkArray } from "../utils/chunk.js";
import { fetchJsonWithRetry } from "../utils/http.js";
import { logger } from "../utils/logger.js";
import { parseEpochToDate } from "../utils/time.js";
import type { ProviderAdapter } from "./types.js";

interface KalshiEventRaw {
  event_ticker?: unknown;
  title?: unknown;
  category?: unknown;
  strike_date?: unknown;
  status?: unknown;
  [key: string]: unknown;
}

interface KalshiMarketRaw {
  ticker?: unknown;
  event_ticker?: unknown;
  title?: unknown;
  close_time?: unknown;
  expiration_time?: unknown;
  status?: unknown;
  volume_24h?: unknown;
  volume_24h_fp?: unknown;
  liquidity?: unknown;
  open_interest?: unknown;
  open_interest_fp?: unknown;
  updated_time?: unknown;
  [key: string]: unknown;
}

interface KalshiCandleRaw {
  end_period_ts?: unknown;
  price?: {
    close?: unknown;
    close_dollars?: unknown;
  };
}

interface KalshiBatchCandlesticksResponse {
  markets?: Array<{
    market_ticker?: unknown;
    candlesticks?: KalshiCandleRaw[];
  }>;
  error?: {
    code?: unknown;
    message?: unknown;
    details?: unknown;
  };
}

interface KalshiOrderbookResponse {
  orderbook?: {
    yes?: Array<[number, number]>;
    no?: Array<[number, number]>;
  };
  orderbook_fp?: {
    yes_dollars?: Array<[string, string]>;
    no_dollars?: Array<[string, string]>;
  };
  [key: string]: unknown;
}

interface KalshiTradeRaw {
  trade_id?: unknown;
  ticker?: unknown;
  price?: unknown;
  yes_price?: unknown;
  yes_price_dollars?: unknown;
  count?: unknown;
  count_fp?: unknown;
  taker_side?: unknown;
  created_time?: unknown;
  [key: string]: unknown;
}

interface KalshiTradesResponse {
  trades?: KalshiTradeRaw[];
  cursor?: unknown;
  next_cursor?: unknown;
}

interface KalshiMarketResponse {
  market?: KalshiMarketRaw;
}

export interface KalshiListState {
  nextCursor: string | null;
  pagesFetched: number;
  completed: boolean;
  stopReason: "exhausted" | "request_budget" | "time_budget";
  partialReason: string | null;
}

export interface KalshiMetadataOptions {
  cursorStart?: string | null;
  maxRequests?: number;
  runBudgetMs?: number;
}

export interface KalshiPagedResult<T> {
  items: T[];
  state: KalshiListState;
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

function normalizeKalshiPrice(value: unknown): number | null {
  const numeric = asNumber(value);
  if (numeric === null) {
    return null;
  }

  if (numeric >= 0 && numeric <= 1) {
    return numeric;
  }

  if (numeric >= 0 && numeric <= 100) {
    return numeric / 100;
  }

  return null;
}

function normalizeStatus(status: string | null): "active" | "closed" | "archived" | "unknown" {
  if (!status) {
    return "unknown";
  }

  const normalized = status.toLowerCase();
  if (["open", "active", "initialized"].includes(normalized)) {
    return "active";
  }

  if (["closed", "settled", "finalized", "resolved", "expired"].includes(normalized)) {
    return "closed";
  }

  if (["archived", "cancelled"].includes(normalized)) {
    return "archived";
  }

  return "unknown";
}

async function fetchKalshiCursorPagesWithState<T>(
  path: string,
  listKey: string,
  options?: {
    query?: Record<string, string>;
    pageLimit?: number;
    cursorStart?: string | null;
    maxRequests?: number;
    runBudgetMs?: number;
  }
): Promise<KalshiPagedResult<T>> {
  const all: T[] = [];
  let cursor: string | null = options?.cursorStart ?? null;
  const maxRequests = options?.maxRequests ?? env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN;
  const runBudgetMs = options?.runBudgetMs ?? env.KALSHI_METADATA_RUN_BUDGET_MS;
  const startedAt = Date.now();
  const pageLimit = options?.pageLimit ?? 200;
  let pagesFetched = 0;
  let completed = false;
  let stopReason: KalshiListState["stopReason"] = "request_budget";
  let partialReason: string | null = null;

  for (let request = 1; request <= maxRequests; request += 1) {
    if (pagesFetched > 0 && Date.now() - startedAt >= runBudgetMs) {
      stopReason = "time_budget";
      partialReason = `Reached run budget (${runBudgetMs}ms) for ${path}`;
      logger.warn({ path, runBudgetMs }, "Stopped Kalshi pagination due run budget");
      break;
    }

    const url = buildKalshiUrl(path);
    url.searchParams.set("limit", String(pageLimit));

    for (const [key, value] of Object.entries(options?.query ?? {})) {
      url.searchParams.set(key, value);
    }

    if (cursor) {
      url.searchParams.set("cursor", cursor);
    }

    const body = await fetchJsonWithRetry<Record<string, unknown>>(url.toString(), undefined, {
      maxAttempts: 3,
      baseDelayMs: 250
    });
    pagesFetched += 1;

    const rows = Array.isArray(body[listKey]) ? (body[listKey] as T[]) : [];
    all.push(...rows);

    const nextCursor = asString(body.next_cursor) ?? asString(body.cursor);
    if (!nextCursor) {
      completed = true;
      stopReason = "exhausted";
      break;
    }

    if (nextCursor === cursor) {
      completed = true;
      stopReason = "exhausted";
      break;
    }

    cursor = nextCursor;
  }

  if (!completed && partialReason === null && pagesFetched >= maxRequests) {
    stopReason = "request_budget";
    partialReason = `Reached request budget (${maxRequests}) for ${path}`;
    logger.warn({ path, maxRequests }, "Stopped Kalshi pagination due request budget");
  }

  return {
    items: all,
    state: {
      nextCursor: cursor,
      pagesFetched,
      completed,
      stopReason,
      partialReason
    }
  };
}

function buildKalshiUrl(path: string): URL {
  const normalizedBase = env.KALSHI_BASE_URL.endsWith("/") ? env.KALSHI_BASE_URL : `${env.KALSHI_BASE_URL}/`;
  const normalizedPath = path.replace(/^\/+/, "");
  return new URL(normalizedPath, normalizedBase);
}

export class KalshiAdapter implements ProviderAdapter {
  readonly providerCode = "kalshi" as const;

  async listEvents(): Promise<NormalizedEvent[]> {
    return (await this.listEventsWithState()).items;
  }

  async listEventsWithState(options?: KalshiMetadataOptions): Promise<KalshiPagedResult<NormalizedEvent>> {
    const eventsResult = await fetchKalshiCursorPagesWithState<KalshiEventRaw>("events", "events", {
      query: {
        status: "open"
      },
      pageLimit: 200,
      cursorStart: options?.cursorStart,
      maxRequests: options?.maxRequests ?? env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
      runBudgetMs: options?.runBudgetMs ?? env.KALSHI_METADATA_RUN_BUDGET_MS
    });

    const normalized: Array<NormalizedEvent | null> = eventsResult.items.map((raw) => {
        const eventTicker = asString(raw.event_ticker);
        if (!eventTicker) {
          return null;
        }

        return {
          eventRef: this.normalizeMarketRef(eventTicker),
          title: asString(raw.title),
          category: asString(raw.category),
          startTime: null,
          endTime: asDate(raw.strike_date),
          status: asString(raw.status),
          rawJson: asObject(raw)
        } satisfies NormalizedEvent;
      });

    return {
      items: normalized.filter((item): item is NormalizedEvent => item !== null),
      state: eventsResult.state
    };
  }

  async listMarkets(): Promise<NormalizedMarket[]> {
    return (await this.listMarketsWithState()).items;
  }

  async listMarketsWithState(options?: KalshiMetadataOptions): Promise<KalshiPagedResult<NormalizedMarket>> {
    const marketsResult = await fetchKalshiCursorPagesWithState<KalshiMarketRaw>("markets", "markets", {
      query: {
        status: "open",
        mve_filter: "exclude"
      },
      pageLimit: 1000,
      cursorStart: options?.cursorStart,
      maxRequests: options?.maxRequests ?? env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
      runBudgetMs: options?.runBudgetMs ?? env.KALSHI_METADATA_RUN_BUDGET_MS
    });

    const items = marketsResult.items
      .map((raw) => {
        const ticker = asString(raw.ticker);
        const eventTicker = asString(raw.event_ticker);
        if (!ticker) {
          return null;
        }

        return {
          marketRef: this.normalizeMarketRef(ticker),
          eventRef: eventTicker ? this.normalizeMarketRef(eventTicker) : null,
          title: asString(raw.title),
          status: normalizeStatus(asString(raw.status)),
          closeTime: asDate(raw.close_time) ?? asDate(raw.expiration_time),
          volume24h: asNumber(raw.volume_24h_fp) ?? asNumber(raw.volume_24h),
          liquidity: asNumber(raw.liquidity),
          rawJson: asObject(raw)
        } satisfies NormalizedMarket;
      })
      .filter((item): item is NormalizedMarket => item !== null);

    return {
      items,
      state: marketsResult.state
    };
  }

  async listMultivariateEventsWithState(options?: KalshiMetadataOptions): Promise<KalshiPagedResult<NormalizedEvent>> {
    const eventsResult = await fetchKalshiCursorPagesWithState<KalshiEventRaw>("events/multivariate", "events", {
      pageLimit: 200,
      cursorStart: options?.cursorStart,
      maxRequests: options?.maxRequests ?? env.KALSHI_MULTIVARIATE_MAX_REQUESTS_PER_RUN,
      runBudgetMs: options?.runBudgetMs ?? env.KALSHI_MULTIVARIATE_RUN_BUDGET_MS
    });

    const normalized: Array<NormalizedEvent | null> = eventsResult.items.map((raw) => {
      const eventTicker = asString(raw.event_ticker);
      if (!eventTicker) {
        return null;
      }

      return {
        eventRef: this.normalizeMarketRef(eventTicker),
        title: asString(raw.title),
        category: asString(raw.category),
        startTime: null,
        endTime: asDate(raw.strike_date),
        status: asString(raw.status),
        rawJson: asObject(raw)
      } satisfies NormalizedEvent;
    });

    return {
      items: normalized.filter((item): item is NormalizedEvent => item !== null),
      state: eventsResult.state
    };
  }

  async listEventsByTickers(eventTickers: string[]): Promise<NormalizedEvent[]> {
    const uniqueTickers = Array.from(new Set(eventTickers.map((ticker) => ticker.trim()).filter((ticker) => ticker.length > 0)));
    const rows: Array<NormalizedEvent | null> = await mapWithConcurrency(
      uniqueTickers,
      env.KALSHI_EVENT_LOOKUP_CONCURRENCY,
      async (eventTicker) => {
      const url = buildKalshiUrl(`events/${encodeURIComponent(eventTicker)}`);
      try {
        const body = await fetchJsonWithRetry<Record<string, unknown>>(url.toString(), undefined, {
          maxAttempts: 3,
          baseDelayMs: 250,
          logRetries: false
        });
        const eventRaw = asObject(body.event);
        if (Object.keys(eventRaw).length === 0) {
          return null;
        }

        return {
          eventRef: this.normalizeMarketRef(asString(eventRaw.event_ticker) ?? eventTicker),
          title: asString(eventRaw.title),
          category: asString(eventRaw.category),
          startTime: null,
          endTime: asDate(eventRaw.strike_date),
          status: asString(eventRaw.status),
          rawJson: eventRaw
        } satisfies NormalizedEvent;
      } catch (error) {
        logger.debug({ eventTicker, error }, "Failed to fetch Kalshi event by ticker");
        return null;
      }
      }
    );

    return rows.filter((item): item is NormalizedEvent => item !== null);
  }

  async listInstruments(markets: NormalizedMarket[]): Promise<NormalizedInstrument[]> {
    const instruments: NormalizedInstrument[] = [];

    for (const market of markets) {
      instruments.push({
        marketRef: market.marketRef,
        instrumentRef: this.normalizeInstrumentRef(`${market.marketRef}:YES`),
        outcomeLabel: "YES",
        outcomeIndex: 0,
        isPrimary: true,
        rawJson: {
          synthetic: true,
          side: "YES"
        }
      });

      instruments.push({
        marketRef: market.marketRef,
        instrumentRef: this.normalizeInstrumentRef(`${market.marketRef}:NO`),
        outcomeLabel: "NO",
        outcomeIndex: 1,
        isPrimary: false,
        rawJson: {
          synthetic: true,
          side: "NO"
        }
      });
    }

    return instruments;
  }

  async listPricePoints(instruments: AdapterInstrumentInput[], window: PriceWindow): Promise<NormalizedPricePoint[]> {
    const points: NormalizedPricePoint[] = [];
    const wantedInstrumentRefs = new Set(instruments.map((item) => item.instrumentRef));
    const marketRefs = Array.from(new Set(instruments.map((item) => item.marketRef)));
    const periodIntervalMinutes = 60;
    const maxCandlesticksPerResponse = 10_000;
    const bucketsPerMarket = Math.max(1, Math.ceil((window.endTs - window.startTs) / (periodIntervalMinutes * 60)));
    const marketsPerRequest = Math.max(1, Math.min(100, Math.floor(maxCandlesticksPerResponse / bucketsPerMarket)));

    const marketChunks = chunkArray(marketRefs, marketsPerRequest);
    const chunkPoints = await mapWithConcurrency(
      marketChunks,
      env.KALSHI_PRICE_BATCH_CONCURRENCY,
      async (chunk): Promise<NormalizedPricePoint[]> => {
        const url = buildKalshiUrl("markets/candlesticks");
        url.searchParams.set("market_tickers", chunk.join(","));
        url.searchParams.set("start_ts", String(window.startTs));
        url.searchParams.set("end_ts", String(window.endTs));
        url.searchParams.set("period_interval", String(periodIntervalMinutes));

        let body: KalshiBatchCandlesticksResponse;
        try {
          body = await fetchJsonWithRetry<KalshiBatchCandlesticksResponse>(url.toString(), undefined, {
            maxAttempts: 3,
            baseDelayMs: 250,
            logRetries: false
          });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          logger.warn({ markets: chunk.length, error: message }, "Failed to fetch Kalshi candlesticks batch");
          return [];
        }

        if (body.error) {
          logger.warn(
            {
              markets: chunk.length,
              errorCode: asString(body.error.code),
              errorMessage: asString(body.error.message),
              errorDetails: asString(body.error.details)
            },
            "Kalshi candlesticks API returned error payload"
          );
          return [];
        }

        const chunkResult: NormalizedPricePoint[] = [];

        for (const marketData of body.markets ?? []) {
          const marketTicker = this.normalizeMarketRef(asString(marketData.market_ticker) ?? "");
          if (!marketTicker) {
            continue;
          }

          const yesRef = `${marketTicker}:YES`;
          const noRef = `${marketTicker}:NO`;

          for (const candle of marketData.candlesticks ?? []) {
            const endTs = asNumber(candle.end_period_ts);
            if (endTs === null) {
              continue;
            }

            const yesPrice = normalizeKalshiPrice(candle.price?.close ?? candle.price?.close_dollars);
            if (yesPrice === null) {
              continue;
            }

            if (wantedInstrumentRefs.has(yesRef)) {
              chunkResult.push({
                instrumentRef: yesRef,
                ts: parseEpochToDate(endTs),
                price: yesPrice,
                source: "kalshi:markets-candlesticks"
              });
            }

            if (wantedInstrumentRefs.has(noRef)) {
              chunkResult.push({
                instrumentRef: noRef,
                ts: parseEpochToDate(endTs),
                price: Math.max(0, Math.min(1, 1 - yesPrice)),
                source: "kalshi:markets-candlesticks"
              });
            }
          }
        }

        return chunkResult;
      }
    );

    for (const rows of chunkPoints) {
      points.push(...rows);
    }

    return points;
  }

  async listOrderbookTop(instruments: AdapterInstrumentInput[]): Promise<NormalizedOrderbookTop[]> {
    const marketRefs = Array.from(new Set(instruments.map((item) => item.marketRef)));

    const snapshotsByMarket = await mapWithConcurrency(marketRefs, env.KALSHI_ORDERBOOK_CONCURRENCY, async (marketRef) => {
      const url = buildKalshiUrl(`markets/${encodeURIComponent(marketRef)}/orderbook`);
      url.searchParams.set("depth", "5");

      try {
        const body = await fetchJsonWithRetry<KalshiOrderbookResponse>(url.toString(), undefined, {
          maxAttempts: 3,
          baseDelayMs: 250,
          logRetries: false
        });

        const yesLevels = Array.isArray(body.orderbook?.yes) ? body.orderbook?.yes ?? [] : [];
        const noLevels = Array.isArray(body.orderbook?.no) ? body.orderbook?.no ?? [] : [];

        const yesBid = yesLevels.length > 0 ? Math.max(...yesLevels.map((level) => level[0])) / 100 : null;
        const noBid = noLevels.length > 0 ? Math.max(...noLevels.map((level) => level[0])) / 100 : null;

        const yesAsk = noBid === null ? null : Math.max(0, Math.min(1, 1 - noBid));
        const noAsk = yesBid === null ? null : Math.max(0, Math.min(1, 1 - yesBid));

        const yesDepthTop5 = yesLevels.slice(0, 5).reduce((sum, level) => sum + Number(level[1] ?? 0), 0);
        const noDepthTop5 = noLevels.slice(0, 5).reduce((sum, level) => sum + Number(level[1] ?? 0), 0);

        return {
          marketRef,
          ts: new Date(),
          yes: {
            bestBid: yesBid,
            bestAsk: yesAsk,
            spread: yesBid !== null && yesAsk !== null ? yesAsk - yesBid : null,
            depthTop5: yesDepthTop5
          },
          no: {
            bestBid: noBid,
            bestAsk: noAsk,
            spread: noBid !== null && noAsk !== null ? noAsk - noBid : null,
            depthTop5: noDepthTop5
          },
          rawJson: asObject(body)
        };
      } catch (error) {
        logger.debug({ marketRef, error }, "Failed to fetch Kalshi orderbook");

        return {
          marketRef,
          ts: new Date(),
          yes: {
            bestBid: null,
            bestAsk: null,
            spread: null,
            depthTop5: null
          },
          no: {
            bestBid: null,
            bestAsk: null,
            spread: null,
            depthTop5: null
          },
          rawJson: {
            error: error instanceof Error ? error.message : "unknown"
          }
        };
      }
    });

    const points: NormalizedOrderbookTop[] = [];

    for (const item of snapshotsByMarket) {
      points.push({
        instrumentRef: `${item.marketRef}:YES`,
        ts: item.ts,
        bestBid: item.yes.bestBid,
        bestAsk: item.yes.bestAsk,
        spread: item.yes.spread,
        bidDepthTop5: item.yes.depthTop5,
        askDepthTop5: item.no.depthTop5,
        rawJson: item.rawJson
      });

      points.push({
        instrumentRef: `${item.marketRef}:NO`,
        ts: item.ts,
        bestBid: item.no.bestBid,
        bestAsk: item.no.bestAsk,
        spread: item.no.spread,
        bidDepthTop5: item.no.depthTop5,
        askDepthTop5: item.yes.depthTop5,
        rawJson: item.rawJson
      });
    }

    return points;
  }

  async listTrades(markets: AdapterMarketInput[], window: PriceWindow): Promise<NormalizedTradeEvent[]> {
    const points: NormalizedTradeEvent[] = [];
    const runBudgetMs = env.KALSHI_TRADES_RUN_BUDGET_MS;
    const maxRequests = env.KALSHI_TRADES_MAX_REQUESTS_PER_RUN;

    await mapWithConcurrency(markets, env.KALSHI_TRADES_MARKET_CONCURRENCY, async (marketInput) => {
      let cursor: string | null = null;
      let requestsUsed = 0;
      const startedAt = Date.now();

      for (let page = 0; page < maxRequests; page += 1) {
        if (requestsUsed > 0 && Date.now() - startedAt >= runBudgetMs) {
          logger.warn({ marketRef: marketInput.marketRef, runBudgetMs }, "Stopped Kalshi trades pagination due run budget");
          break;
        }

        const url = buildKalshiUrl("markets/trades");
        url.searchParams.set("ticker", marketInput.marketRef);
        url.searchParams.set("min_ts", String(window.startTs));
        url.searchParams.set("max_ts", String(window.endTs));
        url.searchParams.set("limit", "1000");
        if (cursor) {
          url.searchParams.set("cursor", cursor);
        }

        let body: KalshiTradesResponse;
        try {
          body = await fetchJsonWithRetry<KalshiTradesResponse>(url.toString(), undefined, {
            maxAttempts: 3,
            baseDelayMs: 250,
            logRetries: false
          });
          requestsUsed += 1;
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          logger.warn({ marketRef: marketInput.marketRef, page, error: message }, "Failed to fetch Kalshi trades page");
          break;
        }

        const rows = Array.isArray(body.trades) ? body.trades : [];
        if (rows.length === 0) {
          break;
        }

        for (const raw of rows) {
          const marketRef = this.normalizeMarketRef(asString(raw.ticker) ?? marketInput.marketRef);
          if (!marketRef) {
            continue;
          }

          const createdAt = asDate(raw.created_time);
          if (!createdAt) {
            continue;
          }

          const side = asString(raw.taker_side)?.toLowerCase() ?? null;
          const instrumentRef =
            side === "yes" || side === "no" ? this.normalizeInstrumentRef(`${marketRef}:${side.toUpperCase()}`) : null;

          const price = normalizeKalshiPrice(raw.yes_price_dollars ?? raw.yes_price ?? raw.price);
          const qty = asNumber(raw.count_fp) ?? asNumber(raw.count);

          points.push({
            tradeRef: asString(raw.trade_id) ?? `${marketRef}:${createdAt.toISOString()}:${price ?? "na"}:${qty ?? "na"}`,
            marketRef,
            instrumentRef,
            ts: createdAt,
            side,
            price,
            qty,
            notionalUsd: qty !== null && price !== null ? qty * price : null,
            traderRef: null,
            rawJson: asObject(raw),
            source: "kalshi:markets-trades"
          });
        }

        const nextCursor = asString(body.next_cursor) ?? asString(body.cursor);
        if (!nextCursor) {
          break;
        }
        cursor = nextCursor;
      }
    });

    return points;
  }

  async listOpenInterest(markets: AdapterMarketInput[], _window: PriceWindow): Promise<NormalizedOpenInterestPoint[]> {
    const points: NormalizedOpenInterestPoint[] = [];

    await mapWithConcurrency(markets, env.KALSHI_OI_MARKET_CONCURRENCY, async (marketInput) => {
      const url = buildKalshiUrl(`markets/${encodeURIComponent(marketInput.marketRef)}`);

      let body: KalshiMarketResponse;
      try {
        body = await fetchJsonWithRetry<KalshiMarketResponse>(url.toString(), undefined, {
          maxAttempts: 3,
          baseDelayMs: 250,
          logRetries: false
        });
      } catch (error) {
        logger.debug({ marketRef: marketInput.marketRef, error }, "Failed to fetch Kalshi market for OI");
        return;
      }

      const marketRaw = asObject(body.market);
      const value = asNumber(marketRaw.open_interest_fp) ?? asNumber(marketRaw.open_interest);
      if (value === null) {
        return;
      }

      points.push({
        marketRef: this.normalizeMarketRef(asString(marketRaw.ticker) ?? marketInput.marketRef),
        ts: asDate(marketRaw.updated_time) ?? new Date(),
        value,
        unit: "contracts",
        rawJson: marketRaw,
        source: "kalshi:market"
      });
    });

    return points;
  }

  normalizeMarketRef(raw: string): string {
    return raw.trim();
  }

  normalizeInstrumentRef(raw: string): string {
    return raw.trim();
  }
}
