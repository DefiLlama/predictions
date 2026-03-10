import { and, asc, count, desc, eq, inArray, isNull, or, sql } from "drizzle-orm";

import { db } from "../db/client.js";
import {
  categoryDim,
  event,
  instrument,
  market,
  marketCategoryAssignment,
  marketCategorySnapshot1h,
  marketLiquidity1h,
  marketPrice1h,
  marketScope,
  oiPoint5m,
  orderbookTop,
  platform,
  pricePoint,
  providerCategoryDim,
  providerCategoryMap,
  tradeEvent
} from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";
import { getCategoryQualitySummary } from "./category-service.js";

export function parseMarketUid(marketUid: string): { providerCode: string; marketRef: string } | null {
  const delimiterIndex = marketUid.indexOf(":");
  if (delimiterIndex <= 0 || delimiterIndex >= marketUid.length - 1) {
    return null;
  }

  const providerCode = marketUid.slice(0, delimiterIndex);
  const marketRef = marketUid.slice(delimiterIndex + 1);

  return { providerCode, marketRef };
}

export function parseEventUid(eventUid: string): { providerCode: string; eventRef: string } | null {
  const parsed = parseMarketUid(eventUid);
  if (!parsed) {
    return null;
  }

  return {
    providerCode: parsed.providerCode,
    eventRef: parsed.marketRef
  };
}

export const EVENT_TRADES_DEFAULT_LIMIT = 50;
export const EVENT_TRADES_MAX_LIMIT = 100;
const EVENT_TRADE_NOTIONAL_SCALE = 6;
const EVENT_TRADE_NOTIONAL_SCALE_FACTOR = 10n ** BigInt(EVENT_TRADE_NOTIONAL_SCALE);

export function resolveEventTradesLimit(limit: number | null | undefined): number {
  if (limit === null || limit === undefined || !Number.isFinite(limit)) {
    return EVENT_TRADES_DEFAULT_LIMIT;
  }

  const normalized = Math.trunc(limit);
  if (normalized < 1) {
    return 1;
  }
  if (normalized > EVENT_TRADES_MAX_LIMIT) {
    return EVENT_TRADES_MAX_LIMIT;
  }

  return normalized;
}

export function normalizeTradeSideForMetrics(side: string | null | undefined): "buy" | "sell" | null {
  const normalized = side?.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  if (normalized === "buy" || normalized === "yes") {
    return "buy";
  }
  if (normalized === "sell" || normalized === "no") {
    return "sell";
  }
  return null;
}

type EventTradeMetricInput = {
  ts: Date;
  side: string | null;
  notionalUsd: string | null;
};

type EventTradesMetrics = {
  tradesCount: number;
  totalTrades: number;
  windowStartTs: string | null;
  windowEndTs: string | null;
  totalNotionalUsd: string;
  buyTrades: number;
  sellTrades: number;
};

function parseScaledDecimal(value: string | null | undefined): bigint | null {
  if (!value) {
    return null;
  }

  const trimmed = value.trim();
  if (!/^[-+]?\d+(\.\d+)?$/.test(trimmed)) {
    return null;
  }

  const isNegative = trimmed.startsWith("-");
  const unsigned = trimmed.replace(/^[-+]/, "");
  const [wholePartRaw, fractionPartRaw = ""] = unsigned.split(".");
  const wholePart = BigInt(wholePartRaw || "0");
  const fractionPart = BigInt((fractionPartRaw + "0".repeat(EVENT_TRADE_NOTIONAL_SCALE)).slice(0, EVENT_TRADE_NOTIONAL_SCALE));
  const scaled = wholePart * EVENT_TRADE_NOTIONAL_SCALE_FACTOR + fractionPart;

  return isNegative ? -scaled : scaled;
}

function formatScaledDecimal(value: bigint): string {
  const isNegative = value < 0n;
  const absValue = isNegative ? -value : value;
  const wholePart = absValue / EVENT_TRADE_NOTIONAL_SCALE_FACTOR;
  const fractionPart = (absValue % EVENT_TRADE_NOTIONAL_SCALE_FACTOR)
    .toString()
    .padStart(EVENT_TRADE_NOTIONAL_SCALE, "0")
    .replace(/0+$/, "");

  const prefix = isNegative ? "-" : "";
  if (fractionPart.length === 0) {
    return `${prefix}${wholePart.toString()}`;
  }
  return `${prefix}${wholePart.toString()}.${fractionPart}`;
}

export function calculateEventTradesMetrics(trades: EventTradeMetricInput[], totalTrades = trades.length): EventTradesMetrics {
  let totalNotionalScaled = 0n;
  let buyTrades = 0;
  let sellTrades = 0;
  let windowStart: Date | null = null;
  let windowEnd: Date | null = null;

  for (const trade of trades) {
    if (!windowStart || trade.ts < windowStart) {
      windowStart = trade.ts;
    }
    if (!windowEnd || trade.ts > windowEnd) {
      windowEnd = trade.ts;
    }

    const side = normalizeTradeSideForMetrics(trade.side);
    if (side === "buy") {
      buyTrades += 1;
    } else if (side === "sell") {
      sellTrades += 1;
    }

    const notionalScaled = parseScaledDecimal(trade.notionalUsd);
    if (notionalScaled !== null) {
      totalNotionalScaled += notionalScaled;
    }
  }

  return {
    tradesCount: trades.length,
    totalTrades,
    windowStartTs: windowStart ? windowStart.toISOString() : null,
    windowEndTs: windowEnd ? windowEnd.toISOString() : null,
    totalNotionalUsd: formatScaledDecimal(totalNotionalScaled),
    buyTrades,
    sellTrades
  };
}

export async function getProvidersMeta(): Promise<Array<{ code: string; name: string }>> {
  return db.select({ code: platform.code, name: platform.name }).from(platform).orderBy(asc(platform.code));
}

export async function getCoverageMeta(): Promise<
  Array<{
    providerCode: string;
    events: number;
    markets: number;
    instruments: number;
    scopedMarkets: number;
    latestPriceTs: string | null;
    latestOrderbookTs: string | null;
  }>
> {
  const result = await db.execute(sql`
    with providers as (
      select id, code
      from core.platform
    ),
    event_counts as (
      select platform_id, count(*)::bigint as events
      from core.event
      group by platform_id
    ),
    market_counts as (
      select platform_id, count(*)::bigint as markets
      from core.market
      group by platform_id
    ),
    instrument_counts as (
      select platform_id, count(*)::bigint as instruments
      from core.instrument
      group by platform_id
    ),
    scope_counts as (
      select platform_id, count(*)::bigint as scoped_markets
      from core.market_scope
      group by platform_id
    ),
    latest_price as (
      select i.platform_id, max(pp.ts) as latest_price_ts
      from raw.price_point pp
      join core.instrument i on i.id = pp.instrument_id
      group by i.platform_id
    ),
    latest_orderbook as (
      select i.platform_id, max(ob.ts) as latest_orderbook_ts
      from raw.orderbook_top ob
      join core.instrument i on i.id = ob.instrument_id
      group by i.platform_id
    )
    select
      p.code as "providerCode",
      coalesce(ec.events, 0) as events,
      coalesce(mc.markets, 0) as markets,
      coalesce(ic.instruments, 0) as instruments,
      coalesce(sc.scoped_markets, 0) as "scopedMarkets",
      lp.latest_price_ts as "latestPriceTs",
      lo.latest_orderbook_ts as "latestOrderbookTs"
    from providers p
    left join event_counts ec on ec.platform_id = p.id
    left join market_counts mc on mc.platform_id = p.id
    left join instrument_counts ic on ic.platform_id = p.id
    left join scope_counts sc on sc.platform_id = p.id
    left join latest_price lp on lp.platform_id = p.id
    left join latest_orderbook lo on lo.platform_id = p.id
    order by p.code
  `);

  const toCount = (value: unknown): number => {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  return result.rows.map((row) => {
    const typed = row as {
      providerCode: string;
      events: string | number | null;
      markets: string | number | null;
      instruments: string | number | null;
      scopedMarkets: string | number | null;
      latestPriceTs: string | null;
      latestOrderbookTs: string | null;
    };

    return {
      providerCode: typed.providerCode,
      events: toCount(typed.events),
      markets: toCount(typed.markets),
      instruments: toCount(typed.instruments),
      scopedMarkets: toCount(typed.scopedMarkets),
      latestPriceTs: typed.latestPriceTs,
      latestOrderbookTs: typed.latestOrderbookTs
    };
  });
}

async function getCoverageCountsMeta(): Promise<
  Array<{
    providerCode: string;
    events: number;
    markets: number;
    instruments: number;
    scopedMarkets: number;
    latestPriceTs: string | null;
    latestOrderbookTs: string | null;
  }>
> {
  const result = await db.execute(sql`
    with providers as (
      select id, code
      from core.platform
    ),
    event_counts as (
      select platform_id, count(*)::bigint as events
      from core.event
      group by platform_id
    ),
    market_counts as (
      select platform_id, count(*)::bigint as markets
      from core.market
      group by platform_id
    ),
    instrument_counts as (
      select platform_id, count(*)::bigint as instruments
      from core.instrument
      group by platform_id
    ),
    scope_counts as (
      select platform_id, count(*)::bigint as scoped_markets
      from core.market_scope
      group by platform_id
    )
    select
      p.code as "providerCode",
      coalesce(ec.events, 0) as events,
      coalesce(mc.markets, 0) as markets,
      coalesce(ic.instruments, 0) as instruments,
      coalesce(sc.scoped_markets, 0) as "scopedMarkets"
    from providers p
    left join event_counts ec on ec.platform_id = p.id
    left join market_counts mc on mc.platform_id = p.id
    left join instrument_counts ic on ic.platform_id = p.id
    left join scope_counts sc on sc.platform_id = p.id
    order by p.code
  `);

  const toCount = (value: unknown): number => {
    const parsed = Number(value ?? 0);
    return Number.isFinite(parsed) ? parsed : 0;
  };

  return result.rows.map((row) => {
    const typed = row as {
      providerCode: string;
      events: string | number | null;
      markets: string | number | null;
      instruments: string | number | null;
      scopedMarkets: string | number | null;
    };

    return {
      providerCode: typed.providerCode,
      events: toCount(typed.events),
      markets: toCount(typed.markets),
      instruments: toCount(typed.instruments),
      scopedMarkets: toCount(typed.scopedMarkets),
      latestPriceTs: null,
      latestOrderbookTs: null
    };
  });
}

export async function getIngestHealth(): Promise<
  Array<{
    providerCode: string;
    jobName: string;
    lastStatus: string;
    lastStartedAt: string;
    lastFinishedAt: string | null;
    lastDurationMs: number;
    lastRowsUpserted: number;
    lastRowsSkipped: number;
    lastSuccessAt: string | null;
    errorCount: number;
    errorCount24h: number;
    lastErrorText: string | null;
  }>
> {
  const result = await db.execute(sql`
    with ranked as (
      select
        provider_code,
        job_name,
        status,
        started_at,
        finished_at,
        rows_upserted,
        rows_skipped,
        error_text,
        row_number() over (partition by provider_code, job_name order by started_at desc) as rn
      from ops.job_run_log
    ),
    aggregate_errors as (
      select
        provider_code,
        job_name,
        count(*) filter (where status = 'failed') as error_count,
        count(*) filter (where status = 'failed' and started_at >= now() - interval '24 hours') as error_count_24h
      from ops.job_run_log
      group by provider_code, job_name
    ),
    latest_success as (
      select
        provider_code,
        job_name,
        max(finished_at) as last_success_at
      from ops.job_run_log
      where status in ('success', 'partial_success')
      group by provider_code, job_name
    )
    select
      r.provider_code as "providerCode",
      r.job_name as "jobName",
      r.status as "lastStatus",
      r.started_at as "lastStartedAt",
      r.finished_at as "lastFinishedAt",
      extract(epoch from (coalesce(r.finished_at, now()) - r.started_at)) * 1000 as "lastDurationMs",
      r.rows_upserted as "lastRowsUpserted",
      r.rows_skipped as "lastRowsSkipped",
      s.last_success_at as "lastSuccessAt",
      coalesce(e.error_count, 0) as "errorCount",
      coalesce(e.error_count_24h, 0) as "errorCount24h",
      r.error_text as "lastErrorText"
    from ranked r
    left join aggregate_errors e
      on e.provider_code = r.provider_code and e.job_name = r.job_name
    left join latest_success s
      on s.provider_code = r.provider_code and s.job_name = r.job_name
    where r.rn = 1
    order by r.provider_code, r.job_name
  `);

  return result.rows as Array<{
    providerCode: string;
    jobName: string;
    lastStatus: string;
    lastStartedAt: string;
    lastFinishedAt: string | null;
    lastDurationMs: number;
    lastRowsUpserted: number;
    lastRowsSkipped: number;
    lastSuccessAt: string | null;
    errorCount: number;
    errorCount24h: number;
    lastErrorText: string | null;
  }>;
}

export async function getDataFreshness(): Promise<
  Array<{
    providerCode: string;
    latestRawPriceTs: string | null;
    latestRawOrderbookTs: string | null;
    latestTradeTs: string | null;
    latestOiTs: string | null;
    latestAggPrice1hTs: string | null;
    latestAggLiquidity1hTs: string | null;
    rawPriceLagMin: number | null;
    rawOrderbookLagMin: number | null;
    tradeLagMin: number | null;
    oiLagMin: number | null;
    aggPrice1hLagMin: number | null;
    aggLiquidity1hLagMin: number | null;
  }>
> {
  const result = await db.execute(sql`
    with providers as (
      select id, code
      from core.platform
    )
    select
      p.code as "providerCode",
      raw_price.latest_raw_price_ts as "latestRawPriceTs",
      raw_book.latest_raw_orderbook_ts as "latestRawOrderbookTs",
      raw_trade.latest_trade_ts as "latestTradeTs",
      raw_oi.latest_oi_ts as "latestOiTs",
      agg_price.latest_agg_price_1h_ts as "latestAggPrice1hTs",
      agg_liq.latest_agg_liquidity_1h_ts as "latestAggLiquidity1hTs",
      case
        when raw_price.latest_raw_price_ts is null then null
        else round(extract(epoch from (now() - raw_price.latest_raw_price_ts)) / 60.0, 2)
      end as "rawPriceLagMin",
      case
        when raw_book.latest_raw_orderbook_ts is null then null
        else round(extract(epoch from (now() - raw_book.latest_raw_orderbook_ts)) / 60.0, 2)
      end as "rawOrderbookLagMin",
      case
        when raw_trade.latest_trade_ts is null then null
        else round(extract(epoch from (now() - raw_trade.latest_trade_ts)) / 60.0, 2)
      end as "tradeLagMin",
      case
        when raw_oi.latest_oi_ts is null then null
        else round(extract(epoch from (now() - raw_oi.latest_oi_ts)) / 60.0, 2)
      end as "oiLagMin",
      case
        when agg_price.latest_agg_price_1h_ts is null then null
        else round(extract(epoch from (now() - agg_price.latest_agg_price_1h_ts)) / 60.0, 2)
      end as "aggPrice1hLagMin",
      case
        when agg_liq.latest_agg_liquidity_1h_ts is null then null
        else round(extract(epoch from (now() - agg_liq.latest_agg_liquidity_1h_ts)) / 60.0, 2)
      end as "aggLiquidity1hLagMin"
    from providers p
    left join lateral (
      select max(pp.ts) as latest_raw_price_ts
      from raw.price_point pp
      join core.instrument i on i.id = pp.instrument_id
      where i.platform_id = p.id
    ) raw_price on true
    left join lateral (
      select max(ob.ts) as latest_raw_orderbook_ts
      from raw.orderbook_top ob
      join core.instrument i on i.id = ob.instrument_id
      where i.platform_id = p.id
    ) raw_book on true
    left join lateral (
      select max(te.ts) as latest_trade_ts
      from raw.trade_event te
      where te.provider_code = p.code
    ) raw_trade on true
    left join lateral (
      select max(oi.ts) as latest_oi_ts
      from raw.oi_point_5m oi
      where oi.provider_code = p.code
    ) raw_oi on true
    left join lateral (
      select max(ap.bucket_ts) as latest_agg_price_1h_ts
      from agg.market_price_1h ap
      join core.instrument i on i.id = ap.instrument_id
      where i.platform_id = p.id
    ) agg_price on true
    left join lateral (
      select max(al.bucket_ts) as latest_agg_liquidity_1h_ts
      from agg.market_liquidity_1h al
      join core.instrument i on i.id = al.instrument_id
      where i.platform_id = p.id
    ) agg_liq on true
    order by p.code
  `);

  return result.rows.map((row) => {
    const typed = row as {
      providerCode: string;
      latestRawPriceTs: string | null;
      latestRawOrderbookTs: string | null;
      latestTradeTs: string | null;
      latestOiTs: string | null;
      latestAggPrice1hTs: string | null;
      latestAggLiquidity1hTs: string | null;
      rawPriceLagMin: string | number | null;
      rawOrderbookLagMin: string | number | null;
      tradeLagMin: string | number | null;
      oiLagMin: string | number | null;
      aggPrice1hLagMin: string | number | null;
      aggLiquidity1hLagMin: string | number | null;
    };

    const toNullableNumber = (value: string | number | null): number | null => {
      if (value === null || value === undefined) {
        return null;
      }
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    return {
      providerCode: typed.providerCode,
      latestRawPriceTs: typed.latestRawPriceTs,
      latestRawOrderbookTs: typed.latestRawOrderbookTs,
      latestTradeTs: typed.latestTradeTs,
      latestOiTs: typed.latestOiTs,
      latestAggPrice1hTs: typed.latestAggPrice1hTs,
      latestAggLiquidity1hTs: typed.latestAggLiquidity1hTs,
      rawPriceLagMin: toNullableNumber(typed.rawPriceLagMin),
      rawOrderbookLagMin: toNullableNumber(typed.rawOrderbookLagMin),
      tradeLagMin: toNullableNumber(typed.tradeLagMin),
      oiLagMin: toNullableNumber(typed.oiLagMin),
      aggPrice1hLagMin: toNullableNumber(typed.aggPrice1hLagMin),
      aggLiquidity1hLagMin: toNullableNumber(typed.aggLiquidity1hLagMin)
    };
  });
}

export async function listMarkets(params: {
  providerCode?: ProviderCode;
  status?: "active" | "all";
  limit: number;
  offset: number;
}): Promise<
  Array<{
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    displayTitle: string | null;
    status: string;
    closeTime: Date | null;
    volume24h: string | null;
    liquidity: string | null;
  }>
> {
  const whereClauses = [];

  if (params.providerCode) {
    whereClauses.push(eq(platform.code, params.providerCode));
  }
  if ((params.status ?? "active") === "active") {
    whereClauses.push(and(eq(market.status, "active"), or(isNull(market.closeTime), sql`${market.closeTime} > now()`)));
  }

  const query = db
    .select({
      marketUid: market.marketUid,
      providerCode: platform.code,
      marketRef: market.marketRef,
      title: market.title,
      displayTitle: market.displayTitle,
      status: market.status,
      closeTime: market.closeTime,
      volume24h: market.volume24h,
      liquidity: market.liquidity
    })
    .from(market)
    .innerJoin(platform, eq(platform.id, market.platformId))
    .orderBy(desc(sql`coalesce(${market.volume24h}, 0)`), desc(sql`coalesce(${market.liquidity}, 0)`), asc(market.marketUid))
    .limit(params.limit)
    .offset(params.offset);

  if (whereClauses.length > 0) {
    return query.where(and(...whereClauses));
  }

  return query;
}

export async function getMarketDetail(marketUid: string): Promise<{
  market: {
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    displayTitle: string | null;
    status: string;
    closeTime: Date | null;
    volume24h: string | null;
    liquidity: string | null;
    eventRef: string | null;
    eventTitle: string | null;
  };
  instruments: Array<{
    instrumentRef: string;
    outcomeLabel: string | null;
    outcomeIndex: number | null;
    latestPriceTs: Date | null;
    latestPrice: string | null;
    latestOrderbookTs: Date | null;
    bestBid: string | null;
    bestAsk: string | null;
    spread: string | null;
    bidDepthTop5: string | null;
    askDepthTop5: string | null;
  }>;
} | null> {
  const parsed = parseMarketUid(marketUid);
  if (!parsed) {
    return null;
  }

  const marketRows = await db
    .select({
      id: market.id,
      marketUid: market.marketUid,
      providerCode: platform.code,
      marketRef: market.marketRef,
      title: market.title,
      displayTitle: market.displayTitle,
      status: market.status,
      closeTime: market.closeTime,
      volume24h: market.volume24h,
      liquidity: market.liquidity,
      eventRef: event.eventRef,
      eventTitle: event.title
    })
    .from(market)
    .innerJoin(platform, eq(platform.id, market.platformId))
    .leftJoin(event, eq(event.id, market.eventId))
    .where(and(eq(platform.code, parsed.providerCode), eq(market.marketRef, parsed.marketRef)))
    .limit(1);

  const marketRow = marketRows[0];
  if (!marketRow) {
    return null;
  }

  const instruments = await db
    .select({
      id: instrument.id,
      instrumentRef: instrument.instrumentRef,
      outcomeLabel: instrument.outcomeLabel,
      outcomeIndex: instrument.outcomeIndex
    })
    .from(instrument)
    .where(eq(instrument.marketId, marketRow.id))
    .orderBy(asc(instrument.outcomeIndex), asc(instrument.instrumentRef));

  const instrumentDetails = await Promise.all(
    instruments.map(async (item) => {
      const [latestPrice] = await db
        .select({ ts: pricePoint.ts, price: pricePoint.price })
        .from(pricePoint)
        .where(eq(pricePoint.instrumentId, item.id))
        .orderBy(desc(pricePoint.ts))
        .limit(1);

      const [latestOrderbook] = await db
        .select({
          ts: orderbookTop.ts,
          bestBid: orderbookTop.bestBid,
          bestAsk: orderbookTop.bestAsk,
          spread: orderbookTop.spread,
          bidDepthTop5: orderbookTop.bidDepthTop5,
          askDepthTop5: orderbookTop.askDepthTop5
        })
        .from(orderbookTop)
        .where(eq(orderbookTop.instrumentId, item.id))
        .orderBy(desc(orderbookTop.ts))
        .limit(1);

      return {
        instrumentRef: item.instrumentRef,
        outcomeLabel: item.outcomeLabel,
        outcomeIndex: item.outcomeIndex,
        latestPriceTs: latestPrice?.ts ?? null,
        latestPrice: latestPrice?.price ?? null,
        latestOrderbookTs: latestOrderbook?.ts ?? null,
        bestBid: latestOrderbook?.bestBid ?? null,
        bestAsk: latestOrderbook?.bestAsk ?? null,
        spread: latestOrderbook?.spread ?? null,
        bidDepthTop5: latestOrderbook?.bidDepthTop5 ?? null,
        askDepthTop5: latestOrderbook?.askDepthTop5 ?? null
      };
    })
  );

  return {
    market: {
      marketUid: marketRow.marketUid,
      providerCode: marketRow.providerCode,
      marketRef: marketRow.marketRef,
      title: marketRow.title,
      displayTitle: marketRow.displayTitle,
      status: marketRow.status,
      closeTime: marketRow.closeTime,
      volume24h: marketRow.volume24h,
      liquidity: marketRow.liquidity,
      eventRef: marketRow.eventRef,
      eventTitle: marketRow.eventTitle
    },
    instruments: instrumentDetails
  };
}

type EventInstrumentDetail = {
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  latestPriceTs: Date | null;
  latestPrice: string | null;
  latestOrderbookTs: Date | null;
  bestBid: string | null;
  bestAsk: string | null;
  spread: string | null;
  bidDepthTop5: string | null;
  askDepthTop5: string | null;
};

function isYesInstrument(item: { instrumentRef: string; outcomeLabel: string | null }): boolean {
  return item.instrumentRef.toUpperCase().endsWith(":YES") || item.outcomeLabel?.trim().toLowerCase() === "yes";
}

function pickYesPrice(instruments: EventInstrumentDetail[]): number | null {
  const yesInstrument = instruments.find((item) => isYesInstrument(item));

  if (!yesInstrument || yesInstrument.latestPrice === null) {
    return null;
  }

  const parsed = Number(yesInstrument.latestPrice);
  return Number.isFinite(parsed) ? parsed : null;
}

type EventMarketDetail = {
  marketUid: string;
  providerCode: string;
  marketRef: string;
  title: string | null;
  displayTitle: string | null;
  status: string;
  closeTime: Date | null;
  volume24h: string | null;
  liquidity: string | null;
  eventRef: string;
  eventTitle: string | null;
  instruments: EventInstrumentDetail[];
};

function compareEventMarketsByYesPrice(a: EventMarketDetail, b: EventMarketDetail): number {
  const aYesPrice = pickYesPrice(a.instruments);
  const bYesPrice = pickYesPrice(b.instruments);

  if (aYesPrice === null && bYesPrice === null) {
    return a.marketRef.localeCompare(b.marketRef);
  }
  if (aYesPrice === null) {
    return 1;
  }
  if (bYesPrice === null) {
    return -1;
  }
  if (aYesPrice !== bYesPrice) {
    return bYesPrice - aYesPrice;
  }

  return a.marketRef.localeCompare(b.marketRef);
}

export async function getEventDetail(eventUid: string): Promise<{
  event: {
    eventUid: string;
    providerCode: string;
    eventRef: string;
    title: string | null;
    category: string | null;
    startTime: Date | null;
    endTime: Date | null;
    status: string | null;
  };
  markets: EventMarketDetail[];
} | null> {
  const parsed = parseEventUid(eventUid);
  if (!parsed) {
    return null;
  }

  const eventRows = await db
    .select({
      id: event.id,
      providerCode: platform.code,
      eventRef: event.eventRef,
      title: event.title,
      category: event.category,
      startTime: event.startTime,
      endTime: event.endTime,
      status: event.status
    })
    .from(event)
    .innerJoin(platform, eq(platform.id, event.platformId))
    .where(and(eq(platform.code, parsed.providerCode), eq(event.eventRef, parsed.eventRef)))
    .limit(1);

  const eventRow = eventRows[0];
  if (!eventRow) {
    return null;
  }

  const marketRows = await db
    .select({
      id: market.id,
      marketUid: market.marketUid,
      providerCode: platform.code,
      marketRef: market.marketRef,
      title: market.title,
      displayTitle: market.displayTitle,
      status: market.status,
      closeTime: market.closeTime,
      volume24h: market.volume24h,
      liquidity: market.liquidity
    })
    .from(market)
    .innerJoin(platform, eq(platform.id, market.platformId))
    .where(and(eq(platform.code, parsed.providerCode), eq(market.eventId, eventRow.id)))
    .orderBy(asc(market.marketRef));

  if (marketRows.length === 0) {
    return {
      event: {
        eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
        providerCode: eventRow.providerCode,
        eventRef: eventRow.eventRef,
        title: eventRow.title,
        category: eventRow.category,
        startTime: eventRow.startTime,
        endTime: eventRow.endTime,
        status: eventRow.status
      },
      markets: []
    };
  }

  const marketIdList = marketRows.map((row) => row.id);

  const instrumentRows = await db
    .select({
      id: instrument.id,
      marketId: instrument.marketId,
      instrumentRef: instrument.instrumentRef,
      outcomeLabel: instrument.outcomeLabel,
      outcomeIndex: instrument.outcomeIndex
    })
    .from(instrument)
    .where(inArray(instrument.marketId, marketIdList))
    .orderBy(asc(instrument.outcomeIndex), asc(instrument.instrumentRef));

  const instrumentIdList = instrumentRows.map((row) => row.id);
  const latestPriceByInstrument = new Map<number, { ts: Date | null; price: string | null }>();
  const latestOrderbookByInstrument = new Map<
    number,
    {
      ts: Date | null;
      bestBid: string | null;
      bestAsk: string | null;
      spread: string | null;
      bidDepthTop5: string | null;
      askDepthTop5: string | null;
    }
  >();

  if (instrumentIdList.length > 0) {
    const priceResult = await db.execute(sql`
      select distinct on (pp.instrument_id)
        pp.instrument_id as "instrumentId",
        pp.ts as ts,
        pp.price::text as price
      from raw.price_point pp
      where pp.instrument_id in (${sql.join(instrumentIdList.map((id) => sql`${id}`), sql`,`)})
      order by pp.instrument_id asc, pp.ts desc
    `);

    type LatestPriceRow = {
      instrumentId: number | string;
      ts: Date | string;
      price: string | null;
    };

    for (const row of priceResult.rows as LatestPriceRow[]) {
      const instrumentId = Number(row.instrumentId);
      const parsedTs = row.ts instanceof Date ? row.ts : new Date(row.ts);
      latestPriceByInstrument.set(instrumentId, {
        ts: Number.isNaN(parsedTs.getTime()) ? null : parsedTs,
        price: row.price
      });
    }

    const orderbookResult = await db.execute(sql`
      select distinct on (ob.instrument_id)
        ob.instrument_id as "instrumentId",
        ob.ts as ts,
        ob.best_bid::text as "bestBid",
        ob.best_ask::text as "bestAsk",
        ob.spread::text as spread,
        ob.bid_depth_top5::text as "bidDepthTop5",
        ob.ask_depth_top5::text as "askDepthTop5"
      from raw.orderbook_top ob
      where ob.instrument_id in (${sql.join(instrumentIdList.map((id) => sql`${id}`), sql`,`)})
      order by ob.instrument_id asc, ob.ts desc
    `);

    type LatestOrderbookRow = {
      instrumentId: number | string;
      ts: Date | string;
      bestBid: string | null;
      bestAsk: string | null;
      spread: string | null;
      bidDepthTop5: string | null;
      askDepthTop5: string | null;
    };

    for (const row of orderbookResult.rows as LatestOrderbookRow[]) {
      const instrumentId = Number(row.instrumentId);
      const parsedTs = row.ts instanceof Date ? row.ts : new Date(row.ts);
      latestOrderbookByInstrument.set(instrumentId, {
        ts: Number.isNaN(parsedTs.getTime()) ? null : parsedTs,
        bestBid: row.bestBid,
        bestAsk: row.bestAsk,
        spread: row.spread,
        bidDepthTop5: row.bidDepthTop5,
        askDepthTop5: row.askDepthTop5
      });
    }
  }

  const instrumentsByMarket = new Map<number, EventInstrumentDetail[]>();
  for (const row of instrumentRows) {
    const latestPrice = latestPriceByInstrument.get(row.id);
    const latestOrderbook = latestOrderbookByInstrument.get(row.id);
    const detail: EventInstrumentDetail = {
      instrumentRef: row.instrumentRef,
      outcomeLabel: row.outcomeLabel,
      outcomeIndex: row.outcomeIndex,
      latestPriceTs: latestPrice?.ts ?? null,
      latestPrice: latestPrice?.price ?? null,
      latestOrderbookTs: latestOrderbook?.ts ?? null,
      bestBid: latestOrderbook?.bestBid ?? null,
      bestAsk: latestOrderbook?.bestAsk ?? null,
      spread: latestOrderbook?.spread ?? null,
      bidDepthTop5: latestOrderbook?.bidDepthTop5 ?? null,
      askDepthTop5: latestOrderbook?.askDepthTop5 ?? null
    };

    const existing = instrumentsByMarket.get(row.marketId);
    if (existing) {
      existing.push(detail);
    } else {
      instrumentsByMarket.set(row.marketId, [detail]);
    }
  }

  const markets: EventMarketDetail[] = marketRows.map((row) => ({
    marketUid: row.marketUid,
    providerCode: row.providerCode,
    marketRef: row.marketRef,
    title: row.title,
    displayTitle: row.displayTitle,
    status: row.status,
    closeTime: row.closeTime,
    volume24h: row.volume24h,
    liquidity: row.liquidity,
    eventRef: eventRow.eventRef,
    eventTitle: eventRow.title,
    instruments: instrumentsByMarket.get(row.id) ?? []
  }));

  markets.sort(compareEventMarketsByYesPrice);

  return {
    event: {
      eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
      providerCode: eventRow.providerCode,
      eventRef: eventRow.eventRef,
      title: eventRow.title,
      category: eventRow.category,
      startTime: eventRow.startTime,
      endTime: eventRow.endTime,
      status: eventRow.status
    },
    markets
  };
}

type EventLatestTrade = {
  tradeRef: string;
  ts: Date;
  side: string | null;
  price: string | null;
  qty: string | null;
  notionalUsd: string | null;
  marketUid: string;
  marketRef: string;
  marketTitle: string | null;
  instrumentRef: string | null;
  outcomeLabel: string | null;
};

export async function getEventLatestTrades(params: {
  eventUid: string;
  limit?: number;
}): Promise<{
  event: {
    eventUid: string;
    providerCode: string;
    eventRef: string;
    title: string | null;
    category: string | null;
    status: string | null;
  };
  metrics: EventTradesMetrics;
  trades: EventLatestTrade[];
  limit: number;
} | null> {
  const parsed = parseEventUid(params.eventUid);
  if (!parsed) {
    return null;
  }

  const resolvedLimit = resolveEventTradesLimit(params.limit);

  const eventRows = await db
    .select({
      id: event.id,
      providerCode: platform.code,
      eventRef: event.eventRef,
      title: event.title,
      category: event.category,
      status: event.status
    })
    .from(event)
    .innerJoin(platform, eq(platform.id, event.platformId))
    .where(and(eq(platform.code, parsed.providerCode), eq(event.eventRef, parsed.eventRef)))
    .limit(1);

  const eventRow = eventRows[0];
  if (!eventRow) {
    return null;
  }

  const totalTradesResult = await db.execute(sql`
    select count(*)::bigint as "totalTrades"
    from raw.trade_event te
    join core.market m on m.id = te.market_id
    where m.event_id = ${eventRow.id}
  `);
  const totalTradesRaw = (totalTradesResult.rows[0] as { totalTrades?: string | number | null } | undefined)?.totalTrades;
  const parsedTotalTrades = Number(totalTradesRaw ?? 0);
  const totalTrades = Number.isFinite(parsedTotalTrades) && parsedTotalTrades >= 0 ? parsedTotalTrades : 0;

  const tradesResult = await db.execute(sql`
    with markets as (
      select
        m.id as market_id,
        m.market_uid as market_uid,
        m.market_ref as market_ref,
        coalesce(m.display_title, m.title, m.market_ref) as market_title
      from core.market m
      where m.event_id = ${eventRow.id}
    ),
    latest_candidates as (
      select
        te.trade_ref as trade_ref,
        te.market_id as market_id,
        te.instrument_id as instrument_id,
        te.ts as ts,
        te.side as side,
        te.price::text as price,
        te.qty::text as qty,
        te.notional_usd::text as notional_usd
      from markets mk
      cross join lateral (
        select te.trade_ref, te.market_id, te.instrument_id, te.ts, te.side, te.price, te.qty, te.notional_usd
        from raw.trade_event te
        where te.market_id = mk.market_id
        order by te.ts desc
        limit ${resolvedLimit}
      ) te
    )
    select
      lc.trade_ref as "tradeRef",
      lc.ts as ts,
      lc.side as side,
      lc.price as price,
      lc.qty as qty,
      lc.notional_usd as "notionalUsd",
      mk.market_uid as "marketUid",
      mk.market_ref as "marketRef",
      mk.market_title as "marketTitle",
      i.instrument_ref as "instrumentRef",
      i.outcome_label as "outcomeLabel"
    from latest_candidates lc
    join markets mk on mk.market_id = lc.market_id
    left join core.instrument i on i.id = lc.instrument_id
    order by lc.ts desc
    limit ${resolvedLimit}
  `);

  type EventLatestTradeRow = {
    tradeRef: string;
    ts: Date | string;
    side: string | null;
    price: string | null;
    qty: string | null;
    notionalUsd: string | null;
    marketUid: string;
    marketRef: string;
    marketTitle: string | null;
    instrumentRef: string | null;
    outcomeLabel: string | null;
  };

  const trades: EventLatestTrade[] = [];
  for (const row of tradesResult.rows as EventLatestTradeRow[]) {
    const parsedTs = row.ts instanceof Date ? row.ts : new Date(row.ts);
    if (Number.isNaN(parsedTs.getTime())) {
      continue;
    }

    trades.push({
      tradeRef: row.tradeRef,
      ts: parsedTs,
      side: row.side,
      price: row.price,
      qty: row.qty,
      notionalUsd: row.notionalUsd,
      marketUid: row.marketUid,
      marketRef: row.marketRef,
      marketTitle: row.marketTitle,
      instrumentRef: row.instrumentRef,
      outcomeLabel: row.outcomeLabel
    });
  }

  const metrics = calculateEventTradesMetrics(trades, totalTrades);

  return {
    event: {
      eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
      providerCode: eventRow.providerCode,
      eventRef: eventRow.eventRef,
      title: eventRow.title,
      category: eventRow.category,
      status: eventRow.status
    },
    metrics,
    trades,
    limit: resolvedLimit
  };
}

/* ── Top Trades ── */

type TopTrade = {
  tradeRef: string;
  ts: Date;
  providerCode: string;
  side: string | null;
  price: string | null;
  qty: string | null;
  notionalUsd: string | null;
  traderRef: string | null;
  marketUid: string;
  marketRef: string;
  marketTitle: string | null;
  eventUid: string | null;
  eventTitle: string | null;
  instrumentRef: string | null;
  outcomeLabel: string | null;
};

type TopTradesSummary = {
  totalVolume: string;
  tradeCount: number;
  avgTradeSize: string;
  buyCount: number;
  sellCount: number;
};

type TopTradesWindow = "24h" | "7d" | "30d";

function resolveWindowCutoff(window: TopTradesWindow): Date {
  const hours: Record<TopTradesWindow, number> = { "24h": 24, "7d": 168, "30d": 720 };
  return new Date(Date.now() - hours[window] * 60 * 60 * 1000);
}

const TOP_TRADES_MAX_LIMIT = 200;

export async function getTopTrades(params: {
  window: TopTradesWindow;
  providerCode?: ProviderCode;
  limit: number;
  offset: number;
  summaryOnly?: boolean;
}): Promise<{
  summary: TopTradesSummary;
  trades: TopTrade[];
  pagination: { limit: number; offset: number; total: number };
}> {
  const cutoff = resolveWindowCutoff(params.window);
  const limit = Math.min(Math.max(1, params.limit), TOP_TRADES_MAX_LIMIT);
  const offset = Math.max(0, params.offset);
  const providerCode = params.providerCode ?? null;
  const summaryOnly = params.summaryOnly ?? false;

  const providerFilter = providerCode
    ? sql`and te.provider_code = ${providerCode}`
    : sql``;

  const summaryResult = await db.execute(sql`
    select
      count(*)::bigint as "tradeCount",
      coalesce(sum(te.notional_usd), 0)::text as "totalVolume",
      count(*) filter (where lower(te.side) in ('buy', 'yes'))::bigint as "buyCount",
      count(*) filter (where lower(te.side) in ('sell', 'no'))::bigint as "sellCount"
    from raw.trade_event te
    where te.ts >= ${cutoff}
      ${providerFilter}
  `);

  type SummaryRow = {
    tradeCount: string | number;
    totalVolume: string;
    buyCount: string | number;
    sellCount: string | number;
  };

  const summaryRow = summaryResult.rows[0] as SummaryRow | undefined;
  const tradeCount = Number(summaryRow?.tradeCount ?? 0);
  const totalVolume = summaryRow?.totalVolume ?? "0";
  const buyCount = Number(summaryRow?.buyCount ?? 0);
  const sellCount = Number(summaryRow?.sellCount ?? 0);
  const avgTradeSize = tradeCount > 0
    ? (Number(totalVolume) / tradeCount).toFixed(2)
    : "0";
  const trades: TopTrade[] = [];
  if (!summaryOnly) {
    const tradesResult = await db.execute(sql`
      select
        te.trade_ref as "tradeRef",
        te.ts as ts,
        te.provider_code as "providerCode",
        te.side as side,
        te.price::text as price,
        te.qty::text as qty,
        te.notional_usd::text as "notionalUsd",
        te.trader_ref as "traderRef",
        m.market_uid as "marketUid",
        m.market_ref as "marketRef",
        coalesce(m.display_title, m.title, m.market_ref) as "marketTitle",
        case when e.id is not null then te.provider_code || ':' || e.event_ref else null end as "eventUid",
        coalesce(e.title, e.event_ref) as "eventTitle",
        i.instrument_ref as "instrumentRef",
        i.outcome_label as "outcomeLabel"
      from raw.trade_event te
      join core.market m on m.id = te.market_id
      left join core.event e on e.id = m.event_id
      left join core.instrument i on i.id = te.instrument_id
      where te.ts >= ${cutoff}
        ${providerFilter}
      order by te.notional_usd desc nulls last, te.ts desc
      limit ${limit} offset ${offset}
    `);

    type TopTradeRow = {
      tradeRef: string;
      ts: Date | string;
      providerCode: string;
      side: string | null;
      price: string | null;
      qty: string | null;
      notionalUsd: string | null;
      traderRef: string | null;
      marketUid: string;
      marketRef: string;
      marketTitle: string | null;
      eventUid: string | null;
      eventTitle: string | null;
      instrumentRef: string | null;
      outcomeLabel: string | null;
    };

    for (const row of tradesResult.rows as TopTradeRow[]) {
      const parsedTs = row.ts instanceof Date ? row.ts : new Date(row.ts);
      if (Number.isNaN(parsedTs.getTime())) {
        continue;
      }

      trades.push({
        tradeRef: row.tradeRef,
        ts: parsedTs,
        providerCode: row.providerCode,
        side: row.side,
        price: row.price,
        qty: row.qty,
        notionalUsd: row.notionalUsd,
        traderRef: row.traderRef,
        marketUid: row.marketUid,
        marketRef: row.marketRef,
        marketTitle: row.marketTitle,
        eventUid: row.eventUid,
        eventTitle: row.eventTitle,
        instrumentRef: row.instrumentRef,
        outcomeLabel: row.outcomeLabel
      });
    }
  }

  return {
    summary: {
      totalVolume,
      tradeCount,
      avgTradeSize,
      buyCount,
      sellCount
    },
    trades,
    pagination: { limit, offset, total: tradeCount }
  };
}

export async function getEventPriceHistory(params: {
  eventUid: string;
  from?: Date;
  to?: Date;
  interval: "1h";
}): Promise<{
  event: {
    eventUid: string;
    providerCode: string;
    eventRef: string;
    title: string | null;
    category: string | null;
    startTime: Date | null;
    endTime: Date | null;
    status: string | null;
  };
  interval: "1h";
  from: string;
  to: string;
  series: Array<{
    marketUid: string;
    marketRef: string;
    marketTitle: string | null;
    marketDisplayTitle: string | null;
    instrumentRef: string;
    outcomeLabel: string | null;
    points: Array<{
      ts: string;
      price: string;
      open?: string;
      high?: string;
      low?: string;
      close?: string;
      points?: number;
    }>;
  }>;
} | null> {
  const parsed = parseEventUid(params.eventUid);
  if (!parsed) {
    return null;
  }

  const to = params.to ?? new Date();
  const from = params.from ?? new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

  const eventRows = await db
    .select({
      id: event.id,
      providerCode: platform.code,
      eventRef: event.eventRef,
      title: event.title,
      category: event.category,
      startTime: event.startTime,
      endTime: event.endTime,
      status: event.status
    })
    .from(event)
    .innerJoin(platform, eq(platform.id, event.platformId))
    .where(and(eq(platform.code, parsed.providerCode), eq(event.eventRef, parsed.eventRef)))
    .limit(1);

  const eventRow = eventRows[0];
  if (!eventRow) {
    return null;
  }

  const marketRows = await db
    .select({
      id: market.id,
      marketUid: market.marketUid,
      marketRef: market.marketRef,
      title: market.title,
      displayTitle: market.displayTitle
    })
    .from(market)
    .where(eq(market.eventId, eventRow.id))
    .orderBy(asc(market.marketRef));

  if (marketRows.length === 0) {
    return {
      event: {
        eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
        providerCode: eventRow.providerCode,
        eventRef: eventRow.eventRef,
        title: eventRow.title,
        category: eventRow.category,
        startTime: eventRow.startTime,
        endTime: eventRow.endTime,
        status: eventRow.status
      },
      interval: params.interval,
      from: from.toISOString(),
      to: to.toISOString(),
      series: []
    };
  }

  const marketIdList = marketRows.map((row) => row.id);
  const instrumentRows = await db
    .select({
      id: instrument.id,
      marketId: instrument.marketId,
      instrumentRef: instrument.instrumentRef,
      outcomeLabel: instrument.outcomeLabel,
      outcomeIndex: instrument.outcomeIndex
    })
    .from(instrument)
    .where(inArray(instrument.marketId, marketIdList))
    .orderBy(asc(instrument.outcomeIndex), asc(instrument.instrumentRef));

  const instrumentsByMarket = new Map<
    number,
    Array<{
      id: number;
      instrumentRef: string;
      outcomeLabel: string | null;
    }>
  >();

  for (const item of instrumentRows) {
    const existing = instrumentsByMarket.get(item.marketId);
    const detail = {
      id: item.id,
      instrumentRef: item.instrumentRef,
      outcomeLabel: item.outcomeLabel
    };
    if (existing) {
      existing.push(detail);
    } else {
      instrumentsByMarket.set(item.marketId, [detail]);
    }
  }

  const yesInstrumentByMarket = new Map<
    number,
    {
      id: number;
      instrumentRef: string;
      outcomeLabel: string | null;
    }
  >();

  for (const row of marketRows) {
    const yesInstrument = (instrumentsByMarket.get(row.id) ?? []).find((item) => isYesInstrument(item));
    if (yesInstrument) {
      yesInstrumentByMarket.set(row.id, yesInstrument);
    }
  }

  const yesInstrumentIds = Array.from(yesInstrumentByMarket.values()).map((item) => item.id);
  if (yesInstrumentIds.length === 0) {
    return {
      event: {
        eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
        providerCode: eventRow.providerCode,
        eventRef: eventRow.eventRef,
        title: eventRow.title,
        category: eventRow.category,
        startTime: eventRow.startTime,
        endTime: eventRow.endTime,
        status: eventRow.status
      },
      interval: params.interval,
      from: from.toISOString(),
      to: to.toISOString(),
      series: []
    };
  }

  type HistoryPointRow = {
    instrumentId: number | string;
    ts: Date | string;
    price: string;
    open?: string;
    high?: string;
    low?: string;
    close?: string;
    points?: number;
  };

  const result = await db.execute(sql`
    select
      mp.instrument_id as "instrumentId",
      mp.bucket_ts as ts,
      mp.close::text as price,
      mp.open::text as open,
      mp.high::text as high,
      mp.low::text as low,
      mp.close::text as close,
      mp.points as points
    from agg.market_price_1h mp
    where mp.instrument_id in (${sql.join(yesInstrumentIds.map((id) => sql`${id}`), sql`,`)})
      and mp.bucket_ts >= ${from}
      and mp.bucket_ts <= ${to}
    order by mp.instrument_id asc, mp.bucket_ts asc
  `);

  const historyRows = result.rows as HistoryPointRow[];
  const historyByInstrument = new Map<number, HistoryPointRow[]>();
  for (const row of historyRows) {
    const instrumentId = Number(row.instrumentId);
    const existing = historyByInstrument.get(instrumentId);
    if (existing) {
      existing.push(row);
    } else {
      historyByInstrument.set(instrumentId, [row]);
    }
  }

  const series = marketRows
    .map((row) => {
      const yesInstrument = yesInstrumentByMarket.get(row.id);
      if (!yesInstrument) {
        return null;
      }

      const points = (historyByInstrument.get(yesInstrument.id) ?? []).map((point) => ({
        ts: (point.ts instanceof Date ? point.ts : new Date(point.ts)).toISOString(),
        price: point.price,
        open: point.open,
        high: point.high,
        low: point.low,
        close: point.close,
        points: point.points
      }));

      if (points.length === 0) {
        return null;
      }

      return {
        marketUid: row.marketUid,
        marketRef: row.marketRef,
        marketTitle: row.title,
        marketDisplayTitle: row.displayTitle,
        instrumentRef: yesInstrument.instrumentRef,
        outcomeLabel: yesInstrument.outcomeLabel,
        points
      };
    })
    .filter((item): item is NonNullable<typeof item> => item !== null);

  const getLatestClose = (item: (typeof series)[number]): number | null => {
    if (item.points.length === 0) {
      return null;
    }

    const latestPoint = item.points[item.points.length - 1];
    const parsed = Number(latestPoint.close ?? latestPoint.price);
    return Number.isFinite(parsed) ? parsed : null;
  };

  series.sort((a, b) => {
    const aLatest = getLatestClose(a);
    const bLatest = getLatestClose(b);

    if (aLatest === null && bLatest === null) {
      return a.marketRef.localeCompare(b.marketRef);
    }
    if (aLatest === null) {
      return 1;
    }
    if (bLatest === null) {
      return -1;
    }
    if (aLatest !== bLatest) {
      return bLatest - aLatest;
    }

    return a.marketRef.localeCompare(b.marketRef);
  });

  return {
    event: {
      eventUid: `${eventRow.providerCode}:${eventRow.eventRef}`,
      providerCode: eventRow.providerCode,
      eventRef: eventRow.eventRef,
      title: eventRow.title,
      category: eventRow.category,
      startTime: eventRow.startTime,
      endTime: eventRow.endTime,
      status: eventRow.status
    },
    interval: params.interval,
    from: from.toISOString(),
    to: to.toISOString(),
    series
  };
}

export async function getMarketPriceHistory(params: {
  marketUid: string;
  from?: Date;
  to?: Date;
  interval: "1h";
}): Promise<{
  market: {
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    displayTitle: string | null;
    status: string;
    closeTime: Date | null;
  };
  interval: "1h";
  from: string;
  to: string;
  instruments: Array<{
    instrumentRef: string;
    outcomeLabel: string | null;
    outcomeIndex: number | null;
    points: Array<{
      ts: string;
      price: string;
      open?: string;
      high?: string;
      low?: string;
      close?: string;
      points?: number;
    }>;
  }>;
} | null> {
  const parsed = parseMarketUid(params.marketUid);
  if (!parsed) {
    return null;
  }

  const to = params.to ?? new Date();
  const from = params.from ?? new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

  const marketRows = await db
    .select({
      id: market.id,
      marketUid: market.marketUid,
      providerCode: platform.code,
      marketRef: market.marketRef,
      title: market.title,
      displayTitle: market.displayTitle,
      status: market.status,
      closeTime: market.closeTime
    })
    .from(market)
    .innerJoin(platform, eq(platform.id, market.platformId))
    .where(and(eq(platform.code, parsed.providerCode), eq(market.marketRef, parsed.marketRef)))
    .limit(1);

  const marketRow = marketRows[0];
  if (!marketRow) {
    return null;
  }

  const instruments = await db
    .select({
      id: instrument.id,
      instrumentRef: instrument.instrumentRef,
      outcomeLabel: instrument.outcomeLabel,
      outcomeIndex: instrument.outcomeIndex
    })
    .from(instrument)
    .where(eq(instrument.marketId, marketRow.id))
    .orderBy(asc(instrument.outcomeIndex), asc(instrument.instrumentRef));

  type HistoryPointRow = {
    instrumentId: number | string;
    ts: Date | string;
    price: string;
    open?: string;
    high?: string;
    low?: string;
    close?: string;
    points?: number;
  };

  const result = await db.execute(sql`
    select
      mp.instrument_id as "instrumentId",
      mp.bucket_ts as ts,
      mp.close::text as price,
      mp.open::text as open,
      mp.high::text as high,
      mp.low::text as low,
      mp.close::text as close,
      mp.points as points
    from agg.market_price_1h mp
    where mp.market_id = ${marketRow.id}
      and mp.bucket_ts >= ${from}
      and mp.bucket_ts <= ${to}
    order by mp.instrument_id asc, mp.bucket_ts asc
  `);

  const historyRows = result.rows as HistoryPointRow[];

  const historyByInstrument = new Map<number, HistoryPointRow[]>();
  for (const row of historyRows) {
    const instrumentId = Number(row.instrumentId);
    const existing = historyByInstrument.get(instrumentId);
    if (existing) {
      existing.push(row);
    } else {
      historyByInstrument.set(instrumentId, [row]);
    }
  }

  return {
    market: {
      marketUid: marketRow.marketUid,
      providerCode: marketRow.providerCode,
      marketRef: marketRow.marketRef,
      title: marketRow.title,
      displayTitle: marketRow.displayTitle,
      status: marketRow.status,
      closeTime: marketRow.closeTime
    },
    interval: params.interval,
    from: from.toISOString(),
    to: to.toISOString(),
    instruments: instruments.map((item) => {
      const points = (historyByInstrument.get(item.id) ?? []).map((point) => ({
        ts: (point.ts instanceof Date ? point.ts : new Date(point.ts)).toISOString(),
        price: point.price,
        open: point.open,
        high: point.high,
        low: point.low,
        close: point.close,
        points: point.points
      }));

      return {
        instrumentRef: item.instrumentRef,
        outcomeLabel: item.outcomeLabel,
        outcomeIndex: item.outcomeIndex,
        points
      };
    })
  };
}

type DashboardMainInstrument = {
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  latestPriceTs: Date | null;
  latestPrice: string | null;
  previousPrice24h: string | null;
  delta24h: string | null;
  latestOrderbookTs: Date | null;
  bestBid: string | null;
  bestAsk: string | null;
  spread: string | null;
  bidDepthTop5: string | null;
  askDepthTop5: string | null;
};

type DashboardMainMarket = {
  marketUid: string;
  providerCode: string;
  marketRef: string;
  title: string | null;
  displayTitle: string | null;
  status: string;
  closeTime: Date | null;
  volume24h: string | null;
  liquidity: string | null;
  instruments: DashboardMainInstrument[];
};

type DashboardMainEvent = {
  eventUid: string;
  providerCode: string;
  eventRef: string;
  title: string | null;
  category: string | null;
  startTime: Date | null;
  endTime: Date | null;
  status: string | null;
  marketCount: number;
  activeMarketCount: number;
  volume24h: string;
  liquidity: string;
  latestMarketCloseTime: Date | null;
  maxAbsDelta24h: string | null;
  markets: DashboardMainMarket[];
};

function parseDateValue(value: Date | string | null): Date | null {
  if (value === null) {
    return null;
  }

  const parsed = value instanceof Date ? value : new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
}

export async function getDashboardMain(params?: {
  providerCode?: ProviderCode;
  limit?: number;
  marketLimitPerEvent?: number;
  includeNested?: boolean;
}): Promise<{
  kpis: Array<{
    providerCode: string;
    scopedMarkets: number;
    totalMarkets: number;
    totalInstruments: number;
    latestPriceTs: string | null;
    latestOrderbookTs: string | null;
  }>;
  events: DashboardMainEvent[];
}> {
  const providerCode = params?.providerCode ?? null;
  const limit = params?.limit ?? null;
  const marketLimitPerEvent = params?.marketLimitPerEvent ?? null;
  const includeNested = params?.includeNested ?? true;
  const eventLimitClause = limit === null ? sql`` : sql`limit ${limit}`;

  const kpis = await getCoverageCountsMeta().then((rows) =>
    rows
      .filter((row) => providerCode === null || row.providerCode === providerCode)
      .map((row) => ({
        providerCode: row.providerCode,
        scopedMarkets: row.scopedMarkets,
        totalMarkets: row.markets,
        totalInstruments: row.instruments,
        latestPriceTs: row.latestPriceTs,
        latestOrderbookTs: row.latestOrderbookTs
      }))
  );

  const eventResult = await db.execute(sql`
    with scoped_markets as (
      select
        m.id as market_id,
        m.event_id,
        m.platform_id,
        m.status,
        m.close_time,
        m.volume_24h,
        m.liquidity
      from core.market_scope ms
      join core.market m on m.id = ms.market_id
      where m.event_id is not null
    ),
    event_agg as (
      select
        sm.platform_id,
        sm.event_id,
        count(*)::int as market_count,
        count(*) filter (where sm.status = 'active')::int as active_market_count,
        sum(coalesce(sm.volume_24h, 0)) as volume24h_sum,
        sum(coalesce(sm.liquidity, 0)) as liquidity_sum,
        max(sm.close_time) as latest_market_close_time
      from scoped_markets sm
      group by sm.platform_id, sm.event_id
    )
    select
      ea.event_id as "eventId",
      p.code as "providerCode",
      e.event_ref as "eventRef",
      e.title as title,
      e.category as category,
      e.start_time as "startTime",
      e.end_time as "endTime",
      case
        when nullif(trim(e.status), '') is not null then e.status
        when ea.active_market_count > 0 and (ea.latest_market_close_time is null or ea.latest_market_close_time > now()) then 'active'
        when ea.latest_market_close_time is not null and ea.latest_market_close_time <= now() then 'closed'
        else null
      end as status,
      ea.market_count as "marketCount",
      ea.active_market_count as "activeMarketCount",
      ea.volume24h_sum::text as "volume24h",
      ea.liquidity_sum::text as "liquidity",
      ea.latest_market_close_time as "latestMarketCloseTime"
    from event_agg ea
    join core.event e on e.id = ea.event_id
    join core.platform p on p.id = ea.platform_id
    where (${providerCode}::text is null or p.code = ${providerCode})
    order by
      ea.volume24h_sum desc,
      ea.liquidity_sum desc,
      p.code asc,
      e.event_ref asc
    ${eventLimitClause}
  `);

  type EventRow = {
    eventId: number | string;
    providerCode: string;
    eventRef: string;
    title: string | null;
    category: string | null;
    startTime: Date | string | null;
    endTime: Date | string | null;
    status: string | null;
    marketCount: number | string;
    activeMarketCount: number | string;
    volume24h: string;
    liquidity: string;
    latestMarketCloseTime: Date | string | null;
  };

  const eventRows = eventResult.rows as EventRow[];
  if (eventRows.length === 0) {
    return { kpis, events: [] };
  }

  const eventById = new Map<number, DashboardMainEvent>();
  const maxAbsDeltaByEventId = new Map<number, number>();

  for (const row of eventRows) {
    const eventId = Number(row.eventId);
    eventById.set(eventId, {
      eventUid: `${row.providerCode}:${row.eventRef}`,
      providerCode: row.providerCode,
      eventRef: row.eventRef,
      title: row.title,
      category: row.category,
      startTime: parseDateValue(row.startTime),
      endTime: parseDateValue(row.endTime),
      status: row.status,
      marketCount: Number(row.marketCount),
      activeMarketCount: Number(row.activeMarketCount),
      volume24h: row.volume24h,
      liquidity: row.liquidity,
      latestMarketCloseTime: parseDateValue(row.latestMarketCloseTime),
      maxAbsDelta24h: null,
      markets: []
    });
  }

  const buildEvents = (): DashboardMainEvent[] =>
    eventRows
      .map((row) => eventById.get(Number(row.eventId)))
      .filter((row): row is DashboardMainEvent => row !== undefined);

  const selectedEventIds = eventRows.map((row) => Number(row.eventId));
  const eventIdValuesSql = sql.join(selectedEventIds.map((id) => sql`(${id}::bigint)`), sql`,`);

  if (!includeNested) {
    const deltaResult = await db.execute(sql`
      with selected_events(event_id) as (
        values ${eventIdValuesSql}
      ),
      scoped_instruments as (
        select
          se.event_id,
          i.id as instrument_id
        from selected_events se
        join core.market m on m.event_id = se.event_id
        join core.market_scope ms on ms.market_id = m.id
        join core.instrument i on i.market_id = m.id
      )
      select
        si.event_id as "eventId",
        max(
          case
            when latest.price is not null and prev.price is not null then abs(latest.price - prev.price)
            else null
          end
        )::text as "maxAbsDelta24h"
      from scoped_instruments si
      left join lateral (
        select pp.price
        from raw.price_point pp
        where pp.instrument_id = si.instrument_id
        order by pp.ts desc
        limit 1
      ) latest on true
      left join lateral (
        select pp.price
        from raw.price_point pp
        where pp.instrument_id = si.instrument_id
          and pp.ts <= now() - interval '24 hours'
        order by pp.ts desc
        limit 1
      ) prev on true
      group by si.event_id
    `);

    type DeltaRow = {
      eventId: number | string;
      maxAbsDelta24h: string | null;
    };

    for (const row of deltaResult.rows as DeltaRow[]) {
      if (row.maxAbsDelta24h === null) {
        continue;
      }

      const delta = Number(row.maxAbsDelta24h);
      if (!Number.isFinite(delta)) {
        continue;
      }

      maxAbsDeltaByEventId.set(Number(row.eventId), delta);
    }

    for (const [eventId, maxAbsDelta] of maxAbsDeltaByEventId.entries()) {
      const eventDetail = eventById.get(eventId);
      if (!eventDetail) {
        continue;
      }

      eventDetail.maxAbsDelta24h = maxAbsDelta.toFixed(6);
    }

    return { kpis, events: buildEvents() };
  }

  const marketLimitClause =
    marketLimitPerEvent === null ? sql`` : sql`where rm.event_rank <= ${marketLimitPerEvent}`;

  const marketResult = await db.execute(sql`
    with selected_events(event_id) as (
      values ${eventIdValuesSql}
    ),
    ranked_markets as (
      select
        m.id as "marketId",
        m.event_id as "eventId",
        p.code as "providerCode",
        m.market_uid as "marketUid",
        m.market_ref as "marketRef",
        m.title as title,
        m.display_title as "displayTitle",
        m.status as status,
        m.close_time as "closeTime",
        m.volume_24h::text as "volume24h",
        m.liquidity::text as "liquidity",
        row_number() over (
          partition by m.event_id
          order by
            coalesce(m.volume_24h, 0) desc,
            coalesce(m.liquidity, 0) desc,
            m.market_uid asc
        ) as event_rank
      from selected_events se
      join core.market m on m.event_id = se.event_id
      join core.market_scope ms on ms.market_id = m.id
      join core.platform p on p.id = m.platform_id
    )
    select
      rm."marketId" as "marketId",
      rm."eventId" as "eventId",
      rm."providerCode" as "providerCode",
      rm."marketUid" as "marketUid",
      rm."marketRef" as "marketRef",
      rm.title as title,
      rm."displayTitle" as "displayTitle",
      rm.status as status,
      rm."closeTime" as "closeTime",
      rm."volume24h" as "volume24h",
      rm."liquidity" as "liquidity"
    from ranked_markets rm
    ${marketLimitClause}
    order by
      rm."eventId" asc,
      rm.event_rank asc,
      rm."marketUid" asc
  `);

  type MarketRow = {
    marketId: number | string;
    eventId: number | string;
    providerCode: string;
    marketUid: string;
    marketRef: string;
    title: string | null;
    displayTitle: string | null;
    status: string;
    closeTime: Date | string | null;
    volume24h: string | null;
    liquidity: string | null;
  };

  const marketRows = marketResult.rows as MarketRow[];
  const marketById = new Map<number, DashboardMainMarket>();
  const eventIdByMarketId = new Map<number, number>();

  for (const row of marketRows) {
    const marketId = Number(row.marketId);
    const eventId = Number(row.eventId);
    const eventDetail = eventById.get(eventId);
    if (!eventDetail) {
      continue;
    }

    const marketDetail: DashboardMainMarket = {
      marketUid: row.marketUid,
      providerCode: row.providerCode,
      marketRef: row.marketRef,
      title: row.title,
      displayTitle: row.displayTitle,
      status: row.status,
      closeTime: parseDateValue(row.closeTime),
      volume24h: row.volume24h,
      liquidity: row.liquidity,
      instruments: []
    };

    eventDetail.markets.push(marketDetail);
    marketById.set(marketId, marketDetail);
    eventIdByMarketId.set(marketId, eventId);
  }

  const marketIds = [...marketById.keys()];
  if (marketIds.length === 0) {
    return { kpis, events: buildEvents() };
  }

  const marketIdsSql = sql.join(marketIds.map((id) => sql`${id}`), sql`,`);
  const instrumentResult = await db.execute(sql`
    select
      i.id as "instrumentId",
      i.market_id as "marketId",
      i.instrument_ref as "instrumentRef",
      i.outcome_label as "outcomeLabel",
      i.outcome_index as "outcomeIndex"
    from core.instrument i
    where i.market_id in (${marketIdsSql})
    order by
      i.market_id asc,
      (i.outcome_index is null) asc,
      i.outcome_index asc,
      i.instrument_ref asc
  `);

  type InstrumentRow = {
    instrumentId: number | string;
    marketId: number | string;
    instrumentRef: string;
    outcomeLabel: string | null;
    outcomeIndex: number | null;
  };

  const instrumentRows = instrumentResult.rows as InstrumentRow[];
  const instrumentById = new Map<number, DashboardMainInstrument>();
  const eventIdByInstrumentId = new Map<number, number>();

  for (const row of instrumentRows) {
    const marketId = Number(row.marketId);
    const instrumentId = Number(row.instrumentId);
    const marketDetail = marketById.get(marketId);
    const eventId = eventIdByMarketId.get(marketId);

    if (!marketDetail || eventId === undefined) {
      continue;
    }

    const instrumentDetail: DashboardMainInstrument = {
      instrumentRef: row.instrumentRef,
      outcomeLabel: row.outcomeLabel,
      outcomeIndex: row.outcomeIndex,
      latestPriceTs: null,
      latestPrice: null,
      previousPrice24h: null,
      delta24h: null,
      latestOrderbookTs: null,
      bestBid: null,
      bestAsk: null,
      spread: null,
      bidDepthTop5: null,
      askDepthTop5: null
    };

    marketDetail.instruments.push(instrumentDetail);
    instrumentById.set(instrumentId, instrumentDetail);
    eventIdByInstrumentId.set(instrumentId, eventId);
  }

  const instrumentIds = [...instrumentById.keys()];
  if (instrumentIds.length === 0) {
    return { kpis, events: buildEvents() };
  }

  const instrumentIdValuesSql = sql.join(instrumentIds.map((id) => sql`(${id}::bigint)`), sql`,`);
  const priceResult = await db.execute(sql`
    with scoped_instruments(instrument_id) as (
      values ${instrumentIdValuesSql}
    )
    select
      si.instrument_id as "instrumentId",
      latest.ts as "latestPriceTs",
      latest.price::text as "latestPrice",
      prev.price::text as "previousPrice24h",
      case
        when latest.price is not null and prev.price is not null then (latest.price - prev.price)::text
        else null
      end as "delta24h"
    from scoped_instruments si
    left join lateral (
      select pp.ts, pp.price
      from raw.price_point pp
      where pp.instrument_id = si.instrument_id
      order by pp.ts desc
      limit 1
    ) latest on true
    left join lateral (
      select pp.ts, pp.price
      from raw.price_point pp
      where pp.instrument_id = si.instrument_id
        and pp.ts <= now() - interval '24 hours'
      order by pp.ts desc
      limit 1
    ) prev on true
  `);

  type PriceRow = {
    instrumentId: number | string;
    latestPriceTs: Date | string | null;
    latestPrice: string | null;
    previousPrice24h: string | null;
    delta24h: string | null;
  };

  for (const row of priceResult.rows as PriceRow[]) {
    const instrumentId = Number(row.instrumentId);
    const instrumentDetail = instrumentById.get(instrumentId);
    if (!instrumentDetail) {
      continue;
    }

    instrumentDetail.latestPriceTs = parseDateValue(row.latestPriceTs);
    instrumentDetail.latestPrice = row.latestPrice;
    instrumentDetail.previousPrice24h = row.previousPrice24h;
    instrumentDetail.delta24h = row.delta24h;

    if (row.delta24h === null) {
      continue;
    }

    const delta = Math.abs(Number(row.delta24h));
    if (!Number.isFinite(delta)) {
      continue;
    }

    const eventId = eventIdByInstrumentId.get(instrumentId);
    if (eventId === undefined) {
      continue;
    }

    const currentMax = maxAbsDeltaByEventId.get(eventId);
    if (currentMax === undefined || delta > currentMax) {
      maxAbsDeltaByEventId.set(eventId, delta);
    }
  }

  const orderbookResult = await db.execute(sql`
    with scoped_instruments(instrument_id) as (
      values ${instrumentIdValuesSql}
    )
    select
      si.instrument_id as "instrumentId",
      ob.ts as "latestOrderbookTs",
      ob.best_bid::text as "bestBid",
      ob.best_ask::text as "bestAsk",
      ob.spread::text as spread,
      ob.bid_depth_top5::text as "bidDepthTop5",
      ob.ask_depth_top5::text as "askDepthTop5"
    from scoped_instruments si
    left join lateral (
      select
        o.ts,
        o.best_bid,
        o.best_ask,
        o.spread,
        o.bid_depth_top5,
        o.ask_depth_top5
      from raw.orderbook_top o
      where o.instrument_id = si.instrument_id
      order by o.ts desc
      limit 1
    ) ob on true
  `);

  type OrderbookRow = {
    instrumentId: number | string;
    latestOrderbookTs: Date | string | null;
    bestBid: string | null;
    bestAsk: string | null;
    spread: string | null;
    bidDepthTop5: string | null;
    askDepthTop5: string | null;
  };

  for (const row of orderbookResult.rows as OrderbookRow[]) {
    const instrumentId = Number(row.instrumentId);
    const instrumentDetail = instrumentById.get(instrumentId);
    if (!instrumentDetail) {
      continue;
    }

    instrumentDetail.latestOrderbookTs = parseDateValue(row.latestOrderbookTs);
    instrumentDetail.bestBid = row.bestBid;
    instrumentDetail.bestAsk = row.bestAsk;
    instrumentDetail.spread = row.spread;
    instrumentDetail.bidDepthTop5 = row.bidDepthTop5;
    instrumentDetail.askDepthTop5 = row.askDepthTop5;
  }

  for (const [eventId, maxAbsDelta] of maxAbsDeltaByEventId.entries()) {
    const eventDetail = eventById.get(eventId);
    if (!eventDetail) {
      continue;
    }

    eventDetail.maxAbsDelta24h = maxAbsDelta.toFixed(6);
  }

  return { kpis, events: buildEvents() };
}

export async function getDashboardTreemap(params?: {
  providerCode?: ProviderCode;
  coverage?: "all" | "scope";
}): Promise<
  Array<{
    providerCode: string;
    coverage: "all" | "scope";
    categoryCode: string;
    categoryLabel: string;
    bucketTs: string;
    value: string;
    marketCount: number;
    activeMarketCount: number;
  }>
> {
  const coverage = params?.coverage ?? "all";
  const providerCode = params?.providerCode ?? null;

  const result = await db.execute(sql`
    with latest_bucket as (
      select distinct on (provider_code)
        provider_code,
        bucket_ts
      from agg.market_category_snapshot_1h
      where coverage_mode = ${coverage}
      order by provider_code asc, bucket_ts desc
    )
    select
      s.provider_code as "providerCode",
      s.coverage_mode as coverage,
      s.category_code as "categoryCode",
      s.category_label as "categoryLabel",
      s.bucket_ts as "bucketTs",
      sum(s.volume24h)::text as value,
      count(*)::int as "marketCount",
      count(*) filter (where s.status = 'active')::int as "activeMarketCount"
    from agg.market_category_snapshot_1h s
    join latest_bucket lb
      on lb.provider_code = s.provider_code
     and lb.bucket_ts = s.bucket_ts
    where s.coverage_mode = ${coverage}
      and (${providerCode}::text is null or s.provider_code = ${providerCode})
    group by
      s.provider_code,
      s.coverage_mode,
      s.category_code,
      s.category_label,
      s.bucket_ts
    order by
      sum(s.volume24h) desc,
      s.provider_code asc,
      s.category_code asc
  `);

  return result.rows.map((row) => {
    const typed = row as {
      providerCode: string;
      coverage: "all" | "scope";
      categoryCode: string;
      categoryLabel: string;
      bucketTs: string;
      value: string;
      marketCount: number | string;
      activeMarketCount: number | string;
    };

    return {
      providerCode: typed.providerCode,
      coverage: typed.coverage,
      categoryCode: typed.categoryCode,
      categoryLabel: typed.categoryLabel,
      bucketTs: typed.bucketTs,
      value: typed.value,
      marketCount: Number(typed.marketCount),
      activeMarketCount: Number(typed.activeMarketCount)
    };
  });
}

export async function getCategoryQualityMeta(): ReturnType<typeof getCategoryQualitySummary> {
  return getCategoryQualitySummary();
}

export async function getVerifySummary(): Promise<{
  latestRuns: Array<{
    providerCode: string;
    jobName: string;
    status: string;
    startedAt: Date | string;
    finishedAt: Date | string | null;
    rowsUpserted: number;
    rowsSkipped: number;
    errorText: string | null;
  }>;
  tableCounts: Record<string, number>;
  scopedCount: number;
  scopedByStatus: Record<string, number>;
  fillRates: {
    priceMarketFillRate: number;
    orderbookBboMarketFillRate: number;
  };
  categoryQuality: Array<{
    providerCode: string;
    scopedUnknownRate: number;
    globalUnknownRate: number;
    scopedSourceMix: Record<string, number>;
    globalSourceMix: Record<string, number>;
  }>;
  samples: Array<{ marketUid: string; latestPriceTs: Date | string | null; latestOrderbookTs: Date | string | null }>;
}> {
  const latestRuns = await db.execute(sql`
    select distinct on (provider_code, job_name)
      provider_code as "providerCode",
      job_name as "jobName",
      status,
      started_at as "startedAt",
      finished_at as "finishedAt",
      rows_upserted as "rowsUpserted",
      rows_skipped as "rowsSkipped",
      error_text as "errorText"
    from ops.job_run_log
    order by provider_code, job_name, started_at desc
  `);

  const [platformCount] = await db.select({ value: count() }).from(platform);
  const [eventCount] = await db.select({ value: count() }).from(event);
  const [marketCount] = await db.select({ value: count() }).from(market);
  const [instrumentCount] = await db.select({ value: count() }).from(instrument);
  const [categoryDimCount] = await db.select({ value: count() }).from(categoryDim);
  const [categoryAssignmentCount] = await db.select({ value: count() }).from(marketCategoryAssignment);
  const [priceCount] = await db.select({ value: count() }).from(pricePoint);
  const [orderbookCount] = await db.select({ value: count() }).from(orderbookTop);
  const [tradeCount] = await db.select({ value: count() }).from(tradeEvent);
  const [oiCount] = await db.select({ value: count() }).from(oiPoint5m);
  const [aggPriceCount] = await db.select({ value: count() }).from(marketPrice1h);
  const [aggLiquidityCount] = await db.select({ value: count() }).from(marketLiquidity1h);
  const [aggMarketCategorySnapshotCount] = await db.select({ value: count() }).from(marketCategorySnapshot1h);
  const [providerCategoryDimCount] = await db.select({ value: count() }).from(providerCategoryDim);
  const [providerCategoryMapCount] = await db.select({ value: count() }).from(providerCategoryMap);
  const [scopeCount] = await db.select({ value: count() }).from(marketScope);

  const scopedByStatusRows = await db.execute(sql`
    select m.status, count(*)::int as count
    from core.market_scope ms
    join core.market m on m.id = ms.market_id
    group by m.status
  `);

  const scopeHealth = await db.execute(sql`
    with scoped as (
      select distinct ms.market_id
      from core.market_scope ms
    ),
    markets_with_recent_price as (
      select distinct i.market_id
      from core.instrument i
      join raw.price_point pp on pp.instrument_id = i.id
      where pp.ts >= now() - interval '6 hours'
    ),
    latest_book as (
      select distinct on (ob.instrument_id)
        ob.instrument_id,
        ob.best_bid,
        ob.best_ask,
        ob.ts
      from raw.orderbook_top ob
      order by ob.instrument_id, ob.ts desc
    ),
    markets_with_bbo as (
      select distinct i.market_id
      from core.instrument i
      join latest_book lb on lb.instrument_id = i.id
      where lb.best_bid is not null and lb.best_ask is not null
    )
    select
      (select count(*) from scoped)::int as scoped_markets,
      (select count(*) from scoped s join markets_with_recent_price p on p.market_id = s.market_id)::int as scoped_with_recent_price,
      (select count(*) from scoped s join markets_with_bbo b on b.market_id = s.market_id)::int as scoped_with_bbo
  `);

  const scopeHealthRow = (scopeHealth.rows[0] ?? {
    scoped_markets: 0,
    scoped_with_recent_price: 0,
    scoped_with_bbo: 0
  }) as {
    scoped_markets: number;
    scoped_with_recent_price: number;
    scoped_with_bbo: number;
  };

  const priceMarketFillRate =
    scopeHealthRow.scoped_markets === 0 ? 0 : scopeHealthRow.scoped_with_recent_price / scopeHealthRow.scoped_markets;
  const orderbookBboMarketFillRate =
    scopeHealthRow.scoped_markets === 0 ? 0 : scopeHealthRow.scoped_with_bbo / scopeHealthRow.scoped_markets;

  const samples = await db.execute(sql`
    with top_markets as (
      select m.id, m.market_uid
      from core.market m
      join core.platform p on p.id = m.platform_id
      where p.code = 'polymarket'
      order by coalesce(m.volume_24h, 0) desc, coalesce(m.liquidity, 0) desc
      limit 5
    ), latest_price as (
      select i.market_id, max(pp.ts) as latest_price_ts
      from core.instrument i
      left join raw.price_point pp on pp.instrument_id = i.id
      group by i.market_id
    ), latest_book as (
      select i.market_id, max(ob.ts) as latest_orderbook_ts
      from core.instrument i
      left join raw.orderbook_top ob on ob.instrument_id = i.id
      group by i.market_id
    )
    select tm.market_uid as "marketUid", lp.latest_price_ts as "latestPriceTs", lb.latest_orderbook_ts as "latestOrderbookTs"
    from top_markets tm
    left join latest_price lp on lp.market_id = tm.id
    left join latest_book lb on lb.market_id = tm.id
    order by tm.market_uid
  `);

  const categoryQualityRaw = await getCategoryQualitySummary();
  const categoryQuality = categoryQualityRaw.map((row) => ({
    providerCode: row.providerCode,
    scopedUnknownRate: row.scopedUnknownRate,
    globalUnknownRate: row.globalUnknownRate,
    scopedSourceMix: row.scopedSourceMix,
    globalSourceMix: row.globalSourceMix
  }));

  return {
    latestRuns: latestRuns.rows as Array<{
      providerCode: string;
      jobName: string;
      status: string;
      startedAt: Date | string;
      finishedAt: Date | string | null;
      rowsUpserted: number;
      rowsSkipped: number;
      errorText: string | null;
    }>,
    tableCounts: {
      "core.platform": platformCount?.value ?? 0,
      "core.event": eventCount?.value ?? 0,
      "core.market": marketCount?.value ?? 0,
      "core.instrument": instrumentCount?.value ?? 0,
      "core.category_dim": categoryDimCount?.value ?? 0,
      "core.market_category_assignment": categoryAssignmentCount?.value ?? 0,
      "core.provider_category_dim": providerCategoryDimCount?.value ?? 0,
      "core.provider_category_map": providerCategoryMapCount?.value ?? 0,
      "core.market_scope": scopeCount?.value ?? 0,
      "raw.price_point": priceCount?.value ?? 0,
      "raw.orderbook_top": orderbookCount?.value ?? 0,
      "raw.trade_event": tradeCount?.value ?? 0,
      "raw.oi_point_5m": oiCount?.value ?? 0,
      "agg.market_price_1h": aggPriceCount?.value ?? 0,
      "agg.market_liquidity_1h": aggLiquidityCount?.value ?? 0,
      "agg.market_category_snapshot_1h": aggMarketCategorySnapshotCount?.value ?? 0
    },
    scopedCount: scopeCount?.value ?? 0,
    scopedByStatus: Object.fromEntries(
      scopedByStatusRows.rows.map((row) => [String((row as { status: string }).status), Number((row as { count: number }).count)])
    ),
    fillRates: {
      priceMarketFillRate,
      orderbookBboMarketFillRate
    },
    categoryQuality,
    samples: samples.rows as Array<{
      marketUid: string;
      latestPriceTs: Date | string | null;
      latestOrderbookTs: Date | string | null;
    }>
  };
}

type ComparisonCategoryResult = {
  providerCode: string;
  categoryCode: string;
  categoryLabel: string;
  volume24h: string;
  liquidity: string;
  marketCount: number;
  activeMarketCount: number;
  openInterest: string;
};

type ComparisonTraderResult = {
  providerCode: string;
  categoryCode: string;
  categoryLabel: string;
  tradeCount: number;
  uniqueTraders: number | null;
  avgTradeSize: string;
  p95TradeSize: string;
  whaleTrades: number;
  totalNotional: string;
};

export async function getProviderComparison(): Promise<{
  categories: ComparisonCategoryResult[];
  traders: ComparisonTraderResult[];
}> {
  const categoryResult = await db.execute(sql`
    with market_data as (
      select
        p.code as provider_code,
        cd.code as category_code,
        cd.label as category_label,
        m.id as market_id,
        m.volume_24h,
        m.liquidity,
        m.status
      from core.market m
      join core.event e on e.id = m.event_id
      join core.platform p on p.id = e.platform_id
      join core.market_category_assignment mca on mca.market_id = m.id
      join core.category_dim cd on cd.id = mca.category_id
    ),
    latest_oi as (
      select distinct on (market_id) market_id, value
      from raw.oi_point_5m
      order by market_id, ts desc
    )
    select
      md.provider_code as "providerCode",
      md.category_code as "categoryCode",
      md.category_label as "categoryLabel",
      sum(coalesce(md.volume_24h, 0))::text as "volume24h",
      sum(coalesce(md.liquidity, 0))::text as "liquidity",
      count(*)::int as "marketCount",
      count(*) filter (where md.status = 'active')::int as "activeMarketCount",
      coalesce(sum(lo.value), 0)::text as "openInterest"
    from market_data md
    left join latest_oi lo on lo.market_id = md.market_id
    group by md.provider_code, md.category_code, md.category_label
    order by sum(coalesce(md.volume_24h, 0)) desc
  `);

  const categories: ComparisonCategoryResult[] = categoryResult.rows.map((row) => {
    const r = row as Record<string, unknown>;
    return {
      providerCode: r.providerCode as string,
      categoryCode: r.categoryCode as string,
      categoryLabel: r.categoryLabel as string,
      volume24h: r.volume24h as string,
      liquidity: r.liquidity as string,
      marketCount: Number(r.marketCount),
      activeMarketCount: Number(r.activeMarketCount),
      openInterest: r.openInterest as string,
    };
  });

  const traderResult = await db.execute(sql`
    select
      te.provider_code as "providerCode",
      cd.code as "categoryCode",
      cd.label as "categoryLabel",
      count(*)::int as "tradeCount",
      count(distinct te.trader_ref)::int as "uniqueTraders",
      coalesce(avg(te.notional_usd), 0)::numeric(12,2)::text as "avgTradeSize",
      coalesce((percentile_cont(0.95) within group (order by te.notional_usd)), 0)::numeric(12,2)::text as "p95TradeSize",
      count(*) filter (where te.notional_usd >= 10000)::int as "whaleTrades",
      coalesce(sum(te.notional_usd), 0)::text as "totalNotional"
    from raw.trade_event te
    join core.market m on m.id = te.market_id
    join core.market_category_assignment mca on mca.market_id = m.id
    join core.category_dim cd on cd.id = mca.category_id
    where te.ts >= now() - interval '7 days'
    group by te.provider_code, cd.code, cd.label
    order by count(*) desc
  `);

  const traders: ComparisonTraderResult[] = traderResult.rows.map((row) => {
    const r = row as Record<string, unknown>;
    const providerCode = r.providerCode as string;
    const rawUniqueTraders = Number(r.uniqueTraders);
    return {
      providerCode,
      categoryCode: r.categoryCode as string,
      categoryLabel: r.categoryLabel as string,
      tradeCount: Number(r.tradeCount),
      uniqueTraders: providerCode === "kalshi" ? null : rawUniqueTraders,
      avgTradeSize: r.avgTradeSize as string,
      p95TradeSize: r.p95TradeSize as string,
      whaleTrades: Number(r.whaleTrades),
      totalNotional: r.totalNotional as string,
    };
  });

  return { categories, traders };
}
