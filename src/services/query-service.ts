import { and, asc, count, desc, eq, sql } from "drizzle-orm";

import { db } from "../db/client.js";
import {
  categoryDim,
  event,
  instrument,
  market,
  marketCategoryAssignment,
  marketLiquidity1h,
  marketPrice1h,
  marketScope,
  oiPoint5m,
  orderbookTop,
  platform,
  pricePoint,
  providerCategory1h,
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
  limit: number;
  offset: number;
}): Promise<
  Array<{
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    status: string;
    closeTime: Date | null;
    volume24h: string | null;
    liquidity: string | null;
  }>
> {
  const query = db
    .select({
      marketUid: market.marketUid,
      providerCode: platform.code,
      marketRef: market.marketRef,
      title: market.title,
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

  if (params.providerCode) {
    return query.where(eq(platform.code, params.providerCode));
  }

  return query;
}

export async function getMarketDetail(marketUid: string): Promise<{
  market: {
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
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

export async function getDashboardMain(): Promise<{
  kpis: Array<{
    providerCode: string;
    scopedMarkets: number;
    totalMarkets: number;
    totalInstruments: number;
    latestPriceTs: string | null;
    latestOrderbookTs: string | null;
  }>;
  topMovers24h: Array<{
    marketUid: string;
    providerCode: string;
    marketTitle: string | null;
    instrumentRef: string;
    outcomeLabel: string | null;
    latestPrice: string;
    previousPrice24h: string;
    delta24h: string;
  }>;
  summaryTable: Array<{
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    status: string;
    closeTime: Date | null;
    volume24h: string | null;
    liquidity: string | null;
  }>;
}> {
  const kpis = await getCoverageMeta().then((rows) =>
    rows.map((row) => ({
      providerCode: row.providerCode,
      scopedMarkets: row.scopedMarkets,
      totalMarkets: row.markets,
      totalInstruments: row.instruments,
      latestPriceTs: row.latestPriceTs,
      latestOrderbookTs: row.latestOrderbookTs
    }))
  );

  const summaryTable = await listMarkets({ limit: 50, offset: 0 });

  const moversResult = await db.execute(sql`
    with scoped_instruments as (
      select
        i.id as instrument_id,
        i.instrument_ref,
        i.outcome_label,
        m.market_uid,
        m.title as market_title,
        p.code as provider_code
      from core.market_scope ms
      join core.market m on m.id = ms.market_id
      join core.platform p on p.id = m.platform_id
      join core.instrument i on i.market_id = m.id
    ),
    latest_price as (
      select distinct on (pp.instrument_id)
        pp.instrument_id,
        pp.ts,
        pp.price
      from raw.price_point pp
      join scoped_instruments si on si.instrument_id = pp.instrument_id
      order by pp.instrument_id, pp.ts desc
    ),
    with_prev as (
      select
        lp.instrument_id,
        lp.price as latest_price,
        prev.price as prev_price
      from latest_price lp
      left join lateral (
        select pp2.price
        from raw.price_point pp2
        where pp2.instrument_id = lp.instrument_id
          and pp2.ts <= now() - interval '24 hours'
        order by pp2.ts desc
        limit 1
      ) prev on true
    )
    select
      si.market_uid as "marketUid",
      si.provider_code as "providerCode",
      si.market_title as "marketTitle",
      si.instrument_ref as "instrumentRef",
      si.outcome_label as "outcomeLabel",
      wp.latest_price::text as "latestPrice",
      wp.prev_price::text as "previousPrice24h",
      (wp.latest_price - wp.prev_price)::text as "delta24h"
    from with_prev wp
    join scoped_instruments si on si.instrument_id = wp.instrument_id
    where wp.prev_price is not null
    order by abs(wp.latest_price - wp.prev_price) desc
    limit 20
  `);

  return {
    kpis,
    topMovers24h: moversResult.rows as Array<{
      marketUid: string;
      providerCode: string;
      marketTitle: string | null;
      instrumentRef: string;
      outcomeLabel: string | null;
      latestPrice: string;
      previousPrice24h: string;
      delta24h: string;
    }>,
    summaryTable
  };
}

export async function getDashboardTreemap(params?: {
  providerCode?: ProviderCode;
  metric?: "volume24h" | "oi";
  status?: "all" | "active";
  groupBy?: "sector" | "providerCategory";
}): Promise<
  Array<{
    providerCode: string;
    groupBy: "sector" | "providerCategory";
    sourceKind: string | null;
    categoryCode: string;
    categoryLabel: string;
    bucketTs: string;
    value: string;
    marketCount: number;
    activeMarketCount: number;
    selectedMarketCount: number;
  }>
> {
  const metric = params?.metric ?? "volume24h";
  const status = params?.status ?? "all";
  const providerCode = params?.providerCode ?? null;
  const groupBy = params?.groupBy ?? "sector";
  const groupByDb = groupBy === "providerCategory" ? "provider_category" : "sector";

  const result = await db.execute(sql`
    with latest_bucket as (
      select provider_code, group_by, max(bucket_ts) as bucket_ts
      from agg.provider_category_1h
      where group_by = ${groupByDb}
      group by provider_code, group_by
    )
    select
      pc.provider_code as "providerCode",
      pc.group_by as "groupBy",
      pc.source_kind as "sourceKind",
      pc.category_code as "categoryCode",
      pc.category_label as "categoryLabel",
      pc.bucket_ts as "bucketTs",
      (
        case
          when ${metric} = 'oi' and ${status} = 'active' then pc.oi_active
          when ${metric} = 'oi' then pc.oi_total
          when ${status} = 'active' then pc.volume24h_active
          else pc.volume24h_total
        end
      )::text as value,
      pc.market_count as "marketCount",
      pc.active_market_count as "activeMarketCount",
      (
        case
          when ${status} = 'active' then pc.active_market_count
          else pc.market_count
        end
      )::int as "selectedMarketCount"
    from agg.provider_category_1h pc
    join latest_bucket lb
      on lb.provider_code = pc.provider_code
     and lb.group_by = pc.group_by
     and lb.bucket_ts = pc.bucket_ts
    where pc.group_by = ${groupByDb}
      and (${providerCode}::text is null or pc.provider_code = ${providerCode})
    order by
      (
        case
          when ${metric} = 'oi' and ${status} = 'active' then pc.oi_active
          when ${metric} = 'oi' then pc.oi_total
          when ${status} = 'active' then pc.volume24h_active
          else pc.volume24h_total
        end
      ) desc,
      pc.provider_code asc,
      pc.category_code asc
  `);

  return result.rows.map((row) => {
    const typed = row as {
      providerCode: string;
      groupBy: "sector" | "provider_category";
      sourceKind: string | null;
      categoryCode: string;
      categoryLabel: string;
      bucketTs: string;
      value: string;
      marketCount: number | string;
      activeMarketCount: number | string;
      selectedMarketCount: number | string;
    };

    return {
      providerCode: typed.providerCode,
      groupBy: typed.groupBy === "provider_category" ? "providerCategory" : "sector",
      sourceKind: typed.sourceKind,
      categoryCode: typed.categoryCode,
      categoryLabel: typed.categoryLabel,
      bucketTs: typed.bucketTs,
      value: typed.value,
      marketCount: Number(typed.marketCount),
      activeMarketCount: Number(typed.activeMarketCount),
      selectedMarketCount: Number(typed.selectedMarketCount)
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
  const [aggProviderCategoryCount] = await db.select({ value: count() }).from(providerCategory1h);
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
      "agg.provider_category_1h": aggProviderCategoryCount?.value ?? 0
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
