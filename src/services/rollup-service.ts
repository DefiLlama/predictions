import { sql } from "drizzle-orm";

import { db } from "../db/client.js";
import type { ProviderCode } from "../types/domain.js";
import type { JobRunResult } from "./job-log-service.js";

interface RollupOptions {
  lookbackHours?: number;
}

function resolveLookbackHours(options?: RollupOptions): number {
  return Math.max(1, Math.floor(options?.lookbackHours ?? 8 * 24));
}

export async function refreshMarketPrice1hRollup(providerCode: ProviderCode, options?: RollupOptions): Promise<JobRunResult> {
  const lookbackHours = resolveLookbackHours(options);

  const result = await db.execute(sql`
    with source as (
      select
        i.market_id,
        pp.instrument_id,
        date_trunc('hour', pp.ts) as bucket_ts,
        pp.ts,
        pp.price
      from raw.price_point_5m pp
      join core.instrument i on i.id = pp.instrument_id
      join core.platform p on p.id = i.platform_id
      where p.code = ${providerCode}
        and pp.ts >= now() - (${lookbackHours} * interval '1 hour')
    ),
    ranked as (
      select
        s.*,
        row_number() over (partition by s.instrument_id, s.bucket_ts order by s.ts asc) as rn_asc,
        row_number() over (partition by s.instrument_id, s.bucket_ts order by s.ts desc) as rn_desc
      from source s
    ),
    aggregated as (
      select
        r.market_id,
        r.instrument_id,
        r.bucket_ts,
        max(case when r.rn_asc = 1 then r.price end) as open,
        max(r.price) as high,
        min(r.price) as low,
        max(case when r.rn_desc = 1 then r.price end) as close,
        count(*)::int as points
      from ranked r
      group by r.market_id, r.instrument_id, r.bucket_ts
    ),
    upserted as (
      insert into agg.market_price_1h (market_id, instrument_id, bucket_ts, open, high, low, close, points, created_at, updated_at)
      select
        a.market_id,
        a.instrument_id,
        a.bucket_ts,
        a.open,
        a.high,
        a.low,
        a.close,
        a.points,
        now(),
        now()
      from aggregated a
      on conflict (instrument_id, bucket_ts) do update
      set
        market_id = excluded.market_id,
        open = excluded.open,
        high = excluded.high,
        low = excluded.low,
        close = excluded.close,
        points = excluded.points,
        updated_at = now()
      returning 1
    )
    select count(*)::int as value from upserted
  `);

  const rowsUpserted = Number((result.rows[0] as { value?: number } | undefined)?.value ?? 0);
  return { rowsUpserted, rowsSkipped: 0 };
}

export async function refreshMarketLiquidity1hRollup(
  providerCode: ProviderCode,
  options?: RollupOptions
): Promise<JobRunResult> {
  const lookbackHours = resolveLookbackHours(options);

  const result = await db.execute(sql`
    with source as (
      select
        i.market_id,
        ob.instrument_id,
        date_trunc('hour', ob.ts) as bucket_ts,
        ob.spread,
        ob.bid_depth_top5,
        ob.ask_depth_top5,
        case when ob.best_bid is not null and ob.best_ask is not null then 1.0 else 0.0 end as bbo_present
      from raw.orderbook_top ob
      join core.instrument i on i.id = ob.instrument_id
      join core.platform p on p.id = i.platform_id
      where p.code = ${providerCode}
        and ob.ts >= now() - (${lookbackHours} * interval '1 hour')
    ),
    aggregated as (
      select
        s.market_id,
        s.instrument_id,
        s.bucket_ts,
        avg(s.spread)::numeric(9, 6) as avg_spread,
        avg(s.bid_depth_top5)::numeric(18, 6) as avg_bid_depth_top5,
        avg(s.ask_depth_top5)::numeric(18, 6) as avg_ask_depth_top5,
        avg(s.bbo_present)::numeric(9, 6) as bbo_presence_rate,
        count(*)::int as sample_count
      from source s
      group by s.market_id, s.instrument_id, s.bucket_ts
    ),
    upserted as (
      insert into agg.market_liquidity_1h (
        market_id,
        instrument_id,
        bucket_ts,
        avg_spread,
        avg_bid_depth_top5,
        avg_ask_depth_top5,
        bbo_presence_rate,
        sample_count,
        created_at,
        updated_at
      )
      select
        a.market_id,
        a.instrument_id,
        a.bucket_ts,
        a.avg_spread,
        a.avg_bid_depth_top5,
        a.avg_ask_depth_top5,
        a.bbo_presence_rate,
        a.sample_count,
        now(),
        now()
      from aggregated a
      on conflict (instrument_id, bucket_ts) do update
      set
        market_id = excluded.market_id,
        avg_spread = excluded.avg_spread,
        avg_bid_depth_top5 = excluded.avg_bid_depth_top5,
        avg_ask_depth_top5 = excluded.avg_ask_depth_top5,
        bbo_presence_rate = excluded.bbo_presence_rate,
        sample_count = excluded.sample_count,
        updated_at = now()
      returning 1
    )
    select count(*)::int as value from upserted
  `);

  const rowsUpserted = Number((result.rows[0] as { value?: number } | undefined)?.value ?? 0);
  return { rowsUpserted, rowsSkipped: 0 };
}

