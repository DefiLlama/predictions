import {
  bigint,
  boolean,
  index,
  integer,
  jsonb,
  numeric,
  pgSchema,
  text,
  timestamp,
  uniqueIndex,
  varchar
} from "drizzle-orm/pg-core";

export const core = pgSchema("core");
export const raw = pgSchema("raw");
export const ops = pgSchema("ops");
export const agg = pgSchema("agg");

const createdAtColumn = () => timestamp("created_at", { withTimezone: true }).defaultNow().notNull();
const updatedAtColumn = () => timestamp("updated_at", { withTimezone: true }).defaultNow().notNull();

export const platform = core.table(
  "platform",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    code: varchar("code", { length: 64 }).notNull(),
    name: varchar("name", { length: 128 }).notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [uniqueIndex("platform_code_uq").on(table.code)]
);

export const event = core.table(
  "event",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "restrict" }),
    eventRef: varchar("event_ref", { length: 256 }).notNull(),
    title: text("title"),
    category: varchar("category", { length: 128 }),
    startTime: timestamp("start_time", { withTimezone: true }),
    endTime: timestamp("end_time", { withTimezone: true }),
    status: varchar("status", { length: 64 }),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("event_platform_ref_uq").on(table.platformId, table.eventRef),
    index("event_platform_idx").on(table.platformId)
  ]
);

export const market = core.table(
  "market",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "restrict" }),
    eventId: bigint("event_id", { mode: "number" }).references(() => event.id, { onDelete: "set null" }),
    marketRef: varchar("market_ref", { length: 256 }).notNull(),
    marketUid: varchar("market_uid", { length: 320 }).notNull(),
    title: text("title"),
    displayTitle: text("display_title"),
    status: varchar("status", { length: 64 }).notNull(),
    closeTime: timestamp("close_time", { withTimezone: true }),
    volume24h: numeric("volume_24h", { precision: 24, scale: 6 }),
    liquidity: numeric("liquidity", { precision: 24, scale: 6 }),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("market_platform_ref_uq").on(table.platformId, table.marketRef),
    uniqueIndex("market_uid_uq").on(table.marketUid),
    index("market_platform_idx").on(table.platformId),
    index("market_platform_event_idx").on(table.platformId, table.eventId),
    index("market_status_idx").on(table.status)
  ]
);

export const instrument = core.table(
  "instrument",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "restrict" }),
    instrumentRef: varchar("instrument_ref", { length: 256 }).notNull(),
    outcomeLabel: varchar("outcome_label", { length: 128 }),
    outcomeIndex: integer("outcome_index"),
    isPrimary: boolean("is_primary").notNull().default(true),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("instrument_market_ref_uq").on(table.marketId, table.instrumentRef),
    index("instrument_market_idx").on(table.marketId),
    index("instrument_platform_idx").on(table.platformId)
  ]
);

export const marketScope = core.table(
  "market_scope",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "cascade" }),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    rank: integer("rank").notNull(),
    reason: varchar("reason", { length: 128 }).notNull(),
    computedAt: timestamp("computed_at", { withTimezone: true }).defaultNow().notNull()
  },
  (table) => [
    uniqueIndex("market_scope_platform_market_uq").on(table.platformId, table.marketId),
    index("market_scope_platform_rank_idx").on(table.platformId, table.rank)
  ]
);

export const categoryDim = core.table(
  "category_dim",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    code: varchar("code", { length: 128 }).notNull(),
    label: varchar("label", { length: 128 }).notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [uniqueIndex("category_dim_code_uq").on(table.code)]
);

export const marketCategoryAssignment = core.table(
  "market_category_assignment",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "cascade" }),
    canonicalCategoryId: bigint("canonical_category_id", { mode: "number" })
      .notNull()
      .references(() => categoryDim.id, { onDelete: "restrict" }),
    categoryId: bigint("category_id", { mode: "number" })
      .notNull()
      .references(() => categoryDim.id, { onDelete: "restrict" }),
    providerCategoryId: bigint("provider_category_id", { mode: "number" }).references(() => providerCategoryDim.id, {
      onDelete: "set null"
    }),
    source: varchar("source", { length: 32 }).notNull(),
    confidence: varchar("confidence", { length: 16 }).notNull().default("low"),
    assignedAt: timestamp("assigned_at", { withTimezone: true }).defaultNow().notNull(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("market_category_assignment_market_uq").on(table.marketId),
    index("market_category_assignment_platform_category_idx").on(table.platformId, table.categoryId),
    index("market_category_assignment_category_idx").on(table.categoryId),
    index("market_category_assignment_canonical_category_idx").on(table.canonicalCategoryId),
    index("market_category_assignment_provider_category_idx").on(table.providerCategoryId)
  ]
);

export const providerCategoryDim = core.table(
  "provider_category_dim",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "cascade" }),
    sourceKind: varchar("source_kind", { length: 64 }).notNull(),
    code: varchar("code", { length: 128 }).notNull(),
    label: varchar("label", { length: 128 }).notNull(),
    isNoise: boolean("is_noise").notNull().default(false),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("provider_category_dim_platform_source_code_uq").on(table.platformId, table.sourceKind, table.code),
    index("provider_category_dim_platform_idx").on(table.platformId),
    index("provider_category_dim_source_kind_idx").on(table.sourceKind)
  ]
);

export const providerCategoryMap = core.table(
  "provider_category_map",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    platformId: bigint("platform_id", { mode: "number" })
      .notNull()
      .references(() => platform.id, { onDelete: "cascade" }),
    sourceKind: varchar("source_kind", { length: 64 }).notNull(),
    sourceCode: varchar("source_code", { length: 128 }).notNull(),
    sourceLabel: varchar("source_label", { length: 128 }).notNull(),
    canonicalCategoryId: bigint("canonical_category_id", { mode: "number" })
      .notNull()
      .references(() => categoryDim.id, { onDelete: "restrict" }),
    priority: integer("priority").notNull().default(100),
    isActive: boolean("is_active").notNull().default(true),
    notes: text("notes"),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("provider_category_map_platform_source_code_uq").on(table.platformId, table.sourceKind, table.sourceCode),
    index("provider_category_map_platform_idx").on(table.platformId),
    index("provider_category_map_canonical_idx").on(table.canonicalCategoryId)
  ]
);

export const pricePoint = raw.table(
  "price_point",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    instrumentId: bigint("instrument_id", { mode: "number" })
      .notNull()
      .references(() => instrument.id, { onDelete: "cascade" }),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    price: numeric("price", { precision: 9, scale: 6 }).notNull(),
    source: varchar("source", { length: 128 }).notNull(),
    createdAt: createdAtColumn()
  },
  (table) => [
    uniqueIndex("price_point_instrument_ts_uq").on(table.instrumentId, table.ts),
    index("price_point_ts_idx").on(table.ts)
  ]
);

// Backward-compatible alias while callers are migrated.
export const pricePoint5m = pricePoint;

export const orderbookTop = raw.table(
  "orderbook_top",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    instrumentId: bigint("instrument_id", { mode: "number" })
      .notNull()
      .references(() => instrument.id, { onDelete: "cascade" }),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    bestBid: numeric("best_bid", { precision: 9, scale: 6 }),
    bestAsk: numeric("best_ask", { precision: 9, scale: 6 }),
    spread: numeric("spread", { precision: 9, scale: 6 }),
    bidDepthTop5: numeric("bid_depth_top5", { precision: 18, scale: 6 }),
    askDepthTop5: numeric("ask_depth_top5", { precision: 18, scale: 6 }),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn()
  },
  (table) => [
    uniqueIndex("orderbook_top_instrument_ts_uq").on(table.instrumentId, table.ts),
    index("orderbook_top_ts_idx").on(table.ts)
  ]
);

export const tradeEvent = raw.table(
  "trade_event",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    providerCode: varchar("provider_code", { length: 64 }).notNull(),
    tradeRef: varchar("trade_ref", { length: 256 }).notNull(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    instrumentId: bigint("instrument_id", { mode: "number" }).references(() => instrument.id, { onDelete: "set null" }),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    side: varchar("side", { length: 16 }),
    price: numeric("price", { precision: 9, scale: 6 }),
    qty: numeric("qty", { precision: 24, scale: 6 }),
    notionalUsd: numeric("notional_usd", { precision: 24, scale: 6 }),
    traderRef: varchar("trader_ref", { length: 256 }),
    source: varchar("source", { length: 128 }).notNull(),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn()
  },
  (table) => [
    uniqueIndex("trade_event_provider_trade_ref_uq").on(table.providerCode, table.tradeRef),
    index("trade_event_provider_ts_idx").on(table.providerCode, table.ts),
    index("trade_event_market_ts_idx").on(table.marketId, table.ts),
    index("trade_event_instrument_ts_idx").on(table.instrumentId, table.ts)
  ]
);

export const oiPoint5m = raw.table(
  "oi_point_5m",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    providerCode: varchar("provider_code", { length: 64 }).notNull(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    ts: timestamp("ts", { withTimezone: true }).notNull(),
    value: numeric("value", { precision: 24, scale: 6 }).notNull(),
    unit: varchar("unit", { length: 32 }).notNull(),
    source: varchar("source", { length: 128 }).notNull(),
    rawJson: jsonb("raw_json").$type<Record<string, unknown>>().notNull(),
    createdAt: createdAtColumn()
  },
  (table) => [
    uniqueIndex("oi_point_5m_provider_market_ts_uq").on(table.providerCode, table.marketId, table.ts),
    index("oi_point_5m_provider_ts_idx").on(table.providerCode, table.ts),
    index("oi_point_5m_market_ts_idx").on(table.marketId, table.ts)
  ]
);

export const marketPrice1h = agg.table(
  "market_price_1h",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    instrumentId: bigint("instrument_id", { mode: "number" })
      .notNull()
      .references(() => instrument.id, { onDelete: "cascade" }),
    bucketTs: timestamp("bucket_ts", { withTimezone: true }).notNull(),
    open: numeric("open", { precision: 9, scale: 6 }).notNull(),
    high: numeric("high", { precision: 9, scale: 6 }).notNull(),
    low: numeric("low", { precision: 9, scale: 6 }).notNull(),
    close: numeric("close", { precision: 9, scale: 6 }).notNull(),
    points: integer("points").notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("market_price_1h_instrument_bucket_uq").on(table.instrumentId, table.bucketTs),
    index("market_price_1h_market_bucket_idx").on(table.marketId, table.bucketTs),
    index("market_price_1h_bucket_idx").on(table.bucketTs)
  ]
);

export const marketLiquidity1h = agg.table(
  "market_liquidity_1h",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    instrumentId: bigint("instrument_id", { mode: "number" })
      .notNull()
      .references(() => instrument.id, { onDelete: "cascade" }),
    bucketTs: timestamp("bucket_ts", { withTimezone: true }).notNull(),
    avgSpread: numeric("avg_spread", { precision: 9, scale: 6 }),
    avgBidDepthTop5: numeric("avg_bid_depth_top5", { precision: 18, scale: 6 }),
    avgAskDepthTop5: numeric("avg_ask_depth_top5", { precision: 18, scale: 6 }),
    bboPresenceRate: numeric("bbo_presence_rate", { precision: 9, scale: 6 }).notNull(),
    sampleCount: integer("sample_count").notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("market_liquidity_1h_instrument_bucket_uq").on(table.instrumentId, table.bucketTs),
    index("market_liquidity_1h_market_bucket_idx").on(table.marketId, table.bucketTs),
    index("market_liquidity_1h_bucket_idx").on(table.bucketTs)
  ]
);

export const marketCategorySnapshot1h = agg.table(
  "market_category_snapshot_1h",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    providerCode: varchar("provider_code", { length: 64 }).notNull(),
    coverageMode: varchar("coverage_mode", { length: 16 }).notNull().default("all"),
    bucketTs: timestamp("bucket_ts", { withTimezone: true }).notNull(),
    marketId: bigint("market_id", { mode: "number" })
      .notNull()
      .references(() => market.id, { onDelete: "cascade" }),
    categoryCode: varchar("category_code", { length: 128 }).notNull(),
    categoryLabel: varchar("category_label", { length: 128 }).notNull(),
    volume24h: numeric("volume24h", { precision: 24, scale: 6 }).notNull(),
    liquidity: numeric("liquidity", { precision: 24, scale: 6 }).notNull(),
    status: varchar("status", { length: 64 }).notNull(),
    createdAt: createdAtColumn(),
    updatedAt: updatedAtColumn()
  },
  (table) => [
    uniqueIndex("market_category_snapshot_1h_provider_coverage_bucket_market_uq").on(
      table.providerCode,
      table.coverageMode,
      table.bucketTs,
      table.marketId
    ),
    index("market_category_snapshot_1h_provider_coverage_bucket_idx").on(table.providerCode, table.coverageMode, table.bucketTs),
    index("market_category_snapshot_1h_bucket_idx").on(table.bucketTs),
    index("market_category_snapshot_1h_provider_category_bucket_idx").on(table.providerCode, table.categoryCode, table.bucketTs)
  ]
);

export const ingestCheckpoint = ops.table(
  "ingest_checkpoint",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    providerCode: varchar("provider_code", { length: 64 }).notNull(),
    jobName: varchar("job_name", { length: 128 }).notNull(),
    cursorJson: jsonb("cursor_json").$type<Record<string, unknown>>().notNull().default({}),
    updatedAt: updatedAtColumn()
  },
  (table) => [uniqueIndex("ingest_checkpoint_provider_job_uq").on(table.providerCode, table.jobName)]
);

export const jobRunLog = ops.table(
  "job_run_log",
  {
    id: bigint("id", { mode: "number" }).generatedAlwaysAsIdentity().primaryKey(),
    requestId: varchar("request_id", { length: 128 }),
    providerCode: varchar("provider_code", { length: 64 }).notNull(),
    jobName: varchar("job_name", { length: 128 }).notNull(),
    startedAt: timestamp("started_at", { withTimezone: true }).defaultNow().notNull(),
    finishedAt: timestamp("finished_at", { withTimezone: true }),
    status: varchar("status", { length: 32 }).notNull(),
    rowsUpserted: integer("rows_upserted").notNull().default(0),
    rowsSkipped: integer("rows_skipped").notNull().default(0),
    errorText: text("error_text")
  },
  (table) => [
    index("job_run_log_provider_job_started_idx").on(table.providerCode, table.jobName, table.startedAt),
    index("job_run_log_request_idx").on(table.requestId)
  ]
);

export type PlatformRow = typeof platform.$inferSelect;
export type MarketRow = typeof market.$inferSelect;
export type InstrumentRow = typeof instrument.$inferSelect;
