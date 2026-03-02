import { and, eq, inArray, sql } from "drizzle-orm";

import { getAdapter } from "../adapters/index.js";
import { KalshiAdapter } from "../adapters/kalshi-adapter.js";
import { PolymarketAdapter } from "../adapters/polymarket-adapter.js";
import { env } from "../config/env.js";
import { db } from "../db/client.js";
import { event, instrument, market, marketScope, oiPoint5m, orderbookTop, platform, pricePoint5m, tradeEvent } from "../db/schema.js";
import type { AdapterInstrumentInput, AdapterMarketInput, NormalizedEvent, NormalizedInstrument, NormalizedMarket, ProviderCode } from "../types/domain.js";
import { chunkArray } from "../utils/chunk.js";
import { logger } from "../utils/logger.js";
import { getHttpMetricsSnapshot } from "../utils/http-metrics.js";
import { getCheckpoint, setCheckpoint } from "./checkpoint-service.js";
import type { JobRunResult } from "./job-log-service.js";

interface SyncOptions {
  requestId?: string;
  scopeStatus?: "active" | "closed" | "all";
  mode?: "topN_live" | "full_catalog";
}

interface RelinkOptions {
  requestId?: string;
  maxMarkets?: number;
}

interface BackfillCursor {
  eventsOffset: number;
  marketsOffset: number;
  eventsDone: boolean;
  marketsDone: boolean;
  completed: boolean;
}

interface KalshiMetadataCursor {
  eventsCursor: string | null;
  marketsCursor: string | null;
  multivariateCursor: string | null;
  eventsDone: boolean;
  marketsDone: boolean;
  multivariateDone: boolean;
  completed: boolean;
  cycle: number;
}

interface IncrementalWindow {
  cursorKey: string;
  startTs: number;
  endTs: number;
}

interface SelectionState {
  mode: "topN_live" | "full_catalog";
  cursorKey: string | null;
  nextMarketCursorId: number | null;
  cycleCompleted: boolean;
}

interface SelectionInstrumentRow {
  marketId: number;
  marketRef: string;
  marketStatus: string;
  instrumentId: number;
  instrumentRef: string;
}

interface SelectionMarketRow {
  marketId: number;
  marketRef: string;
  marketStatus: string;
}

function toNullableNumericString(value: number | null): string | null {
  return value === null ? null : value.toFixed(6);
}

function toNullableDepthNumericString(value: number | null): string | null {
  return value === null ? null : value.toFixed(6);
}

function computeMarketUid(providerCode: ProviderCode, marketRef: string): string {
  return `${providerCode}:${marketRef}`;
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

function parseEpochSeconds(value: unknown): number | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) {
    return null;
  }

  return Math.floor(value);
}

function parseIsoEpochSeconds(value: unknown): number | null {
  if (typeof value !== "string") {
    return null;
  }

  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) {
    return null;
  }

  return Math.floor(parsed / 1000);
}

function parseLastWindowEndTs(cursor: Record<string, unknown> | null): number | null {
  const direct =
    parseEpochSeconds(cursor?.lastWindowEndTs) ??
    parseEpochSeconds(cursor?.windowEndTs) ??
    parseEpochSeconds(cursor?.nextWindowStartTs);

  if (direct !== null) {
    return direct;
  }

  return parseIsoEpochSeconds(cursor?.lastRunAt);
}

function resolveIngestMode(mode: SyncOptions["mode"]): "topN_live" | "full_catalog" {
  return mode === "full_catalog" ? "full_catalog" : "topN_live";
}

function resolveScopeStatus(scopeStatus: SyncOptions["scopeStatus"], mode: "topN_live" | "full_catalog"): "active" | "closed" | "all" {
  if (scopeStatus === "active" || scopeStatus === "closed" || scopeStatus === "all") {
    return scopeStatus;
  }

  return mode === "full_catalog" ? "active" : "all";
}

function getIncrementalCursorKey(
  baseCheckpointKey: string,
  scopeStatus: SyncOptions["scopeStatus"],
  mode: "topN_live" | "full_catalog"
): string {
  if (mode === "topN_live") {
    return `${baseCheckpointKey}:${scopeStatus ?? "all"}`;
  }

  return `${baseCheckpointKey}:${mode}:${scopeStatus ?? "all"}`;
}

function getFullCatalogCursorKey(baseCheckpointKey: string, scopeStatus: "active" | "closed" | "all"): string {
  return `${baseCheckpointKey}:full_catalog:market_cursor:${scopeStatus}`;
}

function shouldLogChunkProgress(mode: "topN_live" | "full_catalog", chunkIndex: number, totalChunks: number): boolean {
  if (mode === "full_catalog") {
    return true;
  }

  return chunkIndex === 1 || chunkIndex === totalChunks || chunkIndex % 5 === 0;
}

function logIngestionProgress(params: {
  providerCode: ProviderCode;
  flow: string;
  mode: "topN_live" | "full_catalog";
  requestId?: string;
  phase: string;
  scopeStatus?: "active" | "closed" | "all";
  chunkIndex?: number;
  totalChunks?: number;
  selectedMarkets?: number;
  selectedInstruments?: number;
  inputTargets?: number;
  rowsFetched?: number;
  rowsUpserted?: number;
  rowsSkipped?: number;
  nextMarketCursorId?: number;
  batchesProcessed?: number;
  windowStartTs?: number;
  windowEndTs?: number;
}): void {
  logger.info(
    {
      providerCode: params.providerCode,
      flow: params.flow,
      mode: params.mode,
      requestId: params.requestId ?? null,
      phase: params.phase,
      scopeStatus: params.scopeStatus,
      chunkIndex: params.chunkIndex,
      totalChunks: params.totalChunks,
      selectedMarkets: params.selectedMarkets,
      selectedInstruments: params.selectedInstruments,
      inputTargets: params.inputTargets,
      rowsFetched: params.rowsFetched,
      rowsUpserted: params.rowsUpserted,
      rowsSkipped: params.rowsSkipped,
      nextMarketCursorId: params.nextMarketCursorId,
      batchesProcessed: params.batchesProcessed,
      windowStartTs: params.windowStartTs,
      windowEndTs: params.windowEndTs,
      httpMetrics: getHttpMetricsSnapshot()
    },
    "Ingestion progress"
  );
}

function parseCursorMarketId(cursor: Record<string, unknown> | null): number {
  const direct = cursor?.nextMarketCursorId ?? cursor?.lastMarketId ?? cursor?.cursorMarketId;
  if (typeof direct === "number" && Number.isFinite(direct) && direct >= 0) {
    return Math.floor(direct);
  }

  if (typeof direct === "string") {
    const parsed = Number(direct);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return Math.floor(parsed);
    }
  }

  return 0;
}

async function resolveIncrementalWindow(params: {
  providerCode: ProviderCode;
  baseCheckpointKey: string;
  scopeStatus: SyncOptions["scopeStatus"];
  initialLookbackDays: number;
  mode: "topN_live" | "full_catalog";
}): Promise<IncrementalWindow> {
  const cursorKey = getIncrementalCursorKey(params.baseCheckpointKey, params.scopeStatus, params.mode);
  const cursor = await getCheckpoint(params.providerCode, cursorKey);

  const nowTs = Math.floor(Date.now() / 1000);
  const defaultStartTs = Math.max(0, nowTs - params.initialLookbackDays * 24 * 60 * 60);
  const overlap = env.INGEST_INCREMENTAL_OVERLAP_SECONDS;
  const lastWindowEndTs = parseLastWindowEndTs(cursor);

  const incrementalStartTs =
    lastWindowEndTs === null ? defaultStartTs : Math.max(0, Math.min(lastWindowEndTs - overlap, nowTs - 1));

  return {
    cursorKey,
    startTs: incrementalStartTs,
    endTs: nowTs
  };
}

async function getPlatformId(providerCode: ProviderCode): Promise<number> {
  const rows = await db.select({ id: platform.id }).from(platform).where(eq(platform.code, providerCode)).limit(1);
  const row = rows[0];
  if (!row) {
    throw new Error(`Missing platform row for ${providerCode}`);
  }
  return row.id;
}

async function loadMarketIdMap(providerId: number, marketRefs: string[]): Promise<Map<string, number>> {
  if (marketRefs.length === 0) {
    return new Map();
  }

  const map = new Map<string, number>();

  for (const refsChunk of chunkArray(marketRefs, 1000)) {
    const rows = await db
      .select({ id: market.id, marketRef: market.marketRef })
      .from(market)
      .where(and(eq(market.platformId, providerId), inArray(market.marketRef, refsChunk)));

    for (const row of rows) {
      map.set(row.marketRef, row.id);
    }
  }

  return map;
}

async function loadEventIdMap(providerId: number, eventRefs: string[]): Promise<Map<string, number>> {
  if (eventRefs.length === 0) {
    return new Map();
  }

  const map = new Map<string, number>();

  for (const refsChunk of chunkArray(eventRefs, 1000)) {
    const rows = await db
      .select({ id: event.id, eventRef: event.eventRef })
      .from(event)
      .where(and(eq(event.platformId, providerId), inArray(event.eventRef, refsChunk)));

    for (const row of rows) {
      map.set(row.eventRef, row.id);
    }
  }

  return map;
}

async function upsertProviderMetadata(params: {
  providerCode: ProviderCode;
  events: NormalizedEvent[];
  markets: NormalizedMarket[];
  instruments: NormalizedInstrument[];
  metadataTimestamp?: Date;
}): Promise<{ rowsUpserted: number; rowsSkipped: number }> {
  const platformId = await getPlatformId(params.providerCode);
  const metadataTimestamp = params.metadataTimestamp ?? new Date();

  let rowsUpserted = 0;
  let rowsSkipped = 0;

  for (const rows of chunkArray(params.events, 500)) {
    await db
      .insert(event)
      .values(
        rows.map((item) => ({
          platformId,
          eventRef: item.eventRef,
          title: item.title,
          category: item.category,
          startTime: item.startTime,
          endTime: item.endTime,
          status: item.status,
          rawJson: item.rawJson,
          updatedAt: metadataTimestamp
        }))
      )
      .onConflictDoUpdate({
        target: [event.platformId, event.eventRef],
        set: {
          title: sql`excluded.title`,
          category: sql`excluded.category`,
          startTime: sql`excluded.start_time`,
          endTime: sql`excluded.end_time`,
          status: sql`excluded.status`,
          rawJson: sql`excluded.raw_json`,
          updatedAt: metadataTimestamp
        }
      });

    rowsUpserted += rows.length;
  }

  const eventRefs = params.markets.map((item) => item.eventRef).filter((value): value is string => value !== null);
  const eventIdMap = await loadEventIdMap(platformId, eventRefs);

  for (const rows of chunkArray(params.markets, 500)) {
    await db
      .insert(market)
      .values(
        rows.map((item) => ({
          platformId,
          eventId: item.eventRef ? (eventIdMap.get(item.eventRef) ?? null) : null,
          marketRef: item.marketRef,
          marketUid: computeMarketUid(params.providerCode, item.marketRef),
          title: item.title,
          status: item.status,
          closeTime: item.closeTime,
          volume24h: toNullableNumericString(item.volume24h),
          liquidity: toNullableNumericString(item.liquidity),
          rawJson: item.rawJson,
          updatedAt: metadataTimestamp
        }))
      )
      .onConflictDoUpdate({
        target: [market.platformId, market.marketRef],
        set: {
          eventId: sql`excluded.event_id`,
          marketUid: sql`excluded.market_uid`,
          title: sql`excluded.title`,
          status: sql`excluded.status`,
          closeTime: sql`excluded.close_time`,
          volume24h: sql`excluded.volume_24h`,
          liquidity: sql`excluded.liquidity`,
          rawJson: sql`excluded.raw_json`,
          updatedAt: metadataTimestamp
        }
      });

    rowsUpserted += rows.length;
  }

  const marketRefs = params.instruments.map((item) => item.marketRef);
  const marketIdMap = await loadMarketIdMap(platformId, marketRefs);

  for (const rows of chunkArray(params.instruments, 1000)) {
    const rowsToUpsert = rows
      .map((item) => {
        const marketId = marketIdMap.get(item.marketRef);
        if (!marketId) {
          rowsSkipped += 1;
          return null;
        }

        return {
          marketId,
          platformId,
          instrumentRef: item.instrumentRef,
          outcomeLabel: item.outcomeLabel,
          outcomeIndex: item.outcomeIndex,
          isPrimary: item.isPrimary,
          rawJson: item.rawJson,
          updatedAt: metadataTimestamp
        };
      })
      .filter((item): item is NonNullable<typeof item> => item !== null);

    if (rowsToUpsert.length === 0) {
      continue;
    }

    await db
      .insert(instrument)
      .values(rowsToUpsert)
      .onConflictDoUpdate({
        target: [instrument.marketId, instrument.instrumentRef],
        set: {
          outcomeLabel: sql`excluded.outcome_label`,
          outcomeIndex: sql`excluded.outcome_index`,
          isPrimary: sql`excluded.is_primary`,
          rawJson: sql`excluded.raw_json`,
          updatedAt: metadataTimestamp
        }
      });

    rowsUpserted += rowsToUpsert.length;
  }

  return {
    rowsUpserted,
    rowsSkipped
  };
}

function extractEventRefFromMarketRaw(
  providerCode: ProviderCode,
  rawJson: Record<string, unknown>,
  normalizeMarketRef: (raw: string) => string
): string | null {
  if (providerCode === "kalshi") {
    const eventTicker = asString(rawJson.event_ticker);
    return eventTicker ? normalizeMarketRef(eventTicker) : null;
  }

  const directEventId = asString(rawJson.eventId);
  if (directEventId) {
    return normalizeMarketRef(directEventId);
  }

  const events = parseJsonArray(rawJson.events);
  for (const entry of events) {
    const primitiveRef = asString(entry);
    if (primitiveRef) {
      return normalizeMarketRef(primitiveRef);
    }

    if (entry && typeof entry === "object" && !Array.isArray(entry)) {
      const eventObj = entry as Record<string, unknown>;
      const nestedRef = asString(eventObj.id) ?? asString(eventObj.eventId) ?? asString(eventObj.event_id);
      if (nestedRef) {
        return normalizeMarketRef(nestedRef);
      }
    }
  }

  return null;
}

async function applyMarketEventLinks(links: Array<{ marketId: number; eventId: number }>, updatedAt: Date): Promise<number> {
  let updatedRows = 0;

  for (const rows of chunkArray(links, 1000)) {
    if (rows.length === 0) {
      continue;
    }

    const valuesSql = sql.join(rows.map((row) => sql`(${row.marketId}::bigint, ${row.eventId}::bigint)`), sql`,`);
    const result = await db.execute(sql`
      update core.market as m
      set event_id = v.event_id,
          updated_at = ${updatedAt}
      from (values ${valuesSql}) as v(market_id, event_id)
      where m.id = v.market_id
        and m.event_id is null
    `);

    updatedRows += Number(result.rowCount ?? 0);
  }

  return updatedRows;
}

async function archiveUnseenActiveMarkets(providerCode: ProviderCode, seenAt: Date): Promise<number> {
  const platformId = await getPlatformId(providerCode);
  const seenAtIso = seenAt.toISOString();

  const result = await db.execute(sql`
    update core.market
    set
      status = 'archived',
      raw_json = coalesce(raw_json, '{}'::jsonb) || jsonb_build_object('_stale_unseen', true, '_stale_marked_at', ${seenAtIso}::text),
      updated_at = ${seenAtIso}::timestamptz
    where platform_id = ${platformId}
      and status = 'active'
      and updated_at < ${seenAtIso}::timestamptz
  `);

  return Number(result.rowCount ?? 0);
}

function parseBackfillCursor(raw: Record<string, unknown> | null): BackfillCursor {
  const numeric = (value: unknown, fallback: number): number => {
    if (typeof value === "number" && Number.isFinite(value)) {
      return value;
    }
    return fallback;
  };

  const bool = (value: unknown, fallback: boolean): boolean => {
    if (typeof value === "boolean") {
      return value;
    }
    return fallback;
  };

  return {
    eventsOffset: numeric(raw?.eventsOffset, 0),
    marketsOffset: numeric(raw?.marketsOffset, 0),
    eventsDone: bool(raw?.eventsDone, false),
    marketsDone: bool(raw?.marketsDone, false),
    completed: bool(raw?.completed, false)
  };
}

function parseKalshiMetadataCursor(raw: Record<string, unknown> | null): KalshiMetadataCursor {
  const asCursor = (value: unknown): string | null => (typeof value === "string" && value.length > 0 ? value : null);
  const asBool = (value: unknown): boolean => (typeof value === "boolean" ? value : false);
  const asCycle = (value: unknown): number =>
    typeof value === "number" && Number.isFinite(value) && value >= 0 ? Math.floor(value) : 0;

  return {
    eventsCursor: asCursor(raw?.eventsCursor),
    marketsCursor: asCursor(raw?.marketsCursor),
    multivariateCursor: asCursor(raw?.multivariateCursor),
    eventsDone: asBool(raw?.eventsDone),
    marketsDone: asBool(raw?.marketsDone),
    multivariateDone: asBool(raw?.multivariateDone),
    completed: asBool(raw?.completed),
    cycle: asCycle(raw?.cycle)
  };
}

function resetBackfillCursor(cycle: number): BackfillCursor & { cycle: number } {
  return {
    eventsOffset: 0,
    marketsOffset: 0,
    eventsDone: false,
    marketsDone: false,
    completed: false,
    cycle
  };
}

function resetKalshiMetadataCursor(cycle: number): KalshiMetadataCursor {
  return {
    eventsCursor: null,
    marketsCursor: null,
    multivariateCursor: null,
    eventsDone: false,
    marketsDone: false,
    multivariateDone: false,
    completed: false,
    cycle
  };
}

export async function relinkMarketEvents(providerCode: ProviderCode, options?: RelinkOptions): Promise<JobRunResult> {
  const platformId = await getPlatformId(providerCode);
  const adapter = getAdapter(providerCode);
  const maxMarkets = options?.maxMarkets ?? env.MARKET_RELINK_MAX_MARKETS_PER_RUN;

  const candidateRows = await db
    .select({
      marketId: market.id,
      rawJson: market.rawJson
    })
    .from(market)
    .where(and(eq(market.platformId, platformId), sql`${market.eventId} is null`))
    .orderBy(market.id)
    .limit(maxMarkets);

  if (candidateRows.length === 0) {
    await setCheckpoint(providerCode, "market:relink:events", {
      lastRunAt: new Date().toISOString(),
      requestId: options?.requestId ?? null,
      scanned: 0,
      linked: 0,
      unresolved: 0,
      remainingNullEventId: 0
    });
    return { rowsUpserted: 0, rowsSkipped: 0 };
  }

  const marketToEventRef = new Map<number, string>();
  const eventRefs: string[] = [];

  for (const row of candidateRows) {
    const extracted = extractEventRefFromMarketRaw(providerCode, row.rawJson, adapter.normalizeMarketRef.bind(adapter));
    if (!extracted) {
      continue;
    }
    marketToEventRef.set(row.marketId, extracted);
    eventRefs.push(extracted);
  }

  const eventIdMap = await loadEventIdMap(platformId, eventRefs);
  const uniqueEventRefs = Array.from(new Set(eventRefs));
  const missingEventRefs = uniqueEventRefs.filter((eventRef) => !eventIdMap.has(eventRef));

  let fallbackEventsFetched = 0;

  if (providerCode === "kalshi" && missingEventRefs.length > 0 && adapter instanceof KalshiAdapter) {
    const fallbackTargets = missingEventRefs.slice(0, env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN);
    const fallbackEvents = await adapter.listEventsByTickers(fallbackTargets);
    fallbackEventsFetched = fallbackEvents.length;

    if (fallbackEvents.length > 0) {
      await upsertProviderMetadata({
        providerCode,
        events: fallbackEvents,
        markets: [],
        instruments: [],
        metadataTimestamp: new Date()
      });

      const fallbackEventIdMap = await loadEventIdMap(platformId, fallbackTargets);
      for (const [eventRef, eventId] of fallbackEventIdMap) {
        eventIdMap.set(eventRef, eventId);
      }
    }
  }

  const links: Array<{ marketId: number; eventId: number }> = [];

  for (const [marketId, eventRef] of marketToEventRef) {
    const eventId = eventIdMap.get(eventRef);
    if (eventId) {
      links.push({ marketId, eventId });
    }
  }

  const linked = await applyMarketEventLinks(links, new Date());
  const unresolved = candidateRows.length - linked;

  const remaining = await db.execute(sql`
    select count(*)::int as value
    from core.market
    where platform_id = ${platformId}
      and event_id is null
  `);

  const remainingNullEventId = Number((remaining.rows[0] as { value?: number } | undefined)?.value ?? 0);

  await setCheckpoint(providerCode, "market:relink:events", {
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    scanned: candidateRows.length,
    extractedEventRefs: uniqueEventRefs.length,
    missingEventRefs: missingEventRefs.length,
    fallbackEventsFetched,
    linked,
    unresolved,
    remainingNullEventId
  });

  return {
    rowsUpserted: linked,
    rowsSkipped: unresolved
  };
}

export async function syncPolymarketMetadata(options?: SyncOptions): Promise<JobRunResult> {
  const providerCode: ProviderCode = "polymarket";
  const adapter = getAdapter(providerCode);

  if (!(adapter instanceof PolymarketAdapter)) {
    throw new Error("Polymarket adapter implementation mismatch");
  }

  const eventsResult = await adapter.listEventsWithState({
    activeOnly: true,
    offsetStart: 0,
    maxRequests: env.POLYMARKET_METADATA_MAX_REQUESTS_PER_RUN,
    runBudgetMs: env.POLYMARKET_METADATA_RUN_BUDGET_MS
  });
  const marketsResult = await adapter.listMarketsWithState({
    activeOnly: true,
    offsetStart: 0,
    maxRequests: env.POLYMARKET_METADATA_MAX_REQUESTS_PER_RUN,
    runBudgetMs: env.POLYMARKET_METADATA_RUN_BUDGET_MS
  });
  const instruments = await adapter.listInstruments(marketsResult.items);

  logger.info(
    {
      providerCode,
      mode: "incremental",
      events: eventsResult.items.length,
      markets: marketsResult.items.length,
      instruments: instruments.length
    },
    "Fetched polymarket metadata"
  );

  const upsert = await upsertProviderMetadata({
    providerCode,
    events: eventsResult.items,
    markets: marketsResult.items,
    instruments
  });

  const partialReasons = [eventsResult.state.partialReason, marketsResult.state.partialReason].filter(
    (item): item is string => !!item
  );

  await setCheckpoint(providerCode, "polymarket:sync:metadata", {
    mode: "incremental",
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    eventsFetched: eventsResult.items.length,
    marketsFetched: marketsResult.items.length,
    instrumentsFetched: instruments.length,
    eventsStopReason: eventsResult.state.stopReason,
    marketsStopReason: marketsResult.state.stopReason,
    partialReason: partialReasons.join(" | ") || null
  });

  return {
    ...upsert,
    partialReason: partialReasons.join(" | ") || null
  };
}

export async function syncPolymarketMetadataBackfill(options?: SyncOptions): Promise<JobRunResult> {
  const providerCode: ProviderCode = "polymarket";
  const adapter = getAdapter(providerCode);

  if (!(adapter instanceof PolymarketAdapter)) {
    throw new Error("Polymarket adapter implementation mismatch");
  }

  const cursor = parseBackfillCursor(await getCheckpoint(providerCode, "polymarket:sync:metadata:backfill_full"));

  if (cursor.completed) {
    return { rowsUpserted: 0, rowsSkipped: 0 };
  }

  const eventsResult = cursor.eventsDone
    ? {
        items: [] as NormalizedEvent[],
        state: {
          nextOffset: cursor.eventsOffset,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listEventsWithState({
        activeOnly: false,
        offsetStart: cursor.eventsOffset,
        maxRequests: env.POLYMARKET_BACKFILL_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.POLYMARKET_BACKFILL_RUN_BUDGET_MS
      });

  const marketsResult = cursor.marketsDone
    ? {
        items: [] as NormalizedMarket[],
        state: {
          nextOffset: cursor.marketsOffset,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listMarketsWithState({
        activeOnly: false,
        offsetStart: cursor.marketsOffset,
        maxRequests: env.POLYMARKET_BACKFILL_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.POLYMARKET_BACKFILL_RUN_BUDGET_MS
      });

  const instruments = await adapter.listInstruments(marketsResult.items);

  const upsert = await upsertProviderMetadata({
    providerCode,
    events: eventsResult.items,
    markets: marketsResult.items,
    instruments
  });

  const nextCursor: BackfillCursor = {
    eventsOffset: eventsResult.state.nextOffset,
    marketsOffset: marketsResult.state.nextOffset,
    eventsDone: cursor.eventsDone || eventsResult.state.completed,
    marketsDone: cursor.marketsDone || marketsResult.state.completed,
    completed: (cursor.eventsDone || eventsResult.state.completed) && (cursor.marketsDone || marketsResult.state.completed)
  };

  const partialReasons = [eventsResult.state.partialReason, marketsResult.state.partialReason].filter(
    (item): item is string => !!item
  );

  await setCheckpoint(providerCode, "polymarket:sync:metadata:backfill_full", {
    mode: "backfill",
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    eventsFetched: eventsResult.items.length,
    marketsFetched: marketsResult.items.length,
    instrumentsFetched: instruments.length,
    ...nextCursor,
    partialReason: partialReasons.join(" | ") || null,
    progress: nextCursor.completed ? "completed" : "in_progress"
  });

  logger.info(
    {
      providerCode,
      mode: "backfill",
      progress: nextCursor,
      rowsUpserted: upsert.rowsUpserted,
      rowsSkipped: upsert.rowsSkipped,
      partialReason: partialReasons.join(" | ") || null
    },
    "Polymarket metadata backfill chunk finished"
  );

  return {
    ...upsert,
    partialReason: partialReasons.join(" | ") || null
  };
}

export async function syncPolymarketMetadataFullCatalog(options?: SyncOptions): Promise<JobRunResult> {
  const providerCode: ProviderCode = "polymarket";
  const adapter = getAdapter(providerCode);

  if (!(adapter instanceof PolymarketAdapter)) {
    throw new Error("Polymarket adapter implementation mismatch");
  }

  const checkpointKey = "polymarket:sync:metadata:full_catalog";
  const rawCheckpoint = await getCheckpoint(providerCode, checkpointKey);
  const rawCursor = parseBackfillCursor(rawCheckpoint);
  const currentCycle =
    typeof rawCheckpoint?.cycle === "number" && Number.isFinite(rawCheckpoint.cycle) && rawCheckpoint.cycle >= 0
      ? Math.floor(rawCheckpoint.cycle)
      : 0;
  const activeCursor = rawCursor.completed ? resetBackfillCursor(currentCycle + 1) : { ...rawCursor, cycle: currentCycle };

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode: "full_catalog",
    requestId: options?.requestId,
    phase: "started",
    nextMarketCursorId: activeCursor.marketsOffset
  });

  const eventsResult = activeCursor.eventsDone
    ? {
        items: [] as NormalizedEvent[],
        state: {
          nextOffset: activeCursor.eventsOffset,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listEventsWithState({
        activeOnly: true,
        offsetStart: activeCursor.eventsOffset,
        maxRequests: env.POLYMARKET_METADATA_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.POLYMARKET_METADATA_RUN_BUDGET_MS
      });

  const marketsResult = activeCursor.marketsDone
    ? {
        items: [] as NormalizedMarket[],
        state: {
          nextOffset: activeCursor.marketsOffset,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listMarketsWithState({
        activeOnly: true,
        offsetStart: activeCursor.marketsOffset,
        maxRequests: env.POLYMARKET_METADATA_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.POLYMARKET_METADATA_RUN_BUDGET_MS
      });

  const instruments = await adapter.listInstruments(marketsResult.items);

  const upsert = await upsertProviderMetadata({
    providerCode,
    events: eventsResult.items,
    markets: marketsResult.items,
    instruments
  });

  const nextCursor = {
    eventsOffset: eventsResult.state.nextOffset,
    marketsOffset: marketsResult.state.nextOffset,
    eventsDone: activeCursor.eventsDone || eventsResult.state.completed,
    marketsDone: activeCursor.marketsDone || marketsResult.state.completed
  };
  const completed = nextCursor.eventsDone && nextCursor.marketsDone;

  const partialReasons = [eventsResult.state.partialReason, marketsResult.state.partialReason].filter(
    (item): item is string => !!item
  );

  await setCheckpoint(providerCode, checkpointKey, {
    mode: "full_catalog",
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    cycle: activeCursor.cycle,
    eventsFetched: eventsResult.items.length,
    marketsFetched: marketsResult.items.length,
    instrumentsFetched: instruments.length,
    eventsStopReason: eventsResult.state.stopReason,
    marketsStopReason: marketsResult.state.stopReason,
    ...nextCursor,
    completed,
    progress: completed ? "completed" : "in_progress",
    partialReason: partialReasons.join(" | ") || null
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode: "full_catalog",
    requestId: options?.requestId,
    phase: completed ? "completed" : "chunk_completed",
    rowsFetched: eventsResult.items.length + marketsResult.items.length + instruments.length,
    rowsUpserted: upsert.rowsUpserted,
    rowsSkipped: upsert.rowsSkipped,
    nextMarketCursorId: nextCursor.marketsOffset
  });

  return {
    ...upsert,
    partialReason: partialReasons.join(" | ") || null
  };
}

export async function syncKalshiMetadata(options?: SyncOptions): Promise<JobRunResult> {
  const providerCode: ProviderCode = "kalshi";
  const adapter = getAdapter(providerCode);

  if (!(adapter instanceof KalshiAdapter)) {
    throw new Error("Kalshi adapter implementation mismatch");
  }

  const metadataTimestamp = new Date();

  const marketsResult = await adapter.listMarketsWithState({
    maxRequests: env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
    runBudgetMs: env.KALSHI_METADATA_RUN_BUDGET_MS
  });
  const openEventsResult = await adapter.listEventsWithState({
    maxRequests: env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
    runBudgetMs: env.KALSHI_METADATA_RUN_BUDGET_MS
  });
  const multivariateEventsResult = await adapter.listMultivariateEventsWithState({
    maxRequests: env.KALSHI_MULTIVARIATE_MAX_REQUESTS_PER_RUN,
    runBudgetMs: env.KALSHI_MULTIVARIATE_RUN_BUDGET_MS
  });

  const mergedEventMap = new Map<string, NormalizedEvent>();
  for (const row of [...openEventsResult.items, ...multivariateEventsResult.items]) {
    mergedEventMap.set(row.eventRef, row);
  }

  const marketEventRefs = Array.from(
    new Set(marketsResult.items.map((item) => item.eventRef).filter((value): value is string => value !== null))
  );
  const missingEventRefs = marketEventRefs.filter((eventRef) => !mergedEventMap.has(eventRef));
  const fallbackTargets = missingEventRefs.slice(0, env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN);
  const fallbackEvents = await adapter.listEventsByTickers(fallbackTargets);

  for (const row of fallbackEvents) {
    mergedEventMap.set(row.eventRef, row);
  }

  const events = Array.from(mergedEventMap.values());
  const markets = marketsResult.items;
  const instruments = await adapter.listInstruments(markets);

  const partialReasons = [
    marketsResult.state.partialReason,
    openEventsResult.state.partialReason,
    multivariateEventsResult.state.partialReason
  ].filter((item): item is string => !!item);

  if (missingEventRefs.length > env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN) {
    partialReasons.push(
      `Skipped ${missingEventRefs.length - env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN} Kalshi direct event lookups due fallback cap (${env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN})`
    );
  }

  logger.info(
    {
      providerCode,
      mode: "incremental",
      eventsOpen: openEventsResult.items.length,
      eventsMultivariate: multivariateEventsResult.items.length,
      eventsFallback: fallbackEvents.length,
      events: events.length,
      markets: markets.length,
      instruments: instruments.length,
      missingEventRefs: missingEventRefs.length,
      marketStopReason: marketsResult.state.stopReason,
      marketPaginationComplete: marketsResult.state.completed
    },
    "Fetched kalshi metadata"
  );

  const upsert = await upsertProviderMetadata({
    providerCode,
    events,
    markets,
    instruments,
    metadataTimestamp
  });

  let archivedStaleMarkets = 0;
  if (marketsResult.state.completed) {
    archivedStaleMarkets = await archiveUnseenActiveMarkets(providerCode, metadataTimestamp);
  } else {
    partialReasons.push("Skipped stale market archival due incomplete Kalshi market pagination");
  }

  await setCheckpoint(providerCode, "kalshi:sync:metadata", {
    mode: "incremental",
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    eventsFetched: events.length,
    marketsFetched: markets.length,
    instrumentsFetched: instruments.length,
    eventsOpenFetched: openEventsResult.items.length,
    eventsMultivariateFetched: multivariateEventsResult.items.length,
    fallbackEventsFetched: fallbackEvents.length,
    missingEventRefs: missingEventRefs.length,
    archivedStaleMarkets,
    marketsStopReason: marketsResult.state.stopReason,
    marketsCompleted: marketsResult.state.completed,
    partialReason: partialReasons.join(" | ") || null
  });

  return {
    rowsUpserted: upsert.rowsUpserted + archivedStaleMarkets,
    rowsSkipped: upsert.rowsSkipped,
    partialReason: partialReasons.join(" | ") || null
  };
}

export async function syncKalshiMetadataFullCatalog(options?: SyncOptions): Promise<JobRunResult> {
  const providerCode: ProviderCode = "kalshi";
  const adapter = getAdapter(providerCode);

  if (!(adapter instanceof KalshiAdapter)) {
    throw new Error("Kalshi adapter implementation mismatch");
  }

  const checkpointKey = "kalshi:sync:metadata:full_catalog";
  const rawCursor = parseKalshiMetadataCursor(await getCheckpoint(providerCode, checkpointKey));
  const activeCursor = rawCursor.completed ? resetKalshiMetadataCursor(rawCursor.cycle + 1) : rawCursor;
  const metadataTimestamp = new Date();

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode: "full_catalog",
    requestId: options?.requestId,
    phase: "started"
  });

  const marketsResult = activeCursor.marketsDone
    ? {
        items: [] as NormalizedMarket[],
        state: {
          nextCursor: activeCursor.marketsCursor,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listMarketsWithState({
        cursorStart: activeCursor.marketsCursor,
        maxRequests: env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.KALSHI_METADATA_RUN_BUDGET_MS
      });

  const openEventsResult = activeCursor.eventsDone
    ? {
        items: [] as NormalizedEvent[],
        state: {
          nextCursor: activeCursor.eventsCursor,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listEventsWithState({
        cursorStart: activeCursor.eventsCursor,
        maxRequests: env.KALSHI_METADATA_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.KALSHI_METADATA_RUN_BUDGET_MS
      });

  const multivariateEventsResult = activeCursor.multivariateDone
    ? {
        items: [] as NormalizedEvent[],
        state: {
          nextCursor: activeCursor.multivariateCursor,
          pagesFetched: 0,
          completed: true,
          stopReason: "exhausted" as const,
          partialReason: null
        }
      }
    : await adapter.listMultivariateEventsWithState({
        cursorStart: activeCursor.multivariateCursor,
        maxRequests: env.KALSHI_MULTIVARIATE_MAX_REQUESTS_PER_RUN,
        runBudgetMs: env.KALSHI_MULTIVARIATE_RUN_BUDGET_MS
      });

  const mergedEventMap = new Map<string, NormalizedEvent>();
  for (const row of [...openEventsResult.items, ...multivariateEventsResult.items]) {
    mergedEventMap.set(row.eventRef, row);
  }

  const marketEventRefs = Array.from(
    new Set(marketsResult.items.map((item) => item.eventRef).filter((value): value is string => value !== null))
  );
  const missingEventRefs = marketEventRefs.filter((eventRef) => !mergedEventMap.has(eventRef));
  const fallbackTargets = missingEventRefs.slice(0, env.KALSHI_EVENT_FALLBACK_MAX_PER_RUN);
  const fallbackEvents = await adapter.listEventsByTickers(fallbackTargets);

  for (const row of fallbackEvents) {
    mergedEventMap.set(row.eventRef, row);
  }

  const events = Array.from(mergedEventMap.values());
  const markets = marketsResult.items;
  const instruments = await adapter.listInstruments(markets);

  const upsert = await upsertProviderMetadata({
    providerCode,
    events,
    markets,
    instruments,
    metadataTimestamp
  });

  const nextCursor = {
    eventsCursor: openEventsResult.state.nextCursor,
    marketsCursor: marketsResult.state.nextCursor,
    multivariateCursor: multivariateEventsResult.state.nextCursor,
    eventsDone: activeCursor.eventsDone || openEventsResult.state.completed,
    marketsDone: activeCursor.marketsDone || marketsResult.state.completed,
    multivariateDone: activeCursor.multivariateDone || multivariateEventsResult.state.completed
  };
  const completed = nextCursor.eventsDone && nextCursor.marketsDone && nextCursor.multivariateDone;

  let archivedStaleMarkets = 0;
  if (nextCursor.marketsDone) {
    archivedStaleMarkets = await archiveUnseenActiveMarkets(providerCode, metadataTimestamp);
  }

  const partialReasons = [
    marketsResult.state.partialReason,
    openEventsResult.state.partialReason,
    multivariateEventsResult.state.partialReason
  ].filter((item): item is string => !!item);

  await setCheckpoint(providerCode, checkpointKey, {
    mode: "full_catalog",
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    cycle: activeCursor.cycle,
    marketsFetched: marketsResult.items.length,
    eventsFetched: events.length,
    instrumentsFetched: instruments.length,
    fallbackEventsFetched: fallbackEvents.length,
    missingEventRefs: missingEventRefs.length,
    archivedStaleMarkets,
    ...nextCursor,
    completed,
    progress: completed ? "completed" : "in_progress",
    partialReason: partialReasons.join(" | ") || null
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode: "full_catalog",
    requestId: options?.requestId,
    phase: completed ? "completed" : "chunk_completed",
    rowsFetched: markets.length + events.length + instruments.length,
    rowsUpserted: upsert.rowsUpserted + archivedStaleMarkets,
    rowsSkipped: upsert.rowsSkipped
  });

  return {
    rowsUpserted: upsert.rowsUpserted + archivedStaleMarkets,
    rowsSkipped: upsert.rowsSkipped,
    partialReason: partialReasons.join(" | ") || null
  };
}

async function listScopedInstruments(providerCode: ProviderCode, scopeStatus: SyncOptions["scopeStatus"]): Promise<
  SelectionInstrumentRow[]
> {
  const platformId = await getPlatformId(providerCode);

  const whereClauses = [eq(marketScope.platformId, platformId)];

  if (scopeStatus === "active") {
    whereClauses.push(eq(market.status, "active"));
  } else if (scopeStatus === "closed") {
    whereClauses.push(inArray(market.status, ["closed", "archived"]));
  }

  return db
    .select({
      marketId: market.id,
      marketRef: market.marketRef,
      marketStatus: market.status,
      instrumentId: instrument.id,
      instrumentRef: instrument.instrumentRef
    })
    .from(marketScope)
    .innerJoin(market, eq(market.id, marketScope.marketId))
    .innerJoin(instrument, eq(instrument.marketId, market.id))
    .where(and(...whereClauses));
}

async function listScopedMarkets(providerCode: ProviderCode, scopeStatus: SyncOptions["scopeStatus"]): Promise<
  SelectionMarketRow[]
> {
  const platformId = await getPlatformId(providerCode);

  const whereClauses = [eq(marketScope.platformId, platformId)];

  if (scopeStatus === "active") {
    whereClauses.push(eq(market.status, "active"));
  } else if (scopeStatus === "closed") {
    whereClauses.push(inArray(market.status, ["closed", "archived"]));
  }

  return db
    .select({
      marketId: market.id,
      marketRef: market.marketRef,
      marketStatus: market.status
    })
    .from(marketScope)
    .innerJoin(market, eq(market.id, marketScope.marketId))
    .where(and(...whereClauses));
}

async function listFullCatalogMarketBatch(
  providerCode: ProviderCode,
  scopeStatus: "active" | "closed" | "all",
  afterMarketId: number,
  limit: number
): Promise<SelectionMarketRow[]> {
  const platformId = await getPlatformId(providerCode);
  const whereClauses = [eq(market.platformId, platformId), sql`${market.id} > ${afterMarketId}`];

  if (scopeStatus === "active") {
    whereClauses.push(eq(market.status, "active"));
  } else if (scopeStatus === "closed") {
    whereClauses.push(inArray(market.status, ["closed", "archived"]));
  }

  return db
    .select({
      marketId: market.id,
      marketRef: market.marketRef,
      marketStatus: market.status
    })
    .from(market)
    .where(and(...whereClauses))
    .orderBy(market.id)
    .limit(limit);
}

async function listInstrumentsForMarkets(providerCode: ProviderCode, marketIds: number[]): Promise<SelectionInstrumentRow[]> {
  if (marketIds.length === 0) {
    return [];
  }

  const platformId = await getPlatformId(providerCode);

  return db
    .select({
      marketId: market.id,
      marketRef: market.marketRef,
      marketStatus: market.status,
      instrumentId: instrument.id,
      instrumentRef: instrument.instrumentRef
    })
    .from(market)
    .innerJoin(instrument, eq(instrument.marketId, market.id))
    .where(and(eq(market.platformId, platformId), inArray(market.id, marketIds)))
    .orderBy(market.id, instrument.id);
}

async function resolveSelectionState(params: {
  providerCode: ProviderCode;
  baseCheckpointKey: string;
  scopeStatus: "active" | "closed" | "all";
  mode: "topN_live" | "full_catalog";
}): Promise<SelectionState> {
  if (params.mode === "topN_live") {
    return {
      mode: "topN_live",
      cursorKey: null,
      nextMarketCursorId: null,
      cycleCompleted: true
    };
  }

  const cursorKey = getFullCatalogCursorKey(params.baseCheckpointKey, params.scopeStatus);
  const cursor = await getCheckpoint(params.providerCode, cursorKey);

  return {
    mode: "full_catalog",
    cursorKey,
    nextMarketCursorId: parseCursorMarketId(cursor),
    cycleCompleted: typeof cursor?.cycleCompleted === "boolean" ? cursor.cycleCompleted : false
  };
}

async function selectInstruments(params: {
  providerCode: ProviderCode;
  baseCheckpointKey: string;
  scopeStatus: "active" | "closed" | "all";
  mode: "topN_live" | "full_catalog";
  requestId?: string;
}): Promise<{ rows: SelectionInstrumentRow[]; selectedMarkets: number; selectionState: SelectionState }> {
  if (params.mode === "topN_live") {
    const rows = await listScopedInstruments(params.providerCode, params.scopeStatus);
    return {
      rows,
      selectedMarkets: new Set(rows.map((row) => row.marketId)).size,
      selectionState: {
        mode: "topN_live",
        cursorKey: null,
        nextMarketCursorId: null,
        cycleCompleted: true
      }
    };
  }

  const selectionState = await resolveSelectionState(params);
  const rows: SelectionInstrumentRow[] = [];
  let cursorMarketId = selectionState.nextMarketCursorId ?? 0;
  let selectedMarkets = 0;
  let batchesProcessed = 0;
  let restartedFromZero = false;

  while (true) {
    const marketBatch = await listFullCatalogMarketBatch(
      params.providerCode,
      params.scopeStatus,
      cursorMarketId,
      env.FULL_CATALOG_MARKET_BATCH_SIZE
    );

    if (marketBatch.length === 0) {
      if (cursorMarketId > 0 && selectedMarkets === 0 && !restartedFromZero) {
        cursorMarketId = 0;
        restartedFromZero = true;
        continue;
      }
      break;
    }

    batchesProcessed += 1;
    selectedMarkets += marketBatch.length;
    cursorMarketId = marketBatch[marketBatch.length - 1]!.marketId;

    const marketIds = marketBatch.map((row) => row.marketId);
    const instrumentRows = await listInstrumentsForMarkets(params.providerCode, marketIds);
    rows.push(...instrumentRows);

    await setCheckpoint(params.providerCode, selectionState.cursorKey!, {
      mode: "full_catalog",
      requestId: params.requestId ?? null,
      lastRunAt: new Date().toISOString(),
      nextMarketCursorId: cursorMarketId,
      batchesProcessed,
      selectedMarkets,
      selectedInstruments: rows.length,
      cycleCompleted: false
    });

    logIngestionProgress({
      providerCode: params.providerCode,
      flow: `${params.baseCheckpointKey}:selector_instruments`,
      mode: params.mode,
      requestId: params.requestId,
      phase: "selection_progress",
      scopeStatus: params.scopeStatus,
      batchesProcessed,
      selectedMarkets,
      selectedInstruments: rows.length,
      nextMarketCursorId: cursorMarketId
    });
  }

  await setCheckpoint(params.providerCode, selectionState.cursorKey!, {
    mode: "full_catalog",
    requestId: params.requestId ?? null,
    lastRunAt: new Date().toISOString(),
    nextMarketCursorId: 0,
    batchesProcessed,
    selectedMarkets,
    selectedInstruments: rows.length,
    cycleCompleted: true
  });

  logIngestionProgress({
    providerCode: params.providerCode,
    flow: `${params.baseCheckpointKey}:selector_instruments`,
    mode: params.mode,
    requestId: params.requestId,
    phase: "selection_completed",
    scopeStatus: params.scopeStatus,
    batchesProcessed,
    selectedMarkets,
    selectedInstruments: rows.length,
    nextMarketCursorId: 0
  });

  return {
    rows,
    selectedMarkets,
    selectionState: {
      ...selectionState,
      nextMarketCursorId: 0,
      cycleCompleted: true
    }
  };
}

async function selectMarkets(params: {
  providerCode: ProviderCode;
  baseCheckpointKey: string;
  scopeStatus: "active" | "closed" | "all";
  mode: "topN_live" | "full_catalog";
  requestId?: string;
}): Promise<{ rows: SelectionMarketRow[]; selectionState: SelectionState }> {
  if (params.mode === "topN_live") {
    const rows = await listScopedMarkets(params.providerCode, params.scopeStatus);
    return {
      rows,
      selectionState: {
        mode: "topN_live",
        cursorKey: null,
        nextMarketCursorId: null,
        cycleCompleted: true
      }
    };
  }

  const selectionState = await resolveSelectionState(params);
  const rows: SelectionMarketRow[] = [];
  let cursorMarketId = selectionState.nextMarketCursorId ?? 0;
  let batchesProcessed = 0;
  let restartedFromZero = false;

  while (true) {
    const marketBatch = await listFullCatalogMarketBatch(
      params.providerCode,
      params.scopeStatus,
      cursorMarketId,
      env.FULL_CATALOG_MARKET_BATCH_SIZE
    );

    if (marketBatch.length === 0) {
      if (cursorMarketId > 0 && rows.length === 0 && !restartedFromZero) {
        cursorMarketId = 0;
        restartedFromZero = true;
        continue;
      }
      break;
    }

    batchesProcessed += 1;
    cursorMarketId = marketBatch[marketBatch.length - 1]!.marketId;
    rows.push(...marketBatch);

    await setCheckpoint(params.providerCode, selectionState.cursorKey!, {
      mode: "full_catalog",
      requestId: params.requestId ?? null,
      lastRunAt: new Date().toISOString(),
      nextMarketCursorId: cursorMarketId,
      batchesProcessed,
      selectedMarkets: rows.length,
      cycleCompleted: false
    });

    logIngestionProgress({
      providerCode: params.providerCode,
      flow: `${params.baseCheckpointKey}:selector_markets`,
      mode: params.mode,
      requestId: params.requestId,
      phase: "selection_progress",
      scopeStatus: params.scopeStatus,
      batchesProcessed,
      selectedMarkets: rows.length,
      nextMarketCursorId: cursorMarketId
    });
  }

  await setCheckpoint(params.providerCode, selectionState.cursorKey!, {
    mode: "full_catalog",
    requestId: params.requestId ?? null,
    lastRunAt: new Date().toISOString(),
    nextMarketCursorId: 0,
    batchesProcessed,
    selectedMarkets: rows.length,
    cycleCompleted: true
  });

  logIngestionProgress({
    providerCode: params.providerCode,
    flow: `${params.baseCheckpointKey}:selector_markets`,
    mode: params.mode,
    requestId: params.requestId,
    phase: "selection_completed",
    scopeStatus: params.scopeStatus,
    batchesProcessed,
    selectedMarkets: rows.length,
    nextMarketCursorId: 0
  });

  return {
    rows,
    selectionState: {
      ...selectionState,
      nextMarketCursorId: 0,
      cycleCompleted: true
    }
  };
}

async function syncProviderPrices(providerCode: ProviderCode, checkpointKey: string, options?: SyncOptions): Promise<JobRunResult> {
  const adapter = getAdapter(providerCode);
  const mode = resolveIngestMode(options?.mode);
  const scopeStatus = resolveScopeStatus(options?.scopeStatus, mode);
  const window = await resolveIncrementalWindow({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    initialLookbackDays: env.PRICE_LOOKBACK_DAYS,
    mode
  });

  const selected = await selectInstruments({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    mode,
    requestId: options?.requestId
  });
  const uniqueInstruments: AdapterInstrumentInput[] = Array.from(
    selected.rows
      .reduce((map, row) => {
        map.set(row.instrumentRef, {
          marketRef: row.marketRef,
          instrumentRef: row.instrumentRef
        });
        return map;
      }, new Map<string, AdapterInstrumentInput>())
      .values()
  );

  const instrumentRefToId = new Map(selected.rows.map((row) => [row.instrumentRef, row.instrumentId]));

  let rowsUpserted = 0;
  let rowsSkipped = 0;
  let pointsFetched = 0;

  const ingestionChunkSize = mode === "full_catalog" ? env.FULL_CATALOG_INSTRUMENT_BATCH_SIZE : uniqueInstruments.length || 1;
  const instrumentChunks = chunkArray(uniqueInstruments, ingestionChunkSize);

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "started",
    scopeStatus,
    selectedMarkets: selected.selectedMarkets,
    selectedInstruments: uniqueInstruments.length,
    inputTargets: instrumentChunks.length,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  for (const [index, instrumentsChunk] of instrumentChunks.entries()) {
    if (instrumentsChunk.length === 0) {
      continue;
    }

    const chunkIndex = index + 1;
    const points = await adapter.listPricePoints(instrumentsChunk, {
      startTs: window.startTs,
      endTs: window.endTs
    });
    pointsFetched += points.length;

    for (const rows of chunkArray(points, 5000)) {
      const rowsToUpsertRaw = rows
        .map((item) => {
          const instrumentId = instrumentRefToId.get(item.instrumentRef);
          if (!instrumentId) {
            rowsSkipped += 1;
            return null;
          }

          return {
            instrumentId,
            ts: item.ts,
            price: item.price.toFixed(6),
            source: item.source
          };
        })
        .filter((item): item is NonNullable<typeof item> => item !== null);

      const rowsToUpsert = Array.from(
        rowsToUpsertRaw
          .reduce((map, row) => {
            map.set(`${row.instrumentId}:${row.ts.toISOString()}`, row);
            return map;
          }, new Map<string, (typeof rowsToUpsertRaw)[number]>())
          .values()
      );

      if (rowsToUpsert.length === 0) {
        continue;
      }

      await db
        .insert(pricePoint5m)
        .values(rowsToUpsert)
        .onConflictDoUpdate({
          target: [pricePoint5m.instrumentId, pricePoint5m.ts],
          set: {
            price: sql`excluded.price`,
            source: sql`excluded.source`
          }
        });

      rowsUpserted += rowsToUpsert.length;
    }

    if (shouldLogChunkProgress(mode, chunkIndex, instrumentChunks.length)) {
      logIngestionProgress({
        providerCode,
        flow: checkpointKey,
        mode,
        requestId: options?.requestId,
        phase: "chunk_completed",
        scopeStatus,
        chunkIndex,
        totalChunks: instrumentChunks.length,
        selectedMarkets: selected.selectedMarkets,
        selectedInstruments: uniqueInstruments.length,
        inputTargets: instrumentsChunk.length,
        rowsFetched: pointsFetched,
        rowsUpserted,
        rowsSkipped
      });
    }
  }

  await setCheckpoint(providerCode, window.cursorKey, {
    cursorVersion: 1,
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    mode,
    scopeStatus,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs,
    nextWindowStartTs: Math.max(0, window.endTs - env.INGEST_INCREMENTAL_OVERLAP_SECONDS),
    lastWindowEndTs: window.endTs,
    markets: selected.selectedMarkets,
    instruments: uniqueInstruments.length,
    points: pointsFetched
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "completed",
    scopeStatus,
    selectedMarkets: selected.selectedMarkets,
    selectedInstruments: uniqueInstruments.length,
    rowsFetched: pointsFetched,
    rowsUpserted,
    rowsSkipped,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  return { rowsUpserted, rowsSkipped };
}

async function syncProviderOrderbook(providerCode: ProviderCode, checkpointKey: string, options?: SyncOptions): Promise<JobRunResult> {
  const adapter = getAdapter(providerCode);
  const mode = resolveIngestMode(options?.mode);
  const scopeStatus = resolveScopeStatus(options?.scopeStatus, mode);
  const checkpointStateKey = getIncrementalCursorKey(checkpointKey, scopeStatus, mode);
  const selected = await selectInstruments({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    mode,
    requestId: options?.requestId
  });

  const instruments = Array.from(
    selected.rows
      .reduce((map, row) => {
        map.set(row.instrumentRef, {
          marketRef: row.marketRef,
          instrumentRef: row.instrumentRef
        });
        return map;
      }, new Map<string, AdapterInstrumentInput>())
      .values()
  );

  const instrumentRefToId = new Map(selected.rows.map((row) => [row.instrumentRef, row.instrumentId]));

  let rowsUpserted = 0;
  let rowsSkipped = 0;
  let snapshotsFetched = 0;

  const ingestionChunkSize = mode === "full_catalog" ? env.FULL_CATALOG_INSTRUMENT_BATCH_SIZE : instruments.length || 1;
  const instrumentChunks = chunkArray(instruments, ingestionChunkSize);

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "started",
    scopeStatus,
    selectedMarkets: selected.selectedMarkets,
    selectedInstruments: instruments.length,
    inputTargets: instrumentChunks.length
  });

  for (const [index, instrumentsChunk] of instrumentChunks.entries()) {
    if (instrumentsChunk.length === 0) {
      continue;
    }

    const chunkIndex = index + 1;
    const snapshots = await adapter.listOrderbookTop(instrumentsChunk);
    snapshotsFetched += snapshots.length;

    for (const rows of chunkArray(snapshots, 1000)) {
      const rowsToUpsertRaw = rows
        .map((item) => {
          const instrumentId = instrumentRefToId.get(item.instrumentRef);
          if (!instrumentId) {
            rowsSkipped += 1;
            return null;
          }

          return {
            instrumentId,
            ts: item.ts,
            bestBid: toNullableNumericString(item.bestBid),
            bestAsk: toNullableNumericString(item.bestAsk),
            spread: toNullableNumericString(item.spread),
            bidDepthTop5: toNullableDepthNumericString(item.bidDepthTop5),
            askDepthTop5: toNullableDepthNumericString(item.askDepthTop5),
            rawJson: item.rawJson
          };
        })
        .filter((item): item is NonNullable<typeof item> => item !== null);

      const rowsToUpsert = Array.from(
        rowsToUpsertRaw
          .reduce((map, row) => {
            map.set(`${row.instrumentId}:${row.ts.toISOString()}`, row);
            return map;
          }, new Map<string, (typeof rowsToUpsertRaw)[number]>())
          .values()
      );

      if (rowsToUpsert.length === 0) {
        continue;
      }

      await db
        .insert(orderbookTop)
        .values(rowsToUpsert)
        .onConflictDoUpdate({
          target: [orderbookTop.instrumentId, orderbookTop.ts],
          set: {
            bestBid: sql`excluded.best_bid`,
            bestAsk: sql`excluded.best_ask`,
            spread: sql`excluded.spread`,
            bidDepthTop5: sql`excluded.bid_depth_top5`,
            askDepthTop5: sql`excluded.ask_depth_top5`,
            rawJson: sql`excluded.raw_json`
          }
        });

      rowsUpserted += rowsToUpsert.length;
    }

    if (shouldLogChunkProgress(mode, chunkIndex, instrumentChunks.length)) {
      logIngestionProgress({
        providerCode,
        flow: checkpointKey,
        mode,
        requestId: options?.requestId,
        phase: "chunk_completed",
        scopeStatus,
        chunkIndex,
        totalChunks: instrumentChunks.length,
        selectedMarkets: selected.selectedMarkets,
        selectedInstruments: instruments.length,
        inputTargets: instrumentsChunk.length,
        rowsFetched: snapshotsFetched,
        rowsUpserted,
        rowsSkipped
      });
    }
  }

  await setCheckpoint(providerCode, checkpointStateKey, {
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    mode,
    scopeStatus,
    markets: selected.selectedMarkets,
    instruments: instruments.length,
    snapshots: snapshotsFetched
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "completed",
    scopeStatus,
    selectedMarkets: selected.selectedMarkets,
    selectedInstruments: instruments.length,
    rowsFetched: snapshotsFetched,
    rowsUpserted,
    rowsSkipped
  });

  return { rowsUpserted, rowsSkipped };
}

async function syncProviderTrades(providerCode: ProviderCode, checkpointKey: string, options?: SyncOptions): Promise<JobRunResult> {
  const adapter = getAdapter(providerCode);
  const mode = resolveIngestMode(options?.mode);
  const scopeStatus = resolveScopeStatus(options?.scopeStatus, mode);
  const window = await resolveIncrementalWindow({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    initialLookbackDays: env.TRADES_LOOKBACK_DAYS,
    mode
  });

  const selectedMarkets = await selectMarkets({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    mode,
    requestId: options?.requestId
  });

  const uniqueMarkets = Array.from(
    selectedMarkets.rows
      .reduce((map, row) => {
        map.set(row.marketRef, row);
        return map;
      }, new Map<string, (typeof selectedMarkets.rows)[number]>())
      .values()
  );

  const marketInputs: AdapterMarketInput[] = uniqueMarkets.map((row) => ({ marketRef: row.marketRef }));
  const marketRefToId = new Map(uniqueMarkets.map((row) => [row.marketRef, row.marketId]));
  const instrumentRows = await listInstrumentsForMarkets(
    providerCode,
    uniqueMarkets.map((row) => row.marketId)
  );
  const instrumentRefToId = new Map(instrumentRows.map((row) => [row.instrumentRef, row.instrumentId]));

  let rowsUpserted = 0;
  let rowsSkipped = 0;
  let tradesFetched = 0;

  const ingestionChunkSize = mode === "full_catalog" ? env.FULL_CATALOG_MARKET_BATCH_SIZE : marketInputs.length || 1;
  const marketChunks = chunkArray(marketInputs, ingestionChunkSize);

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "started",
    scopeStatus,
    selectedMarkets: marketInputs.length,
    selectedInstruments: instrumentRows.length,
    inputTargets: marketChunks.length,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  for (const [index, marketInputsChunk] of marketChunks.entries()) {
    if (marketInputsChunk.length === 0) {
      continue;
    }

    const chunkIndex = index + 1;
    const trades = await adapter.listTrades(marketInputsChunk, {
      startTs: window.startTs,
      endTs: window.endTs
    });
    tradesFetched += trades.length;

    for (const rows of chunkArray(trades, 2000)) {
      const rowsToUpsertRaw = rows
        .map((item) => {
          const marketId = marketRefToId.get(item.marketRef);
          if (!marketId || !item.tradeRef) {
            rowsSkipped += 1;
            return null;
          }

          const instrumentId = item.instrumentRef ? (instrumentRefToId.get(item.instrumentRef) ?? null) : null;

          return {
            providerCode,
            tradeRef: item.tradeRef,
            marketId,
            instrumentId,
            ts: item.ts,
            side: item.side,
            price: toNullableNumericString(item.price),
            qty: toNullableDepthNumericString(item.qty),
            notionalUsd: toNullableDepthNumericString(item.notionalUsd),
            traderRef: item.traderRef,
            source: item.source,
            rawJson: item.rawJson
          };
        })
        .filter((item): item is NonNullable<typeof item> => item !== null);

      const rowsToUpsert = Array.from(
        rowsToUpsertRaw
          .reduce((map, row) => {
            map.set(`${row.providerCode}:${row.tradeRef}`, row);
            return map;
          }, new Map<string, (typeof rowsToUpsertRaw)[number]>())
          .values()
      );

      if (rowsToUpsert.length === 0) {
        continue;
      }

      await db
        .insert(tradeEvent)
        .values(rowsToUpsert)
        .onConflictDoUpdate({
          target: [tradeEvent.providerCode, tradeEvent.tradeRef],
          set: {
            marketId: sql`excluded.market_id`,
            instrumentId: sql`excluded.instrument_id`,
            ts: sql`excluded.ts`,
            side: sql`excluded.side`,
            price: sql`excluded.price`,
            qty: sql`excluded.qty`,
            notionalUsd: sql`excluded.notional_usd`,
            traderRef: sql`excluded.trader_ref`,
            source: sql`excluded.source`,
            rawJson: sql`excluded.raw_json`
          }
        });

      rowsUpserted += rowsToUpsert.length;
    }

    if (shouldLogChunkProgress(mode, chunkIndex, marketChunks.length)) {
      logIngestionProgress({
        providerCode,
        flow: checkpointKey,
        mode,
        requestId: options?.requestId,
        phase: "chunk_completed",
        scopeStatus,
        chunkIndex,
        totalChunks: marketChunks.length,
        selectedMarkets: marketInputs.length,
        selectedInstruments: instrumentRows.length,
        inputTargets: marketInputsChunk.length,
        rowsFetched: tradesFetched,
        rowsUpserted,
        rowsSkipped
      });
    }
  }

  await setCheckpoint(providerCode, window.cursorKey, {
    cursorVersion: 1,
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    mode,
    scopeStatus,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs,
    nextWindowStartTs: Math.max(0, window.endTs - env.INGEST_INCREMENTAL_OVERLAP_SECONDS),
    lastWindowEndTs: window.endTs,
    markets: marketInputs.length,
    instruments: instrumentRows.length,
    trades: tradesFetched
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "completed",
    scopeStatus,
    selectedMarkets: marketInputs.length,
    selectedInstruments: instrumentRows.length,
    rowsFetched: tradesFetched,
    rowsUpserted,
    rowsSkipped,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  return { rowsUpserted, rowsSkipped };
}

async function syncProviderOpenInterest(
  providerCode: ProviderCode,
  checkpointKey: string,
  options?: SyncOptions
): Promise<JobRunResult> {
  const adapter = getAdapter(providerCode);
  const mode = resolveIngestMode(options?.mode);
  const scopeStatus = resolveScopeStatus(options?.scopeStatus, mode);
  const window = await resolveIncrementalWindow({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    initialLookbackDays: env.OI_LOOKBACK_DAYS,
    mode
  });

  const selected = await selectMarkets({
    providerCode,
    baseCheckpointKey: checkpointKey,
    scopeStatus,
    mode,
    requestId: options?.requestId
  });
  const uniqueMarkets = Array.from(
    selected.rows
      .reduce((map, row) => {
        map.set(row.marketRef, row);
        return map;
      }, new Map<string, (typeof selected.rows)[number]>())
      .values()
  );

  const marketInputs: AdapterMarketInput[] = uniqueMarkets.map((row) => ({ marketRef: row.marketRef }));
  const marketRefToId = new Map(uniqueMarkets.map((row) => [row.marketRef, row.marketId]));

  let rowsUpserted = 0;
  let rowsSkipped = 0;
  let pointsFetched = 0;

  const ingestionChunkSize = mode === "full_catalog" ? env.FULL_CATALOG_MARKET_BATCH_SIZE : marketInputs.length || 1;
  const marketChunks = chunkArray(marketInputs, ingestionChunkSize);

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "started",
    scopeStatus,
    selectedMarkets: marketInputs.length,
    inputTargets: marketChunks.length,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  for (const [index, marketInputsChunk] of marketChunks.entries()) {
    if (marketInputsChunk.length === 0) {
      continue;
    }

    const chunkIndex = index + 1;
    const points = await adapter.listOpenInterest(marketInputsChunk, {
      startTs: window.startTs,
      endTs: window.endTs
    });
    pointsFetched += points.length;

    for (const rows of chunkArray(points, 2000)) {
      const rowsToUpsertRaw = rows
        .map((item) => {
          const marketId = marketRefToId.get(item.marketRef);
          if (!marketId) {
            rowsSkipped += 1;
            return null;
          }

          return {
            providerCode,
            marketId,
            ts: item.ts,
            value: item.value.toFixed(6),
            unit: item.unit,
            source: item.source,
            rawJson: item.rawJson
          };
        })
        .filter((item): item is NonNullable<typeof item> => item !== null);

      const rowsToUpsert = Array.from(
        rowsToUpsertRaw
          .reduce((map, row) => {
            map.set(`${row.providerCode}:${row.marketId}:${row.ts.toISOString()}`, row);
            return map;
          }, new Map<string, (typeof rowsToUpsertRaw)[number]>())
          .values()
      );

      if (rowsToUpsert.length === 0) {
        continue;
      }

      await db
        .insert(oiPoint5m)
        .values(rowsToUpsert)
        .onConflictDoUpdate({
          target: [oiPoint5m.providerCode, oiPoint5m.marketId, oiPoint5m.ts],
          set: {
            value: sql`excluded.value`,
            unit: sql`excluded.unit`,
            source: sql`excluded.source`,
            rawJson: sql`excluded.raw_json`
          }
        });

      rowsUpserted += rowsToUpsert.length;
    }

    if (shouldLogChunkProgress(mode, chunkIndex, marketChunks.length)) {
      logIngestionProgress({
        providerCode,
        flow: checkpointKey,
        mode,
        requestId: options?.requestId,
        phase: "chunk_completed",
        scopeStatus,
        chunkIndex,
        totalChunks: marketChunks.length,
        selectedMarkets: marketInputs.length,
        inputTargets: marketInputsChunk.length,
        rowsFetched: pointsFetched,
        rowsUpserted,
        rowsSkipped
      });
    }
  }

  await setCheckpoint(providerCode, window.cursorKey, {
    cursorVersion: 1,
    lastRunAt: new Date().toISOString(),
    requestId: options?.requestId ?? null,
    mode,
    scopeStatus,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs,
    nextWindowStartTs: Math.max(0, window.endTs - env.INGEST_INCREMENTAL_OVERLAP_SECONDS),
    lastWindowEndTs: window.endTs,
    markets: marketInputs.length,
    points: pointsFetched
  });

  logIngestionProgress({
    providerCode,
    flow: checkpointKey,
    mode,
    requestId: options?.requestId,
    phase: "completed",
    scopeStatus,
    selectedMarkets: marketInputs.length,
    rowsFetched: pointsFetched,
    rowsUpserted,
    rowsSkipped,
    windowStartTs: window.startTs,
    windowEndTs: window.endTs
  });

  return { rowsUpserted, rowsSkipped };
}

export async function syncPolymarketPrices(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderPrices("polymarket", "polymarket:sync:prices", options);
}

export async function syncKalshiPrices(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderPrices("kalshi", "kalshi:sync:prices", options);
}

export async function syncPolymarketOrderbook(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderOrderbook("polymarket", "polymarket:sync:orderbook", options);
}

export async function syncKalshiOrderbook(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderOrderbook("kalshi", "kalshi:sync:orderbook", options);
}

export async function syncPolymarketTrades(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderTrades("polymarket", "polymarket:sync:trades", options);
}

export async function syncKalshiTrades(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderTrades("kalshi", "kalshi:sync:trades", options);
}

export async function syncPolymarketOpenInterest(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderOpenInterest("polymarket", "polymarket:sync:oi", options);
}

export async function syncKalshiOpenInterest(options?: SyncOptions): Promise<JobRunResult> {
  return syncProviderOpenInterest("kalshi", "kalshi:sync:oi", options);
}
