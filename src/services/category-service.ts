import { and, eq, gt, inArray, sql } from "drizzle-orm";

import { db } from "../db/client.js";
import {
  categoryDim,
  event,
  market,
  marketCategoryAssignment,
  marketScope,
  platform,
  providerCategoryDim,
  providerCategoryMap
} from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";
import { getCheckpoint, setCheckpoint } from "./checkpoint-service.js";
import {
  CANONICAL_SECTORS,
  POLYMARKET_TAG_NOISE_CODES,
  getCanonicalSector,
  getProviderSeedMap,
  mapCategoryByText,
  normalizeCategoryCode,
  normalizeCategoryLabel,
  type CategoryConfidence,
  type CategorySourceKind
} from "./category-taxonomy.js";
import type { JobRunResult } from "./job-log-service.js";
import { getPlatformOrThrow } from "./platform-service.js";

export interface RebuildCategoryOptions {
  target?: "scope" | "all";
  maxMarkets?: number;
  requestId?: string;
}

interface CategoryBackfillCursor {
  lastMarketId: number;
  processedCount: number;
  completed: boolean;
  updatedAt: string;
}

interface MarketClassificationRow {
  marketId: number;
  platformId: number;
  title: string | null;
  eventCategory: string | null;
  eventRawJson: Record<string, unknown> | null;
}

interface ProviderCategoryCandidate {
  sourceKind: CategorySourceKind;
  code: string;
  label: string;
  isNoise: boolean;
}

interface ClassifiedCategory {
  canonicalCode: string;
  canonicalLabel: string;
  providerCategory: ProviderCategoryCandidate | null;
  source: string;
  confidence: CategoryConfidence;
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0 ? value.trim() : null;
}

function parseBackfillCursor(raw: Record<string, unknown> | null): CategoryBackfillCursor {
  const lastMarketId = typeof raw?.lastMarketId === "number" && Number.isFinite(raw.lastMarketId) ? raw.lastMarketId : 0;
  const processedCount =
    typeof raw?.processedCount === "number" && Number.isFinite(raw.processedCount) ? raw.processedCount : 0;
  const completed = typeof raw?.completed === "boolean" ? raw.completed : false;
  const updatedAt = typeof raw?.updatedAt === "string" ? raw.updatedAt : new Date(0).toISOString();

  return { lastMarketId, processedCount, completed, updatedAt };
}

function getScopedCheckpointJobName(_providerCode: ProviderCode): string {
  return "analytics:category:assign:markets:all";
}

function resolveCanonicalCode(
  providerCode: ProviderCode,
  sourceKind: CategorySourceKind,
  sourceCode: string,
  sourceLabel: string
): string | null {
  const seed = getProviderSeedMap(providerCode, sourceKind).get(sourceCode);
  if (seed) {
    return seed.canonicalCode;
  }

  if (sourceKind === "event_category" || sourceKind === "event_tag") {
    if (CANONICAL_SECTORS.some((sector) => sector.code === sourceCode)) {
      return sourceCode;
    }
  }

  const byText = mapCategoryByText(`${sourceCode.replaceAll("_", " ")} ${sourceLabel}`);
  if (byText) {
    return byText;
  }

  return null;
}

function extractPolymarketTagCandidates(eventRawJson: Record<string, unknown> | null): ProviderCategoryCandidate[] {
  const tagsRaw = eventRawJson?.tags;
  if (!Array.isArray(tagsRaw)) {
    return [];
  }

  const candidates: ProviderCategoryCandidate[] = [];

  for (const tag of tagsRaw) {
    const tagObj = asRecord(tag);
    if (!tagObj) {
      continue;
    }

    const slug = asString(tagObj.slug);
    const label = asString(tagObj.label) ?? slug;
    if (!label && !slug) {
      continue;
    }

    const code = normalizeCategoryCode(slug ?? label);
    candidates.push({
      sourceKind: "event_tag",
      code,
      label: normalizeCategoryLabel(label, code),
      isNoise: POLYMARKET_TAG_NOISE_CODES.has(code)
    });
  }

  return candidates;
}

function extractKalshiMetadataCandidates(eventRawJson: Record<string, unknown> | null): ProviderCategoryCandidate[] {
  const productMetadata = asRecord(eventRawJson?.product_metadata);
  if (!productMetadata) {
    return [];
  }

  const candidates: ProviderCategoryCandidate[] = [];

  const scope = asString(productMetadata.competition_scope);
  if (scope) {
    const code = normalizeCategoryCode(scope);
    candidates.push({
      sourceKind: "product_metadata_scope",
      code,
      label: normalizeCategoryLabel(scope, code),
      isNoise: false
    });
  }

  const competition = asString(productMetadata.competition);
  if (competition) {
    const code = normalizeCategoryCode(competition);
    candidates.push({
      sourceKind: "product_metadata_competition",
      code,
      label: normalizeCategoryLabel(competition, code),
      isNoise: false
    });
  }

  return candidates;
}

function classifyMarket(row: MarketClassificationRow, providerCode: ProviderCode): ClassifiedCategory {
  const eventCategory = asString(row.eventCategory);

  if (eventCategory) {
    const code = normalizeCategoryCode(eventCategory);
    const label = normalizeCategoryLabel(eventCategory, code);
    const canonicalCode = resolveCanonicalCode(providerCode, "event_category", code, label);

    if (canonicalCode && canonicalCode !== "unknown") {
      const canonical = getCanonicalSector(canonicalCode);
      return {
        canonicalCode: canonical.code,
        canonicalLabel: canonical.label,
        providerCategory: {
          sourceKind: "event_category",
          code,
          label,
          isNoise: false
        },
        source: "event.category",
        confidence: "high"
      };
    }
  }

  if (providerCode === "polymarket") {
    const tagCandidates = extractPolymarketTagCandidates(row.eventRawJson).filter((candidate) => !candidate.isNoise);

    for (let index = 0; index < tagCandidates.length; index += 1) {
      const candidate = tagCandidates[index]!;
      const canonicalCode = resolveCanonicalCode(providerCode, candidate.sourceKind, candidate.code, candidate.label);

      if (canonicalCode && canonicalCode !== "unknown") {
        const canonical = getCanonicalSector(canonicalCode);
        return {
          canonicalCode: canonical.code,
          canonicalLabel: canonical.label,
          providerCategory: candidate,
          source: index === 0 ? "event.tag.primary" : "event.tag.secondary",
          confidence: index === 0 ? "high" : "medium"
        };
      }
    }
  }

  if (providerCode === "kalshi") {
    const metadataCandidates = extractKalshiMetadataCandidates(row.eventRawJson);

    for (let index = 0; index < metadataCandidates.length; index += 1) {
      const candidate = metadataCandidates[index]!;
      const canonicalCode = resolveCanonicalCode(providerCode, candidate.sourceKind, candidate.code, candidate.label);

      if (canonicalCode && canonicalCode !== "unknown") {
        const canonical = getCanonicalSector(canonicalCode);
        return {
          canonicalCode: canonical.code,
          canonicalLabel: canonical.label,
          providerCategory: candidate,
          source: candidate.sourceKind === "product_metadata_scope" ? "event.product_metadata.scope" : "event.product_metadata.competition",
          confidence: index === 0 ? "high" : "medium"
        };
      }
    }
  }

  const canonicalByTitle = mapCategoryByText(row.title);
  if (canonicalByTitle) {
    const canonical = getCanonicalSector(canonicalByTitle);
    return {
      canonicalCode: canonical.code,
      canonicalLabel: canonical.label,
      providerCategory: null,
      source: "title.keyword",
      confidence: "low"
    };
  }

  return {
    canonicalCode: "unknown",
    canonicalLabel: "Unknown",
    providerCategory: null,
    source: "fallback.unknown",
    confidence: "low"
  };
}

async function seedCanonicalSectors(): Promise<Map<string, number>> {
  await db
    .insert(categoryDim)
    .values(
      CANONICAL_SECTORS.map((sector) => ({
        code: sector.code,
        label: sector.label,
        updatedAt: new Date()
      }))
    )
    .onConflictDoUpdate({
      target: [categoryDim.code],
      set: {
        label: sql`excluded.label`,
        updatedAt: new Date()
      }
    });

  const rows = await db
    .select({ id: categoryDim.id, code: categoryDim.code })
    .from(categoryDim)
    .where(inArray(categoryDim.code, CANONICAL_SECTORS.map((sector) => sector.code)));

  return new Map(rows.map((row) => [row.code, row.id]));
}

async function seedProviderCategoryMappings(providerCode: ProviderCode, platformId: number, canonicalByCode: Map<string, number>): Promise<number> {
  const seedRows = [
    ...Array.from(getProviderSeedMap(providerCode, "event_category").values()),
    ...Array.from(getProviderSeedMap(providerCode, "event_tag").values()),
    ...Array.from(getProviderSeedMap(providerCode, "product_metadata_scope").values()),
    ...Array.from(getProviderSeedMap(providerCode, "product_metadata_competition").values())
  ];

  const seeds = Array.from(
    new Map(
      seedRows.map((seed) => [`${seed.sourceKind}:${normalizeCategoryCode(seed.sourceCode)}`, seed] as const)
    ).values()
  );

  const rows = seeds
    .map((seed) => {
      const canonicalCategoryId = canonicalByCode.get(seed.canonicalCode);
      if (!canonicalCategoryId) {
        return null;
      }

      return {
        platformId,
        sourceKind: seed.sourceKind,
        sourceCode: normalizeCategoryCode(seed.sourceCode),
        sourceLabel: normalizeCategoryLabel(seed.sourceLabel, seed.sourceCode),
        canonicalCategoryId,
        priority: seed.priority,
        isActive: true,
        notes: seed.notes ?? null,
        updatedAt: new Date()
      };
    })
    .filter((row): row is NonNullable<typeof row> => row !== null);

  if (rows.length === 0) {
    return 0;
  }

  const upserted = await db
    .insert(providerCategoryMap)
    .values(rows)
    .onConflictDoUpdate({
      target: [providerCategoryMap.platformId, providerCategoryMap.sourceKind, providerCategoryMap.sourceCode],
      set: {
        sourceLabel: sql`excluded.source_label`,
        canonicalCategoryId: sql`excluded.canonical_category_id`,
        priority: sql`excluded.priority`,
        isActive: sql`excluded.is_active`,
        notes: sql`excluded.notes`,
        updatedAt: new Date()
      }
    })
    .returning({ id: providerCategoryMap.id });

  return upserted.length;
}

async function listMarketsForClassification(params: {
  providerCode: ProviderCode;
  target: "scope" | "all";
  lastMarketId: number;
  maxMarkets: number;
}): Promise<MarketClassificationRow[]> {
  const baseQuery = db
    .select({
      marketId: market.id,
      platformId: market.platformId,
      title: market.title,
      eventCategory: event.category,
      eventRawJson: event.rawJson
    })
    .from(market)
    .innerJoin(platform, eq(platform.id, market.platformId))
    .leftJoin(event, eq(event.id, market.eventId))
    .where(and(eq(platform.code, params.providerCode), gt(market.id, params.lastMarketId)))
    .orderBy(market.id)
    .limit(params.maxMarkets);

  if (params.target === "scope") {
    return db
      .select({
        marketId: market.id,
        platformId: market.platformId,
        title: market.title,
        eventCategory: event.category,
        eventRawJson: event.rawJson
      })
      .from(marketScope)
      .innerJoin(market, eq(market.id, marketScope.marketId))
      .innerJoin(platform, eq(platform.id, market.platformId))
      .leftJoin(event, eq(event.id, market.eventId))
      .where(and(eq(platform.code, params.providerCode), gt(market.id, params.lastMarketId)))
      .orderBy(market.id)
      .limit(params.maxMarkets);
  }

  return baseQuery;
}

async function classifyAndUpsertMarketBatch(params: {
  markets: MarketClassificationRow[];
  providerCode: ProviderCode;
  canonicalByCode: Map<string, number>;
}): Promise<{ providerCategoryRowsUpserted: number; assignmentsUpserted: number; rowsSkipped: number }> {
  if (params.markets.length === 0) {
    return { providerCategoryRowsUpserted: 0, assignmentsUpserted: 0, rowsSkipped: 0 };
  }

  const classified = params.markets.map((row) => ({
    marketId: row.marketId,
    platformId: row.platformId,
    ...classifyMarket(row, params.providerCode)
  }));

  const providerCategories = Array.from(
    new Map(
      classified
        .filter((row) => row.providerCategory !== null)
        .map((row) => {
          const providerCategory = row.providerCategory!;
          return [
            `${providerCategory.sourceKind}:${providerCategory.code}`,
            {
              platformId: row.platformId,
              sourceKind: providerCategory.sourceKind,
              code: providerCategory.code,
              label: providerCategory.label,
              isNoise: providerCategory.isNoise
            }
          ] as const;
        })
    ).values()
  );

  const providerCategoryIdByComposite = new Map<string, number>();
  let providerCategoryRowsUpserted = 0;

  if (providerCategories.length > 0) {
    const upsertedProviderCategories = await db
      .insert(providerCategoryDim)
      .values(
        providerCategories.map((row) => ({
          platformId: row.platformId,
          sourceKind: row.sourceKind,
          code: row.code,
          label: row.label,
          isNoise: row.isNoise,
          updatedAt: new Date()
        }))
      )
      .onConflictDoUpdate({
        target: [providerCategoryDim.platformId, providerCategoryDim.sourceKind, providerCategoryDim.code],
        set: {
          label: sql`excluded.label`,
          isNoise: sql`excluded.is_noise`,
          updatedAt: new Date()
        }
      })
      .returning({
        id: providerCategoryDim.id,
        sourceKind: providerCategoryDim.sourceKind,
        code: providerCategoryDim.code
      });

    providerCategoryRowsUpserted = upsertedProviderCategories.length;

    for (const row of upsertedProviderCategories) {
      providerCategoryIdByComposite.set(`${row.sourceKind}:${row.code}`, row.id);
    }
  }

  let rowsSkipped = 0;

  const assignmentRows = classified
    .map((row) => {
      const canonicalCategoryId = params.canonicalByCode.get(row.canonicalCode);
      if (!canonicalCategoryId) {
        rowsSkipped += 1;
        return null;
      }

      const providerCategoryId = row.providerCategory
        ? (providerCategoryIdByComposite.get(`${row.providerCategory.sourceKind}:${row.providerCategory.code}`) ?? null)
        : null;

      return {
        marketId: row.marketId,
        platformId: row.platformId,
        canonicalCategoryId,
        categoryId: canonicalCategoryId,
        providerCategoryId,
        source: row.source.slice(0, 32),
        confidence: row.confidence,
        assignedAt: new Date(),
        updatedAt: new Date()
      };
    })
    .filter((row): row is NonNullable<typeof row> => row !== null);

  let assignmentsUpserted = 0;

  if (assignmentRows.length > 0) {
    const upserted = await db
      .insert(marketCategoryAssignment)
      .values(assignmentRows)
      .onConflictDoUpdate({
        target: [marketCategoryAssignment.marketId],
        set: {
          platformId: sql`excluded.platform_id`,
          canonicalCategoryId: sql`excluded.canonical_category_id`,
          categoryId: sql`excluded.category_id`,
          providerCategoryId: sql`excluded.provider_category_id`,
          source: sql`excluded.source`,
          confidence: sql`excluded.confidence`,
          assignedAt: new Date(),
          updatedAt: new Date()
        }
      })
      .returning({ id: marketCategoryAssignment.id });

    assignmentsUpserted = upserted.length;
  }

  return {
    providerCategoryRowsUpserted,
    assignmentsUpserted,
    rowsSkipped
  };
}

export async function rebuildMarketCategoryAssignments(
  providerCode: ProviderCode,
  options?: RebuildCategoryOptions
): Promise<JobRunResult> {
  const target = options?.target ?? "scope";
  const hasExplicitMaxMarkets = typeof options?.maxMarkets === "number" && Number.isFinite(options.maxMarkets);
  const maxMarkets = Math.max(1, Math.min(Math.floor(options?.maxMarkets ?? 5000), 50000));
  const platformRow = await getPlatformOrThrow(providerCode);
  const canonicalByCode = await seedCanonicalSectors();
  const rowsSeeded = await seedProviderCategoryMappings(providerCode, platformRow.id, canonicalByCode);

  const checkpointJobName = getScopedCheckpointJobName(providerCode);
  const cursor = target === "all" ? parseBackfillCursor(await getCheckpoint(providerCode, checkpointJobName)) : null;
  let providerCategoryRowsUpserted = 0;
  let assignmentsUpserted = 0;
  let rowsSkipped = 0;

  if (target === "all" || hasExplicitMaxMarkets) {
    const lastMarketId = target === "all" ? cursor?.lastMarketId ?? 0 : 0;
    const markets = await listMarketsForClassification({
      providerCode,
      target,
      lastMarketId,
      maxMarkets
    });
    const batchResult = await classifyAndUpsertMarketBatch({
      markets,
      providerCode,
      canonicalByCode
    });
    providerCategoryRowsUpserted += batchResult.providerCategoryRowsUpserted;
    assignmentsUpserted += batchResult.assignmentsUpserted;
    rowsSkipped += batchResult.rowsSkipped;

    if (target === "all") {
      const nextLastMarketId = markets.length > 0 ? markets[markets.length - 1]!.marketId : lastMarketId;
      const completed = markets.length < maxMarkets;

      await setCheckpoint(providerCode, checkpointJobName, {
        lastMarketId: nextLastMarketId,
        processedCount: (cursor?.processedCount ?? 0) + markets.length,
        completed,
        updatedAt: new Date().toISOString(),
        requestId: options?.requestId ?? null
      });
    }
  } else {
    let lastMarketId = 0;

    while (true) {
      const markets = await listMarketsForClassification({
        providerCode,
        target,
        lastMarketId,
        maxMarkets
      });
      if (markets.length === 0) {
        break;
      }

      const batchResult = await classifyAndUpsertMarketBatch({
        markets,
        providerCode,
        canonicalByCode
      });
      providerCategoryRowsUpserted += batchResult.providerCategoryRowsUpserted;
      assignmentsUpserted += batchResult.assignmentsUpserted;
      rowsSkipped += batchResult.rowsSkipped;
      lastMarketId = markets[markets.length - 1]!.marketId;

      if (markets.length < maxMarkets) {
        break;
      }
    }
  }

  return {
    rowsUpserted: rowsSeeded + providerCategoryRowsUpserted + assignmentsUpserted,
    rowsSkipped
  };
}

export async function refreshMarketCategorySnapshot1h(providerCode: ProviderCode): Promise<JobRunResult> {
  const rowsUpserted = await db.transaction(async (tx) => {
    const currentBucket = sql`date_trunc('hour', now())`;
    const retentionThreshold = sql`date_trunc('hour', now()) - interval '30 days'`;

    await tx.execute(sql`
      delete from agg.market_category_snapshot_1h snapshot
      where snapshot.provider_code = ${providerCode}
        and snapshot.coverage_mode in ('all', 'scope')
        and snapshot.bucket_ts = ${currentBucket}
    `);

    const upsertResult = await tx.execute(sql`
      with coverage_modes as (
        select unnest(array['all', 'scope'])::varchar(16) as coverage_mode
      ),
      upserted as (
        insert into agg.market_category_snapshot_1h (
          provider_code,
          coverage_mode,
          bucket_ts,
          market_id,
          category_code,
          category_label,
          volume24h,
          liquidity,
          status,
          created_at,
          updated_at
        )
        select
          p.code as provider_code,
          cm.coverage_mode,
          ${currentBucket} as bucket_ts,
          m.id as market_id,
          coalesce(cc.code, 'unknown') as category_code,
          coalesce(cc.label, 'Unknown') as category_label,
          coalesce(m.volume_24h, 0)::numeric(24, 6) as volume24h,
          coalesce(m.liquidity, 0)::numeric(24, 6) as liquidity,
          m.status,
          now(),
          now()
        from core.market m
        join core.platform p on p.id = m.platform_id
        cross join coverage_modes cm
        left join core.market_category_assignment mca on mca.market_id = m.id
        left join core.category_dim cc on cc.id = mca.canonical_category_id
        where p.code = ${providerCode}
          and (
            cm.coverage_mode = 'all'
            or exists (
              select 1
              from core.market_scope ms
              where ms.platform_id = p.id
                and ms.market_id = m.id
            )
          )
        on conflict (provider_code, coverage_mode, bucket_ts, market_id)
        do update set
          category_code = excluded.category_code,
          category_label = excluded.category_label,
          volume24h = excluded.volume24h,
          liquidity = excluded.liquidity,
          status = excluded.status,
          updated_at = now()
        returning 1
      )
      select count(*)::int as upserted_count
      from upserted
    `);

    await tx.execute(sql`
      delete from agg.market_category_snapshot_1h snapshot
      where snapshot.provider_code = ${providerCode}
        and snapshot.bucket_ts < ${retentionThreshold}
    `);

    const summary = upsertResult.rows[0] as { upserted_count?: number | string } | undefined;
    return Number(summary?.upserted_count ?? 0);
  });

  return { rowsUpserted, rowsSkipped: 0 };
}

export async function getCategoryQualitySummary(): Promise<
  Array<{
    providerCode: string;
    scopedUnknownThreshold: number;
    isScopedQualityPass: boolean;
    qualityStatus: "ok" | "degraded";
    scopedTotal: number;
    scopedUnknown: number;
    scopedUnknownRate: number;
    globalTotal: number;
    globalUnknown: number;
    globalUnknownRate: number;
    scopedSourceMix: Record<string, number>;
    globalSourceMix: Record<string, number>;
  }>
> {
  const metricsResult = await db.execute(sql`
    with base as (
      select
        p.code as provider_code,
        m.id as market_id,
        case when ms.market_id is null then false else true end as in_scope,
        coalesce(cc.code, 'unknown') as canonical_code,
        coalesce(mca.source, 'fallback.unknown') as source
      from core.market m
      join core.platform p on p.id = m.platform_id
      left join core.market_scope ms on ms.market_id = m.id and ms.platform_id = m.platform_id
      left join core.market_category_assignment mca on mca.market_id = m.id
      left join core.category_dim cc on cc.id = mca.canonical_category_id
    )
    select
      provider_code as "providerCode",
      count(*) filter (where in_scope)::int as "scopedTotal",
      count(*) filter (where in_scope and canonical_code = 'unknown')::int as "scopedUnknown",
      count(*)::int as "globalTotal",
      count(*) filter (where canonical_code = 'unknown')::int as "globalUnknown"
    from base
    group by provider_code
    order by provider_code
  `);

  const sourceResult = await db.execute(sql`
    with base as (
      select
        p.code as provider_code,
        case when ms.market_id is null then false else true end as in_scope,
        coalesce(mca.source, 'fallback.unknown') as source
      from core.market m
      join core.platform p on p.id = m.platform_id
      left join core.market_scope ms on ms.market_id = m.id and ms.platform_id = m.platform_id
      left join core.market_category_assignment mca on mca.market_id = m.id
    )
    select
      provider_code as "providerCode",
      source,
      count(*)::int as count,
      in_scope as "inScope"
    from base
    group by provider_code, source, in_scope
    order by provider_code, source, in_scope
  `);

  const sourceMixByProvider = new Map<
    string,
    {
      scoped: Record<string, number>;
      global: Record<string, number>;
    }
  >();

  for (const rawRow of sourceResult.rows) {
    const row = rawRow as { providerCode: string; source: string; count: number | string; inScope: boolean };
    const providerEntry = sourceMixByProvider.get(row.providerCode) ?? { scoped: {}, global: {} };
    const count = Number(row.count);

    providerEntry.global[row.source] = (providerEntry.global[row.source] ?? 0) + count;
    if (row.inScope) {
      providerEntry.scoped[row.source] = (providerEntry.scoped[row.source] ?? 0) + count;
    }

    sourceMixByProvider.set(row.providerCode, providerEntry);
  }

  return metricsResult.rows.map((rawRow) => {
    const row = rawRow as {
      providerCode: string;
      scopedTotal: number | string;
      scopedUnknown: number | string;
      globalTotal: number | string;
      globalUnknown: number | string;
    };

    const scopedTotal = Number(row.scopedTotal);
    const scopedUnknown = Number(row.scopedUnknown);
    const globalTotal = Number(row.globalTotal);
    const globalUnknown = Number(row.globalUnknown);
    const scopedUnknownRate = scopedTotal > 0 ? scopedUnknown / scopedTotal : 0;
    const globalUnknownRate = globalTotal > 0 ? globalUnknown / globalTotal : 0;
    const scopedUnknownThreshold = row.providerCode === "kalshi" ? 0.15 : 0.3;
    const isScopedQualityPass = scopedUnknownRate <= scopedUnknownThreshold;

    const sourceMix = sourceMixByProvider.get(row.providerCode) ?? { scoped: {}, global: {} };

    return {
      providerCode: row.providerCode,
      scopedUnknownThreshold,
      isScopedQualityPass,
      qualityStatus: isScopedQualityPass ? "ok" : "degraded",
      scopedTotal,
      scopedUnknown,
      scopedUnknownRate,
      globalTotal,
      globalUnknown,
      globalUnknownRate,
      scopedSourceMix: sourceMix.scoped,
      globalSourceMix: sourceMix.global
    };
  });
}
