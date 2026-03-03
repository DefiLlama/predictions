import { and, desc, eq, inArray, notInArray, sql } from "drizzle-orm";

import { env } from "../config/env.js";
import { db } from "../db/client.js";
import { market, marketScope } from "../db/schema.js";
import { getPlatformOrThrow } from "./platform-service.js";

interface SeedMarketRow {
  id: number;
  eventId: number | null;
}

interface ExpandedScopeMarketRow {
  id: number;
  eventId: number | null;
  primaryRankValue: unknown;
  secondaryRankValue: unknown;
}

function buildPrimaryRankExpr(providerCode: "polymarket" | "kalshi") {
  if (providerCode === "kalshi") {
    return sql`
      coalesce(
        nullif(${market.volume24h}, 0),
        nullif(nullif(${market.rawJson} ->> 'volume_24h_fp', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'volume_24h', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'volume_fp', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'volume', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'open_interest_fp', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'open_interest', '')::numeric, 0),
        0
      )
    `;
  }

  return sql`coalesce(${market.volume24h}, 0)`;
}

function buildSecondaryRankExpr(providerCode: "polymarket" | "kalshi") {
  if (providerCode === "kalshi") {
    return sql`
      coalesce(
        nullif(${market.liquidity}, 0),
        nullif(nullif(${market.rawJson} ->> 'liquidity_dollars', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'liquidity', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'open_interest_fp', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'open_interest', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'volume_fp', '')::numeric, 0),
        nullif(nullif(${market.rawJson} ->> 'volume', '')::numeric, 0),
        0
      )
    `;
  }

  return sql`coalesce(${market.liquidity}, 0)`;
}

function toRankNumber(value: unknown): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }

  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  return 0;
}

export function rankExpandedScopeMarketIds(seedMarkets: SeedMarketRow[], expandedMarkets: ExpandedScopeMarketRow[]): number[] {
  const seedRankByMarketId = new Map<number, number>();
  const seedEventRankByEventId = new Map<number, number>();

  for (let index = 0; index < seedMarkets.length; index += 1) {
    const seed = seedMarkets[index]!;
    const seedRank = index + 1;
    seedRankByMarketId.set(seed.id, seedRank);

    if (seed.eventId === null) {
      continue;
    }

    const currentRank = seedEventRankByEventId.get(seed.eventId);
    if (currentRank === undefined || seedRank < currentRank) {
      seedEventRankByEventId.set(seed.eventId, seedRank);
    }
  }

  const candidateByMarketId = new Map<number, ExpandedScopeMarketRow>();

  for (const row of expandedMarkets) {
    candidateByMarketId.set(row.id, row);
  }

  for (const seed of seedMarkets) {
    if (candidateByMarketId.has(seed.id)) {
      continue;
    }

    candidateByMarketId.set(seed.id, {
      id: seed.id,
      eventId: seed.eventId,
      primaryRankValue: 0,
      secondaryRankValue: 0
    });
  }

  const ranked = Array.from(candidateByMarketId.values()).map((row) => {
    const seedRank = seedRankByMarketId.get(row.id) ?? null;
    return {
      id: row.id,
      eventId: row.eventId,
      isSeed: seedRank !== null,
      seedRank,
      seedEventRank: row.eventId === null ? Number.MAX_SAFE_INTEGER : (seedEventRankByEventId.get(row.eventId) ?? Number.MAX_SAFE_INTEGER),
      primaryRankValue: toRankNumber(row.primaryRankValue),
      secondaryRankValue: toRankNumber(row.secondaryRankValue)
    };
  });

  ranked.sort((left, right) => {
    if (left.seedEventRank !== right.seedEventRank) {
      return left.seedEventRank - right.seedEventRank;
    }

    if (left.eventId === right.eventId) {
      if (left.isSeed !== right.isSeed) {
        return left.isSeed ? -1 : 1;
      }

      if (left.isSeed && right.isSeed && left.seedRank !== right.seedRank) {
        return (left.seedRank ?? Number.MAX_SAFE_INTEGER) - (right.seedRank ?? Number.MAX_SAFE_INTEGER);
      }
    }

    if (left.primaryRankValue !== right.primaryRankValue) {
      return right.primaryRankValue - left.primaryRankValue;
    }

    if (left.secondaryRankValue !== right.secondaryRankValue) {
      return right.secondaryRankValue - left.secondaryRankValue;
    }

    return right.id - left.id;
  });

  return ranked.map((row) => row.id);
}

export async function rebuildScopeTopN(providerCode: "polymarket" | "kalshi", topN = env.POLYMARKET_SCOPE_TOP_N): Promise<number> {
  const platformRow = await getPlatformOrThrow(providerCode);
  const primaryRankExpr = buildPrimaryRankExpr(providerCode);
  const secondaryRankExpr = buildSecondaryRankExpr(providerCode);

  const activeCandidates = await db
    .select({
      id: market.id,
      eventId: market.eventId
    })
    .from(market)
    .where(and(eq(market.platformId, platformRow.id), eq(market.status, "active")))
    .orderBy(desc(primaryRankExpr), desc(secondaryRankExpr), desc(market.id))
    .limit(topN);

  const remainingSlots = Math.max(topN - activeCandidates.length, 0);
  const activeIds = activeCandidates.map((row) => row.id);

  const fallbackCandidates =
    remainingSlots > 0
      ? await db
          .select({
            id: market.id,
            eventId: market.eventId
          })
          .from(market)
          .where(
            and(
              eq(market.platformId, platformRow.id),
              inArray(market.status, ["closed", "unknown"]),
              activeIds.length > 0 ? notInArray(market.id, activeIds) : sql`true`
            )
          )
          .orderBy(desc(primaryRankExpr), desc(secondaryRankExpr), desc(market.id))
          .limit(remainingSlots)
      : [];

  const seedCandidates = [...activeCandidates, ...fallbackCandidates];
  const seedEventIds = Array.from(new Set(seedCandidates.map((row) => row.eventId).filter((value): value is number => value !== null)));

  const expandedEventCandidates =
    seedEventIds.length > 0
      ? await db
          .select({
            id: market.id,
            eventId: market.eventId,
            primaryRankValue: primaryRankExpr,
            secondaryRankValue: secondaryRankExpr
          })
          .from(market)
          .where(and(eq(market.platformId, platformRow.id), inArray(market.status, ["active", "closed", "unknown"]), inArray(market.eventId, seedEventIds)))
      : [];

  const rankedCandidateIds = rankExpandedScopeMarketIds(seedCandidates, expandedEventCandidates);

  await db.delete(marketScope).where(eq(marketScope.platformId, platformRow.id));

  if (rankedCandidateIds.length > 0) {
    await db.insert(marketScope).values(
      rankedCandidateIds.map((marketId, index) => ({
        platformId: platformRow.id,
        marketId,
        rank: index + 1,
        reason: "top_seed_event_expanded",
        computedAt: new Date()
      }))
    );
  }

  return rankedCandidateIds.length;
}
