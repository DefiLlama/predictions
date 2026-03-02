import { and, desc, eq, inArray, notInArray, sql } from "drizzle-orm";

import { env } from "../config/env.js";
import { db } from "../db/client.js";
import { market, marketScope } from "../db/schema.js";
import { getPlatformOrThrow } from "./platform-service.js";

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

export async function rebuildScopeTopN(providerCode: "polymarket" | "kalshi", topN = env.POLYMARKET_SCOPE_TOP_N): Promise<number> {
  const platformRow = await getPlatformOrThrow(providerCode);
  const primaryRankExpr = buildPrimaryRankExpr(providerCode);
  const secondaryRankExpr = buildSecondaryRankExpr(providerCode);

  const activeCandidates = await db
    .select({ id: market.id })
    .from(market)
    .where(and(eq(market.platformId, platformRow.id), eq(market.status, "active")))
    .orderBy(desc(primaryRankExpr), desc(secondaryRankExpr), desc(market.id))
    .limit(topN);

  const remainingSlots = Math.max(topN - activeCandidates.length, 0);
  const activeIds = activeCandidates.map((row) => row.id);

  const fallbackCandidates =
    remainingSlots > 0
      ? await db
          .select({ id: market.id })
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

  const candidates = [...activeCandidates, ...fallbackCandidates];

  await db.delete(marketScope).where(eq(marketScope.platformId, platformRow.id));

  if (candidates.length > 0) {
    await db.insert(marketScope).values(
      candidates.map((row, index) => ({
        platformId: platformRow.id,
        marketId: row.id,
        rank: index + 1,
        reason: providerCode === "kalshi" ? "top_volume24h_liquidity_with_fallbacks" : "top_volume24h_liquidity",
        computedAt: new Date()
      }))
    );
  }

  return candidates.length;
}
