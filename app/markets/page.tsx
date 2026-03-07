import { Suspense } from "react";
import { listMarkets } from "@/lib/api/client";
import { MarketCard } from "@/components/market-card";
import { ProviderFilter } from "@/components/provider-filter";
import { MarketStatusFilter } from "@/components/market-status-filter";
import { Pagination } from "@/components/pagination";
import { EmptyState } from "@/components/empty-state";
import { effectiveStatus } from "@/lib/utils/format";

const LIMIT = 50;

export default async function MarketsPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;
  const status = params.status === "all" ? "all" : "active";
  const page = Math.max(
    1,
    parseInt(typeof params.page === "string" ? params.page : "1", 10) || 1,
  );
  const offset = (page - 1) * LIMIT;

  const res = await listMarkets({
    provider,
    status,
    limit: String(LIMIT),
    offset: String(offset),
  });

  const markets = res.data;
  const visibleMarkets =
    status === "active"
      ? markets.filter((market) => effectiveStatus(market.status, market.closeTime) === "active")
      : markets;
  const hasNext = markets.length >= LIMIT;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold tracking-tight text-[var(--text-primary)]">
            Markets
          </h1>
          <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
            Individual market contracts across providers
          </p>
        </div>
        <div className="flex items-center gap-3">
          <Suspense>
            <MarketStatusFilter />
          </Suspense>
          <Suspense>
            <ProviderFilter />
          </Suspense>
        </div>
      </div>

      {visibleMarkets.length === 0 ? (
        <EmptyState message="No markets found" />
      ) : (
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {visibleMarkets.map((market) => (
            <MarketCard key={market.marketUid} market={market} />
          ))}
        </div>
      )}

      <Suspense>
        <Pagination page={page} hasNext={hasNext} />
      </Suspense>
    </div>
  );
}
