import { Suspense } from "react";
import { listMarkets } from "@/lib/api/client";
import { MarketCard } from "@/components/market-card";
import { ProviderFilter } from "@/components/provider-filter";
import { Pagination } from "@/components/pagination";
import { EmptyState } from "@/components/empty-state";

const LIMIT = 50;

export default async function MarketsPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;
  const page = Math.max(
    1,
    parseInt(typeof params.page === "string" ? params.page : "1", 10) || 1,
  );
  const offset = (page - 1) * LIMIT;

  const res = await listMarkets({
    provider,
    limit: String(LIMIT),
    offset: String(offset),
  });

  const markets = res.data;
  const hasNext = markets.length >= LIMIT;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-[var(--text-primary)]">
          Markets
        </h1>
        <div className="flex items-center gap-4">
          <Suspense>
            <ProviderFilter />
          </Suspense>
        </div>
      </div>

      {markets.length === 0 ? (
        <EmptyState message="No markets found" />
      ) : (
        <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
          {markets.map((market) => (
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
