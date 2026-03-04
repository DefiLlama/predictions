import { Suspense } from "react";
import { getTopTrades } from "@/lib/api/client";
import { TopTradesKpis } from "@/components/top-trades-kpis";
import { TopTradesTable } from "@/components/top-trades-table";
import { ProviderFilter } from "@/components/provider-filter";
import { WindowFilter } from "@/components/window-filter";
import { Pagination } from "@/components/pagination";

const LIMIT = 50;

export default async function TopTradesPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider =
    typeof params.provider === "string" ? params.provider : undefined;
  const window =
    typeof params.window === "string" ? params.window : "24h";
  const page = Math.max(
    1,
    parseInt(typeof params.page === "string" ? params.page : "1", 10) || 1,
  );
  const offset = (page - 1) * LIMIT;

  const res = await getTopTrades({
    window,
    provider,
    limit: String(LIMIT),
    offset: String(offset),
  });

  const { summary, trades, pagination } = res.data;
  const hasNext = offset + LIMIT < pagination.total;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-[var(--text-primary)]">
          Top Trades
        </h1>
        <div className="flex items-center gap-4">
          <Suspense>
            <WindowFilter />
          </Suspense>
          <Suspense>
            <ProviderFilter />
          </Suspense>
        </div>
      </div>

      <TopTradesKpis summary={summary} />

      <TopTradesTable trades={trades} offset={offset} />

      <Suspense>
        <Pagination page={page} hasNext={hasNext} />
      </Suspense>
    </div>
  );
}
