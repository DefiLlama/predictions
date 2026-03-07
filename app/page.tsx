import type { Metadata } from "next";
import { Suspense } from "react";
import {
  getDashboardBenchmarks,
  getDashboardMain,
  getDashboardTreemap,
  getTopTrades,
} from "@/lib/api/client";
import { DefiLlamaBenchmarkPanel } from "@/components/defillama-benchmark-panel";
import { KpiCards } from "@/components/kpi-cards";
import { EventCard } from "@/components/event-card";
import { TreemapChartView } from "@/components/treemap-chart";
import { ProviderFilter } from "@/components/provider-filter";
import { EmptyState } from "@/components/empty-state";

export const metadata: Metadata = {
  title: "Dashboard | Prediction Markets",
  description: "Provider benchmarks, category flows, and top events across Polymarket and Kalshi.",
};

export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;

  const [mainRes, treemapRes, topTradesRes, benchmarkRes] = await Promise.all([
    getDashboardMain({
      provider,
      limit: "12",
      includeNested: "0",
    }),
    getDashboardTreemap({ provider, coverage: "all" }),
    getTopTrades({ window: "24h", provider, summaryOnly: "1" }),
    getDashboardBenchmarks({ provider }),
  ]);

  const { kpis, events } = mainRes.data;

  return (
    <div className="space-y-8">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-lg font-bold tracking-tight text-[var(--text-primary)]">
            Dashboard
          </h1>
          <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
            Real-time overview across prediction market protocols
          </p>
        </div>
        <Suspense>
          <ProviderFilter />
        </Suspense>
      </div>

      {/* KPIs */}
      <KpiCards
        kpis={kpis}
        tradeFlow24h={topTradesRes.data.summary}
        providerCode={provider}
      />

      {/* Benchmarks */}
      <DefiLlamaBenchmarkPanel data={benchmarkRes.data} />

      {/* Treemap */}
      <section>
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Category Breakdown
        </h2>
        <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
          <TreemapChartView data={treemapRes.data} />
        </div>
      </section>

      {/* Top Events */}
      <section>
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Top Events
        </h2>
        {events.length === 0 ? (
          <EmptyState message="No events available" />
        ) : (
          <div className="grid gap-3 lg:grid-cols-2">
            {events.map((event) => (
              <EventCard key={event.eventUid} event={event} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
