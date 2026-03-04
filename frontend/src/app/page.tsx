import { Suspense } from "react";
import { getDashboardMain, getDashboardTreemap } from "@/lib/api/client";
import { KpiCards } from "@/components/kpi-cards";
import { EventCard } from "@/components/event-card";
import { TreemapChartView } from "@/components/treemap-chart";
import { TreemapControls } from "@/components/treemap-controls";
import { ProviderFilter } from "@/components/provider-filter";
import { EmptyState } from "@/components/empty-state";

export default async function DashboardPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;
  const metric = typeof params.metric === "string" ? params.metric : undefined;

  const [mainRes, treemapRes] = await Promise.all([
    getDashboardMain(provider),
    getDashboardTreemap({ provider, metric, coverage: "all" }),
  ]);

  const { kpis, events } = mainRes.data;

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-[var(--text-primary)]">
          Dashboard
        </h1>
        <div className="flex items-center gap-4">
          <Suspense>
            <ProviderFilter />
          </Suspense>
        </div>
      </div>

      {/* KPIs */}
      <KpiCards kpis={kpis} />

      {/* Treemap */}
      <section>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-base font-semibold text-[var(--text-primary)]">
            Category Breakdown
          </h2>
          <Suspense>
            <TreemapControls />
          </Suspense>
        </div>
        <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
          <TreemapChartView data={treemapRes.data} />
        </div>
      </section>

      {/* Top Events */}
      <section>
        <h2 className="mb-3 text-base font-semibold text-[var(--text-primary)]">
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
