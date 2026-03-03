import { Suspense } from "react";
import Link from "next/link";
import { getDashboardMain } from "@/lib/api/client";
import { ProviderFilter } from "@/components/provider-filter";
import { RefreshBar } from "@/components/refresh-bar";
import { EmptyState } from "@/components/empty-state";
import { formatUsd, formatDelta, providerLabel } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";

export default async function EventsPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;
  const page = Math.max(1, parseInt(typeof params.page === "string" ? params.page : "1", 10) || 1);
  const perPage = 20;

  const res = await getDashboardMain(provider);
  const allEvents = res.data.events;

  const start = (page - 1) * perPage;
  const events = allEvents.slice(start, start + perPage);
  const hasNext = start + perPage < allEvents.length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-[var(--text-primary)]">Events</h1>
        <div className="flex items-center gap-4">
          <Suspense>
            <ProviderFilter />
          </Suspense>
          <RefreshBar timestamp={res.timestamp} />
        </div>
      </div>

      {events.length === 0 ? (
        <EmptyState message="No events found" />
      ) : (
        <div className="space-y-2">
          {/* Table header */}
          <div className="grid grid-cols-[1fr_120px_100px_80px_80px] gap-4 px-4 text-xs font-medium text-[var(--text-tertiary)]">
            <span>Event</span>
            <span className="text-right">Volume 24h</span>
            <span className="text-right">Liquidity</span>
            <span className="text-right">Markets</span>
            <span className="text-right">24h Δ</span>
          </div>

          {events.map((event) => {
            const delta = formatDelta(event.maxAbsDelta24h);
            return (
              <Link
                key={event.eventUid}
                href={uidToPath(event.eventUid, "/events")}
                className="grid grid-cols-[1fr_120px_100px_80px_80px] gap-4 rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] px-4 py-3 hover:bg-[var(--bg-card-hover)] transition-colors items-center"
              >
                <div className="min-w-0">
                  <p className="text-sm font-medium text-[var(--text-primary)] truncate">
                    {event.title ?? event.eventRef}
                  </p>
                  <div className="mt-0.5 flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
                    <span>{providerLabel(event.providerCode)}</span>
                    {event.category && <span>{event.category}</span>}
                  </div>
                </div>
                <span className="text-right text-sm font-mono text-[var(--text-primary)]">
                  {formatUsd(event.volume24h)}
                </span>
                <span className="text-right text-sm font-mono text-[var(--text-secondary)]">
                  {formatUsd(event.liquidity)}
                </span>
                <span className="text-right text-sm text-[var(--text-secondary)]">
                  {event.activeMarketCount}/{event.marketCount}
                </span>
                <span className={`text-right text-sm font-mono ${delta.className}`}>
                  {delta.text}
                </span>
              </Link>
            );
          })}
        </div>
      )}

      {/* Pagination */}
      {(page > 1 || hasNext) && (
        <EventsPagination page={page} hasNext={hasNext} />
      )}
    </div>
  );
}

function EventsPagination({ page, hasNext }: { page: number; hasNext: boolean }) {
  return (
    <div className="flex items-center justify-center gap-3 pt-4">
      {page > 1 ? (
        <Link
          href={`/events${page > 2 ? `?page=${page - 1}` : ""}`}
          className="rounded-md bg-[var(--bg-card)] px-4 py-2 text-sm font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)] transition-colors"
        >
          Previous
        </Link>
      ) : (
        <span className="rounded-md bg-[var(--bg-card)] px-4 py-2 text-sm font-medium text-[var(--text-secondary)] opacity-40">
          Previous
        </span>
      )}
      <span className="text-sm text-[var(--text-secondary)]">Page {page}</span>
      {hasNext ? (
        <Link
          href={`/events?page=${page + 1}`}
          className="rounded-md bg-[var(--bg-card)] px-4 py-2 text-sm font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)] transition-colors"
        >
          Next
        </Link>
      ) : (
        <span className="rounded-md bg-[var(--bg-card)] px-4 py-2 text-sm font-medium text-[var(--text-secondary)] opacity-40">
          Next
        </span>
      )}
    </div>
  );
}
