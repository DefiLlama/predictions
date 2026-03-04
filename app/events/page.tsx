import { Suspense } from "react";
import Link from "next/link";
import { getDashboardMain } from "@/lib/api/client";
import { ProviderFilter } from "@/components/provider-filter";
import { MarketStatusFilter } from "@/components/market-status-filter";
import { EmptyState } from "@/components/empty-state";
import { effectiveStatus, formatUsd, formatDelta, providerLabel, statusBadgeClass } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";

export default async function EventsPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const params = await searchParams;
  const provider = typeof params.provider === "string" ? params.provider : undefined;
  const status = params.status === "all" ? "all" : "active";
  const page = Math.max(1, parseInt(typeof params.page === "string" ? params.page : "1", 10) || 1);
  const perPage = 20;

  const res = await getDashboardMain(provider);
  const allEvents = res.data.events;
  const visibleEvents =
    status === "active"
      ? allEvents.filter((event) => effectiveStatus(event.status, event.endTime ?? event.latestMarketCloseTime) === "active")
      : allEvents;

  const start = (page - 1) * perPage;
  const events = visibleEvents.slice(start, start + perPage);
  const hasNext = start + perPage < visibleEvents.length;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-[var(--text-primary)]">Events</h1>
        <div className="flex items-center gap-4">
          <Suspense>
            <MarketStatusFilter />
          </Suspense>
          <Suspense>
            <ProviderFilter />
          </Suspense>
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
            const displayStatus = effectiveStatus(event.status, event.endTime ?? event.latestMarketCloseTime);
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
                    {displayStatus && (
                      <span className={`rounded px-1.5 py-0.5 ${statusBadgeClass(displayStatus)}`}>
                        {displayStatus}
                      </span>
                    )}
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
        <EventsPagination page={page} hasNext={hasNext} provider={provider} status={status} />
      )}
    </div>
  );
}

function EventsPagination({
  page,
  hasNext,
  provider,
  status,
}: {
  page: number;
  hasNext: boolean;
  provider?: string;
  status: "active" | "all";
}) {
  const hrefForPage = (nextPage: number): string => {
    const query = new URLSearchParams();
    if (provider) {
      query.set("provider", provider);
    }
    if (status === "all") {
      query.set("status", "all");
    } else {
      query.set("status", "active");
    }
    if (nextPage > 1) {
      query.set("page", String(nextPage));
    }

    const queryString = query.toString();
    return queryString.length > 0 ? `/events?${queryString}` : "/events";
  };

  return (
    <div className="flex items-center justify-center gap-3 pt-4">
      {page > 1 ? (
        <Link
          href={hrefForPage(page - 1)}
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
          href={hrefForPage(page + 1)}
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
