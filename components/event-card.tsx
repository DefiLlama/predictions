import Link from "next/link";
import type { DashboardEvent } from "@/lib/api/types";
import { formatUsd, formatDelta, providerLabel } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";
import { InstrumentTable } from "./instrument-table";

export function EventCard({ event }: { event: DashboardEvent }) {
  const delta = formatDelta(event.maxAbsDelta24h);
  const href = uidToPath(event.eventUid, "/events");

  return (
    <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
      <div className="mb-3 flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <Link
            href={href}
            className="text-sm font-semibold text-[var(--text-primary)] hover:text-[var(--color-primary)] transition-colors line-clamp-2"
          >
            {event.title ?? event.eventUid}
          </Link>
          <div className="mt-1 flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
            <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
              {providerLabel(event.providerCode)}
            </span>
            {event.category && (
              <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
                {event.category}
              </span>
            )}
            <span>
              {event.activeMarketCount}/{event.marketCount} markets
            </span>
          </div>
        </div>
        <div className="flex flex-col items-end gap-1 shrink-0">
          <span className="text-sm font-medium text-[var(--text-primary)]">
            {formatUsd(event.volume24h)}
          </span>
          <span className="text-xs text-[var(--text-tertiary)]">24h vol</span>
          <span className={`text-xs font-mono ${delta.className}`}>
            {delta.text}
          </span>
        </div>
      </div>

      {/* Show top market instruments inline */}
      {event.markets.length > 0 && (
        <div className="mt-3 border-t border-[var(--bg-border)]/50 pt-3">
          {event.markets.slice(0, 2).map((mkt) => (
            <div key={mkt.marketUid} className="mb-2 last:mb-0">
              <Link
                href={uidToPath(mkt.marketUid, "/markets")}
                className="mb-1 block text-xs font-medium text-[var(--text-secondary)] hover:text-[var(--color-primary)] transition-colors truncate"
              >
                {mkt.title ?? mkt.marketRef}
              </Link>
              <InstrumentTable instruments={mkt.instruments} />
            </div>
          ))}
          {event.markets.length > 2 && (
            <Link
              href={href}
              className="mt-1 block text-xs text-[var(--color-primary)] hover:underline"
            >
              +{event.markets.length - 2} more markets
            </Link>
          )}
        </div>
      )}
    </div>
  );
}
