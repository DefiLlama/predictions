import Link from "next/link";
import type { DashboardEvent } from "@/lib/api/types";
import { effectiveStatus, formatUsd, formatDelta, providerLabel, statusBadgeClass } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";
import { InstrumentTable } from "./instrument-table";

export function EventCard({ event }: { event: DashboardEvent }) {
  const delta = formatDelta(event.maxAbsDelta24h);
  const href = uidToPath(event.eventUid, "/events");
  const displayStatus = effectiveStatus(event.status, event.endTime ?? event.latestMarketCloseTime);
  const isActive = displayStatus === "active";

  return (
    <div
      className={`rounded-lg border bg-[var(--bg-card)] p-4 transition-colors hover:bg-[var(--bg-card-hover)] ${
        isActive ? "border-[var(--color-primary)]/20" : "border-[var(--bg-border)]"
      }`}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <Link
            href={href}
            className="text-sm font-semibold text-[var(--text-primary)] hover:text-[var(--color-primary)] transition-colors line-clamp-2"
          >
            {event.title ?? event.eventUid}
          </Link>
          <div className="mt-1.5 flex items-center gap-1.5 text-xs">
            <span className="text-[var(--text-tertiary)]">
              {providerLabel(event.providerCode)}
            </span>
            {event.category && (
              <>
                <span className="text-[var(--bg-muted)]">&middot;</span>
                <span className="text-[var(--text-tertiary)]">{event.category}</span>
              </>
            )}
            {displayStatus && (
              <>
                <span className="text-[var(--bg-muted)]">&middot;</span>
                <span className={statusBadgeClass(displayStatus)}>{displayStatus}</span>
              </>
            )}
            <span className="text-[var(--bg-muted)]">&middot;</span>
            <span className="text-[var(--text-tertiary)]">
              {event.activeMarketCount}/{event.marketCount} mkts
            </span>
          </div>
        </div>
        <div className="flex flex-col items-end gap-0.5 shrink-0 text-right">
          <span className="text-sm font-semibold font-mono tabular-nums text-[var(--text-primary)]">
            {formatUsd(event.volume24h)}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">24h vol</span>
          <span className={`text-xs font-mono tabular-nums ${delta.className}`}>
            {delta.text}
          </span>
        </div>
      </div>

      {event.markets.length > 0 && (
        <div className="mt-3 border-t border-[var(--bg-border)]/40 pt-3 space-y-2">
          {event.markets.slice(0, 2).map((mkt) => (
            <div key={mkt.marketUid}>
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
              className="block text-xs font-medium text-[var(--color-primary)] hover:underline"
            >
              +{event.markets.length - 2} more &rarr;
            </Link>
          )}
        </div>
      )}
    </div>
  );
}
