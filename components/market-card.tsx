import Link from "next/link";
import type { MarketSummary } from "@/lib/api/types";
import { effectiveStatus, formatUsd, providerLabel, statusBadgeClass } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";

export function MarketCard({ market }: { market: MarketSummary }) {
  const href = uidToPath(market.marketUid, "/markets");
  const displayStatus = effectiveStatus(market.status, market.closeTime) ?? market.status;

  return (
    <Link
      href={href}
      className="block rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4 hover:bg-[var(--bg-card-hover)] transition-colors"
    >
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium text-[var(--text-primary)] line-clamp-2">
            {market.title ?? market.marketRef}
          </p>
          <div className="mt-1 flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
            <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
              {providerLabel(market.providerCode)}
            </span>
            <span className={`rounded px-1.5 py-0.5 ${statusBadgeClass(displayStatus)}`}>{displayStatus}</span>
          </div>
        </div>
        <div className="flex flex-col items-end gap-1 shrink-0">
          <span className="text-sm font-medium text-[var(--text-primary)]">
            {formatUsd(market.volume24h)}
          </span>
          <span className="text-xs text-[var(--text-tertiary)]">vol</span>
          <span className="text-xs text-[var(--text-secondary)]">
            {formatUsd(market.liquidity)} liq
          </span>
        </div>
      </div>
    </Link>
  );
}
