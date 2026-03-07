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
      className="group block rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4 transition-all hover:bg-[var(--bg-card-hover)] hover:border-[var(--color-primary)]/20"
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <p className="text-sm font-medium text-[var(--text-primary)] line-clamp-2 group-hover:text-[var(--color-primary)] transition-colors">
            {market.title ?? market.marketRef}
          </p>
          <div className="mt-1.5 flex items-center gap-1.5 text-xs text-[var(--text-tertiary)]">
            <span>{providerLabel(market.providerCode)}</span>
            <span className="text-[var(--bg-muted)]">&middot;</span>
            <span className={statusBadgeClass(displayStatus)}>{displayStatus}</span>
          </div>
        </div>
        <div className="flex flex-col items-end gap-0.5 shrink-0">
          <span className="text-sm font-semibold font-mono tabular-nums text-[var(--text-primary)]">
            {formatUsd(market.volume24h)}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">vol</span>
          <span className="text-xs font-mono tabular-nums text-[var(--text-secondary)]">
            {formatUsd(market.liquidity)} liq
          </span>
        </div>
      </div>
    </Link>
  );
}
