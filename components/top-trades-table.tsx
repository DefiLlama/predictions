import Link from "next/link";
import type { TopTrade } from "@/lib/api/types";
import { formatPct, formatTs, formatUsd, providerLabel } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";
import { EmptyState } from "@/components/empty-state";

function formatQty(value: string | null): string {
  if (value === null) return "\u2014";
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return "\u2014";
  return parsed.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

function renderSide(side: string | null): { text: string; className: string } {
  if (!side) return { text: "\u2014", className: "text-[var(--text-tertiary)]" };
  const normalized = side.trim().toLowerCase();
  if (normalized === "buy" || normalized === "yes") {
    return { text: "Buy", className: "text-[var(--color-success)]" };
  }
  if (normalized === "sell" || normalized === "no") {
    return { text: "Sell", className: "text-[var(--color-error)]" };
  }
  return { text: side, className: "text-[var(--text-secondary)]" };
}

function truncateAddress(ref: string | null): string {
  if (!ref) return "\u2014";
  if (ref.length <= 12) return ref;
  return `${ref.slice(0, 6)}\u2026${ref.slice(-4)}`;
}

export function TopTradesTable({
  trades,
  offset = 0,
}: {
  trades: TopTrade[];
  offset?: number;
}) {
  if (trades.length === 0) {
    return <EmptyState message="No trades found for this time window" />;
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-[var(--bg-border)]">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b border-[var(--bg-border)] bg-[var(--bg-surface)] text-left text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">
            <th className="px-3 py-2.5 font-medium">#</th>
            <th className="px-3 py-2.5 font-medium">Time</th>
            <th className="px-3 py-2.5 font-medium">Provider</th>
            <th className="px-3 py-2.5 font-medium">Event</th>
            <th className="px-3 py-2.5 font-medium">Market</th>
            <th className="px-3 py-2.5 font-medium">Outcome</th>
            <th className="px-3 py-2.5 font-medium text-right">Side</th>
            <th className="px-3 py-2.5 font-medium text-right">Price</th>
            <th className="px-3 py-2.5 font-medium text-right">Qty</th>
            <th className="px-3 py-2.5 font-medium text-right">Notional</th>
            <th className="px-3 py-2.5 font-medium">Trader</th>
          </tr>
        </thead>
        <tbody>
          {trades.map((trade, idx) => {
            const side = renderSide(trade.side);
            return (
              <tr
                key={trade.tradeRef}
                className="border-b border-[var(--bg-border)]/30 last:border-0 transition-colors hover:bg-[var(--bg-card-hover)]"
              >
                <td className="px-3 py-2 font-mono tabular-nums text-[var(--text-tertiary)]">
                  {offset + idx + 1}
                </td>
                <td className="whitespace-nowrap px-3 py-2 text-[var(--text-secondary)]">
                  {formatTs(trade.ts)}
                </td>
                <td className="px-3 py-2 text-[var(--text-secondary)]">
                  {providerLabel(trade.providerCode)}
                </td>
                <td className="max-w-[180px] truncate px-3 py-2">
                  {trade.eventUid ? (
                    <Link
                      href={uidToPath(trade.eventUid, "/events")}
                      className="text-[var(--color-primary)] hover:underline"
                    >
                      {trade.eventTitle ?? "\u2014"}
                    </Link>
                  ) : (
                    <span className="text-[var(--text-primary)]">{trade.eventTitle ?? "\u2014"}</span>
                  )}
                </td>
                <td className="max-w-[180px] truncate px-3 py-2">
                  <Link
                    href={uidToPath(trade.marketUid, "/markets")}
                    className="text-[var(--color-primary)] hover:underline"
                  >
                    {trade.marketTitle ?? trade.marketRef}
                  </Link>
                </td>
                <td className="px-3 py-2 text-[var(--text-secondary)]">
                  {trade.outcomeLabel ?? trade.instrumentRef ?? "\u2014"}
                </td>
                <td className={`px-3 py-2 text-right font-medium ${side.className}`}>
                  {side.text}
                </td>
                <td className="px-3 py-2 text-right font-mono tabular-nums text-[var(--text-primary)]">
                  {formatPct(trade.price)}
                </td>
                <td className="px-3 py-2 text-right font-mono tabular-nums text-[var(--text-secondary)]">
                  {formatQty(trade.qty)}
                </td>
                <td className="px-3 py-2 text-right font-mono tabular-nums font-semibold text-[var(--text-primary)]">
                  {formatUsd(trade.notionalUsd)}
                </td>
                <td className="px-3 py-2 font-mono text-[var(--text-tertiary)]">
                  {truncateAddress(trade.traderRef)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
