import Link from "next/link";
import type { EventTrade } from "@/lib/api/types";
import { formatPct, formatTs, formatUsd } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";
import { EmptyState } from "@/components/empty-state";

function formatQty(value: string | null): string {
  if (value === null) {
    return "—";
  }

  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return "—";
  }

  return parsed.toLocaleString("en-US", {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

function renderSide(side: string | null): { text: string; className: string } {
  if (!side) {
    return { text: "—", className: "text-[var(--text-tertiary)]" };
  }

  const normalized = side.trim().toLowerCase();
  if (normalized === "buy" || normalized === "yes") {
    return { text: "Buy", className: "text-[var(--color-success)]" };
  }
  if (normalized === "sell" || normalized === "no") {
    return { text: "Sell", className: "text-[var(--color-error)]" };
  }

  return { text: side, className: "text-[var(--text-secondary)]" };
}

export function EventTradesTable({
  trades,
  eventUid,
}: {
  trades: EventTrade[];
  eventUid?: string;
}) {
  return (
    trades.length === 0 ? (
      <EmptyState message="No recent trades >= $100 for this event" />
    ) : (
      <div className="overflow-x-auto rounded-lg border border-[var(--bg-border)]">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-[var(--bg-border)] bg-[var(--bg-surface)] text-left text-[var(--text-tertiary)]">
              <th className="px-3 py-2 font-medium">Time</th>
              {eventUid && <th className="px-3 py-2 font-medium">Event</th>}
              <th className="px-3 py-2 font-medium">Market</th>
              <th className="px-3 py-2 font-medium">Outcome</th>
              <th className="px-3 py-2 font-medium text-right">Side</th>
              <th className="px-3 py-2 font-medium text-right">Price</th>
              <th className="px-3 py-2 font-medium text-right">Qty</th>
              <th className="px-3 py-2 font-medium text-right">Notional</th>
            </tr>
          </thead>
          <tbody>
            {trades.map((trade) => {
              const side = renderSide(trade.side);
              return (
                <tr key={trade.tradeRef} className="border-b border-[var(--bg-border)]/50 last:border-0">
                  <td className="px-3 py-2 text-[var(--text-secondary)]">{formatTs(trade.ts)}</td>
                  {eventUid && (
                    <td className="px-3 py-2">
                      <Link
                        href={uidToPath(eventUid, "/events")}
                        className="text-[var(--color-primary)] underline decoration-[var(--color-primary)]/30 hover:decoration-[var(--color-primary)]"
                      >
                        Event
                      </Link>
                    </td>
                  )}
                  <td className="px-3 py-2">
                    <Link
                      href={uidToPath(trade.marketUid, "/markets")}
                      className="text-[var(--color-primary)] underline decoration-[var(--color-primary)]/30 hover:decoration-[var(--color-primary)]"
                    >
                      {trade.marketTitle ?? trade.marketRef}
                    </Link>
                  </td>
                  <td className="px-3 py-2 text-[var(--text-secondary)]">{trade.outcomeLabel ?? trade.instrumentRef ?? "—"}</td>
                  <td className={`px-3 py-2 text-right font-medium ${side.className}`}>{side.text}</td>
                  <td className="px-3 py-2 text-right font-mono text-[var(--text-primary)]">{formatPct(trade.price)}</td>
                  <td className="px-3 py-2 text-right font-mono text-[var(--text-secondary)]">{formatQty(trade.qty)}</td>
                  <td className="px-3 py-2 text-right font-mono text-[var(--text-primary)]">{formatUsd(trade.notionalUsd)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    )
  );
}
