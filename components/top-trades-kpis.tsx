import type { TopTradesSummary } from "@/lib/api/types";
import { formatUsd } from "@/lib/utils/format";

export function TopTradesKpis({ summary }: { summary: TopTradesSummary }) {
  const total = summary.buyCount + summary.sellCount;
  const buyPct = total > 0 ? ((summary.buyCount / total) * 100).toFixed(0) : "\u2014";

  return (
    <div className="grid grid-cols-2 gap-px overflow-hidden rounded-lg border border-[var(--bg-border)] bg-[var(--bg-border)] sm:grid-cols-4">
      <KpiCell label="Total Volume" value={formatUsd(summary.totalVolume)} accent />
      <KpiCell label="Trade Count" value={summary.tradeCount.toLocaleString()} />
      <KpiCell label="Avg Trade Size" value={formatUsd(summary.avgTradeSize)} />
      <KpiCell label="Buy %" value={buyPct === "\u2014" ? "\u2014" : `${buyPct}%`} />
    </div>
  );
}

function KpiCell({ label, value, accent }: { label: string; value: string; accent?: boolean }) {
  return (
    <div className="bg-[var(--bg-card)] p-4">
      <p className="text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">{label}</p>
      <p className={`mt-2 text-2xl font-bold font-mono tabular-nums ${accent ? "text-[var(--color-primary)]" : "text-[var(--text-primary)]"}`}>
        {value}
      </p>
    </div>
  );
}
