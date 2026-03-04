import type { TopTradesSummary } from "@/lib/api/types";
import { formatUsd } from "@/lib/utils/format";

function KpiCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
      <p className="text-xs text-[var(--text-tertiary)]">{label}</p>
      <p className="mt-1 text-lg font-semibold font-mono text-[var(--text-primary)]">
        {value}
      </p>
    </div>
  );
}

export function TopTradesKpis({ summary }: { summary: TopTradesSummary }) {
  const total = summary.buyCount + summary.sellCount;
  const buyPct = total > 0 ? ((summary.buyCount / total) * 100).toFixed(0) : "—";

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <KpiCard label="Total Volume" value={formatUsd(summary.totalVolume)} />
      <KpiCard label="Trade Count" value={summary.tradeCount.toLocaleString()} />
      <KpiCard label="Avg Trade Size" value={formatUsd(summary.avgTradeSize)} />
      <KpiCard label="Buy %" value={buyPct === "—" ? "—" : `${buyPct}%`} />
    </div>
  );
}
