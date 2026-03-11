import type { DashboardBenchmarkProvider, DashboardKpi, TopTradesSummary } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";

export function KpiCards({
  kpis,
  tradeFlow24h,
  providerCode,
  benchmarkProviders,
}: {
  kpis: DashboardKpi[];
  tradeFlow24h?: TopTradesSummary;
  providerCode?: string;
  benchmarkProviders?: DashboardBenchmarkProvider[];
}) {
  const buySellTotal = (tradeFlow24h?.buyCount ?? 0) + (tradeFlow24h?.sellCount ?? 0);
  const buyPct = buySellTotal > 0 && tradeFlow24h
    ? Math.round((tradeFlow24h.buyCount / buySellTotal) * 100)
    : null;

  const benchmarkVolume = benchmarkProviders?.reduce((sum, p) => {
    const v = p.volume24h ? parseFloat(p.volume24h) : 0;
    return sum + (Number.isFinite(v) ? v : 0);
  }, 0);

  const totalCells = kpis.length + (tradeFlow24h ? 2 : 0);
  const gridCols =
    totalCells >= 4
      ? "sm:grid-cols-2 lg:grid-cols-4"
      : totalCells === 3
        ? "sm:grid-cols-3"
        : "sm:grid-cols-2";

  return (
    <div
      className={`grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-[var(--bg-border)] bg-[var(--bg-border)] ${gridCols}`}
    >
      {kpis.map((kpi) => (
        <div key={kpi.providerCode} className="bg-[var(--bg-card)] p-4">
          <p className="text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
            {providerLabel(kpi.providerCode)}
          </p>
          <p className="mt-2 text-2xl font-bold tabular-nums text-[var(--text-primary)]">
            {kpi.totalMarkets.toLocaleString()}
          </p>
          <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
            markets &middot; {kpi.scopedMarkets.toLocaleString()} scoped &middot; {kpi.totalInstruments.toLocaleString()} instruments
          </p>
        </div>
      ))}

      {tradeFlow24h && (
        <>
          <div className="bg-[var(--bg-card)] p-4">
            <p className="text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
              24h Volume
            </p>
            <p className="mt-2 text-2xl font-bold font-mono tabular-nums text-[var(--color-primary)]">
              {formatUsd(benchmarkVolume != null && benchmarkVolume > 0 ? String(benchmarkVolume) : tradeFlow24h.totalVolume)}
            </p>
            <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
              {providerCode ? providerLabel(providerCode) : "all providers"}
            </p>
          </div>
          <div className="bg-[var(--bg-card)] p-4">
            <p className="text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
              24h Trades
            </p>
            <p className="mt-2 text-2xl font-bold font-mono tabular-nums text-[var(--text-primary)]">
              {tradeFlow24h.tradeCount.toLocaleString()}
            </p>
            {buyPct !== null && (
              <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
                <span className="text-[var(--color-success)]">{buyPct}%</span> buy
                {" / "}
                <span className="text-[var(--color-error)]">{100 - buyPct}%</span> sell
              </p>
            )}
          </div>
        </>
      )}
    </div>
  );
}
