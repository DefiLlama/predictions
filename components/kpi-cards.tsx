import type { DashboardKpi, TopTradesSummary } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";

export function KpiCards({
  kpis,
  tradeFlow24h,
  providerCode,
}: {
  kpis: DashboardKpi[];
  tradeFlow24h?: TopTradesSummary;
  providerCode?: string;
}) {
  const totalCards = kpis.length + (tradeFlow24h ? 1 : 0);
  const desktopGridCols = totalCards >= 3 ? "lg:grid-cols-3" : "lg:grid-cols-2";
  const buySellTotal = (tradeFlow24h?.buyCount ?? 0) + (tradeFlow24h?.sellCount ?? 0);
  const buyPct = buySellTotal > 0 && tradeFlow24h
    ? `${Math.round((tradeFlow24h.buyCount / buySellTotal) * 100)}%`
    : "—";

  return (
    <div className={`grid grid-cols-1 gap-3 sm:grid-cols-2 ${desktopGridCols}`}>
      {kpis.map((kpi) => (
        <div
          key={kpi.providerCode}
          className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4"
        >
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-[var(--text-primary)]">
              {providerLabel(kpi.providerCode)}
            </h3>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <Stat label="Scoped" value={kpi.scopedMarkets} />
            <Stat label="Markets" value={kpi.totalMarkets} />
            <Stat label="Instruments" value={kpi.totalInstruments} />
          </div>
        </div>
      ))}
      {tradeFlow24h ? (
        <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-[var(--text-primary)]">
              24h Trade Flow
            </h3>
            <span className="text-xs text-[var(--text-tertiary)]">
              {providerCode ? providerLabel(providerCode) : "All providers"}
            </span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <TradeStat label="Volume" value={formatUsd(tradeFlow24h.totalVolume)} />
            <TradeStat
              label="Trades"
              value={tradeFlow24h.tradeCount.toLocaleString()}
            />
            <TradeStat label="Buy %" value={buyPct} />
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Stat({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <p className="text-xs text-[var(--text-tertiary)]">{label}</p>
      <p className="text-lg font-semibold text-[var(--text-primary)]">
        {value.toLocaleString()}
      </p>
    </div>
  );
}

function TradeStat({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs text-[var(--text-tertiary)]">{label}</p>
      <p className="text-lg font-semibold font-mono text-[var(--text-primary)]">
        {value}
      </p>
    </div>
  );
}
