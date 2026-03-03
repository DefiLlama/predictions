import type { DashboardKpi } from "@/lib/api/types";
import { providerLabel, relativeTime } from "@/lib/utils/format";

export function KpiCards({ kpis }: { kpis: DashboardKpi[] }) {
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3">
      {kpis.map((kpi) => (
        <div
          key={kpi.providerCode}
          className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4"
        >
          <div className="mb-3 flex items-center justify-between">
            <h3 className="text-sm font-semibold text-[var(--text-primary)]">
              {providerLabel(kpi.providerCode)}
            </h3>
            <span className="text-xs text-[var(--text-tertiary)]">
              {relativeTime(kpi.latestPriceTs)}
            </span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            <Stat label="Scoped" value={kpi.scopedMarkets} />
            <Stat label="Markets" value={kpi.totalMarkets} />
            <Stat label="Instruments" value={kpi.totalInstruments} />
          </div>
        </div>
      ))}
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
