import type { DashboardBenchmarksData, DashboardBenchmarkProvider } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";
import { BenchmarkHistoryChart } from "@/components/benchmark-history-chart";
import { EmptyState } from "@/components/empty-state";

function formatPercentValue(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return "\u2014";
  return `${value.toFixed(1)}%`;
}

function formatDailyChange(value: number | null | undefined): {
  text: string;
  className: string;
} {
  if (value == null || !Number.isFinite(value)) {
    return { text: "\u2014", className: "text-[var(--text-tertiary)]" };
  }
  if (value > 0) return { text: `+${value.toFixed(1)}%`, className: "text-[var(--color-success)]" };
  if (value < 0) return { text: `${value.toFixed(1)}%`, className: "text-[var(--color-error)]" };
  return { text: `${value.toFixed(1)}%`, className: "text-[var(--text-tertiary)]" };
}

function Metric({
  label,
  value,
  change,
  primary,
}: {
  label: string;
  value: string;
  change?: { text: string; className: string };
  primary?: boolean;
}) {
  return (
    <div>
      <p className="text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">{label}</p>
      <p
        className={`mt-1 font-mono tabular-nums font-semibold ${
          primary ? "text-lg text-[var(--text-primary)]" : "text-sm text-[var(--text-secondary)]"
        }`}
      >
        {value}
      </p>
      {change && (
        <p className={`mt-0.5 text-xs font-mono tabular-nums ${change.className}`}>
          {change.text}
        </p>
      )}
    </div>
  );
}

function ProviderBenchmarkCard({ row }: { row: DashboardBenchmarkProvider }) {
  const volumeChange = formatDailyChange(row.volumeChange1d);
  const oiChange = formatDailyChange(row.openInterestChange1d);
  const feesChange = formatDailyChange(row.feesChange1d);

  return (
    <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h3 className="text-sm font-bold text-[var(--text-primary)]">
            {providerLabel(row.providerCode)}
          </h3>
          <p className="mt-0.5 text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">
            {row.chainLabel ?? "Prediction market protocol"}
          </p>
        </div>
        <div className="flex gap-3 text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">
          <span>
            Vol{" "}
            <span className="text-[var(--text-secondary)] font-mono">
              {formatPercentValue(row.volumeShare24h)}
            </span>
          </span>
          <span>
            OI{" "}
            <span className="text-[var(--text-secondary)] font-mono">
              {formatPercentValue(row.openInterestShare24h)}
            </span>
          </span>
        </div>
      </div>

      {/* Primary metrics */}
      <div className="mt-4 grid grid-cols-2 gap-4 sm:grid-cols-4">
        <Metric label="24h Vol" value={formatUsd(row.volume24h)} change={volumeChange} primary />
        <Metric label="24h OI" value={formatUsd(row.openInterest24h)} change={oiChange} primary />
        <Metric label="30d Vol" value={formatUsd(row.volume30d)} />
        <Metric label="30d OI" value={formatUsd(row.openInterest30d)} />
      </div>

      {/* Footer stats */}
      <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1 border-t border-[var(--bg-border)]/40 pt-3 text-xs text-[var(--text-tertiary)]">
        <span>
          TVL <span className="font-mono text-[var(--text-secondary)]">{formatUsd(row.tvl)}</span>
        </span>
        <span>
          TVL share{" "}
          <span className="font-mono text-[var(--text-secondary)]">
            {formatPercentValue(row.tvlShare)}
          </span>
        </span>
        {row.fees24h && (
          <span>
            Fees{" "}
            <span className="font-mono text-[var(--text-secondary)]">{formatUsd(row.fees24h)}</span>
            {" "}
            <span className={`font-mono ${feesChange.className}`}>{feesChange.text}</span>
          </span>
        )}
        {row.fees30d && (
          <span>
            30d fees{" "}
            <span className="font-mono text-[var(--text-secondary)]">{formatUsd(row.fees30d)}</span>
          </span>
        )}
      </div>
    </div>
  );
}

export function DefiLlamaBenchmarkPanel({
  data,
}: {
  data: DashboardBenchmarksData;
}) {
  if (!data.available || data.providers.length === 0) {
    return <EmptyState message={data.note ?? "DefiLlama benchmarks unavailable"} />;
  }

  const volumeSeries = data.history.filter((series) => series.metric === "volume");
  const openInterestSeries = data.history.filter((series) => series.metric === "openInterest");

  return (
    <section>
      <div className="mb-3 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Protocol Benchmarks
        </h2>
        {data.note && (
          <p className="text-[10px] text-[var(--text-tertiary)]">{data.note}</p>
        )}
      </div>

      <div className="grid gap-3 lg:grid-cols-2">
        {data.providers.map((row) => (
          <ProviderBenchmarkCard key={row.providerCode} row={row} />
        ))}
      </div>

      {(volumeSeries.length > 0 || openInterestSeries.length > 0) && (
        <div className="mt-3 grid gap-3 lg:grid-cols-2">
          <BenchmarkHistoryChart
            title="90D Volume Trend"
            subtitle="Protocol-level daily volume series from DefiLlama."
            series={volumeSeries}
          />
          <BenchmarkHistoryChart
            title="90D Open Interest Trend"
            subtitle="Protocol-level open interest from DefiLlama."
            series={openInterestSeries}
          />
        </div>
      )}
    </section>
  );
}
