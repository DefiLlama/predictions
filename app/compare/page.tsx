import type { Metadata } from "next";
import { getCachedProviderComparison } from "@/lib/api/server/dashboard-data";
import { ComparisonBarChart } from "@/components/comparison-bar-chart";
import { ComparisonTable } from "@/components/comparison-table";
import { ComparisonTraderTable } from "@/components/comparison-trader-table";
import { formatUsd } from "@/lib/utils/format";

export const dynamic = "force-dynamic";

export const metadata: Metadata = {
  title: "Compare | Prediction Markets",
  description:
    "Side-by-side Polymarket vs Kalshi comparison across categories, volume, liquidity, OI, and trader activity.",
};

const PROVIDER_COLORS: Record<string, string> = {
  polymarket: "#5a8ed4",
  kalshi: "#5bba6f",
};

export default async function ComparePage() {
  const { categories, traders } = await getCachedProviderComparison();

  const polyCategories = categories.filter((r) => r.providerCode === "polymarket");
  const kalshiCategories = categories.filter((r) => r.providerCode === "kalshi");

  const polyVolume = polyCategories.reduce((s, r) => s + (parseFloat(r.volume24h) || 0), 0);
  const kalshiVolume = kalshiCategories.reduce((s, r) => s + (parseFloat(r.volume24h) || 0), 0);
  const polyLiquidity = polyCategories.reduce((s, r) => s + (parseFloat(r.liquidity) || 0), 0);
  const kalshiLiquidity = kalshiCategories.reduce((s, r) => s + (parseFloat(r.liquidity) || 0), 0);
  const polyOI = polyCategories.reduce((s, r) => s + (parseFloat(r.openInterest) || 0), 0);
  const kalshiOI = kalshiCategories.reduce((s, r) => s + (parseFloat(r.openInterest) || 0), 0);

  const polyTraders = traders.filter((r) => r.providerCode === "polymarket");
  const kalshiTraders = traders.filter((r) => r.providerCode === "kalshi");
  const polyTradeCount = polyTraders.reduce((s, r) => s + r.tradeCount, 0);
  const kalshiTradeCount = kalshiTraders.reduce((s, r) => s + r.tradeCount, 0);
  const polyUniqueTraders = polyTraders.reduce(
    (s, r) => (r.uniqueTraders !== null ? s + r.uniqueTraders : s),
    0,
  );

  const kpis = [
    {
      label: "Polymarket Volume 24h",
      value: formatUsd(polyVolume),
      color: PROVIDER_COLORS.polymarket,
      sub: `${polyCategories.reduce((s, r) => s + r.activeMarketCount, 0).toLocaleString()} active markets`,
    },
    {
      label: "Kalshi Volume 24h",
      value: formatUsd(kalshiVolume),
      color: PROVIDER_COLORS.kalshi,
      sub: `${kalshiCategories.reduce((s, r) => s + r.activeMarketCount, 0).toLocaleString()} active markets`,
    },
    {
      label: "Polymarket Liquidity",
      value: formatUsd(polyLiquidity),
      color: PROVIDER_COLORS.polymarket,
      sub: `OI: ${formatUsd(polyOI)}`,
    },
    {
      label: "Kalshi Liquidity",
      value: formatUsd(kalshiLiquidity),
      color: PROVIDER_COLORS.kalshi,
      sub: `OI: ${formatUsd(kalshiOI)}`,
    },
    {
      label: "7d Trades (Poly / Kalshi)",
      value: `${polyTradeCount.toLocaleString()} / ${kalshiTradeCount.toLocaleString()}`,
      color: undefined,
      sub: `${formatUsd(polyTraders.reduce((s, r) => s + (parseFloat(r.totalNotional) || 0), 0))} / ${formatUsd(kalshiTraders.reduce((s, r) => s + (parseFloat(r.totalNotional) || 0), 0))} notional`,
    },
    {
      label: "Poly Unique Traders (7d)",
      value: polyUniqueTraders.toLocaleString(),
      color: PROVIDER_COLORS.polymarket,
      sub: "Kalshi does not expose trader data",
    },
  ];

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-lg font-bold tracking-tight text-[var(--text-primary)]">
          Provider Comparison
        </h1>
        <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
          Polymarket vs Kalshi — category breakdown, volume, liquidity, OI, and
          trader activity
        </p>
      </div>

      <div className="grid grid-cols-1 gap-px overflow-hidden rounded-lg border border-[var(--bg-border)] bg-[var(--bg-border)] sm:grid-cols-2 lg:grid-cols-3">
        {kpis.map((kpi) => (
          <div key={kpi.label} className="bg-[var(--bg-card)] p-4">
            <p className="text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
              {kpi.label}
            </p>
            <p
              className="mt-2 text-2xl font-bold tabular-nums"
              style={{ color: kpi.color ?? "var(--text-primary)" }}
            >
              {kpi.value}
            </p>
            <p className="mt-0.5 text-xs text-[var(--text-tertiary)]">
              {kpi.sub}
            </p>
          </div>
        ))}
      </div>

      <section>
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Category Comparison
        </h2>
        <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
          <ComparisonBarChart data={categories} />
        </div>
      </section>

      <section>
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Market Metrics by Category
        </h2>
        <ComparisonTable data={categories} />
      </section>

      <section>
        <h2 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[var(--text-tertiary)]">
          Trader Activity by Category
        </h2>
        <ComparisonTraderTable data={traders} />
      </section>
    </div>
  );
}
