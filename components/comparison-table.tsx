"use client";

import { useMemo, useState } from "react";
import type { ComparisonCategoryRow } from "@/lib/api/types";
import { formatUsd } from "@/lib/utils/format";

type Paired = {
  categoryCode: string;
  categoryLabel: string;
  polyVolume: number;
  kalshiVolume: number;
  polyLiquidity: number;
  kalshiLiquidity: number;
  polyOI: number;
  kalshiOI: number;
  polyMarkets: number;
  kalshiMarkets: number;
  polyActive: number;
  kalshiActive: number;
};

type SortKey = "totalVolume" | "totalLiquidity" | "totalOI" | "totalMarkets";

const PROVIDER_COLORS: Record<string, string> = {
  polymarket: "#5a8ed4",
  kalshi: "#5bba6f",
};

function pairData(rows: ComparisonCategoryRow[]): Paired[] {
  const map = new Map<string, Paired>();

  for (const r of rows) {
    let entry = map.get(r.categoryCode);
    if (!entry) {
      entry = {
        categoryCode: r.categoryCode,
        categoryLabel: r.categoryLabel,
        polyVolume: 0,
        kalshiVolume: 0,
        polyLiquidity: 0,
        kalshiLiquidity: 0,
        polyOI: 0,
        kalshiOI: 0,
        polyMarkets: 0,
        kalshiMarkets: 0,
        polyActive: 0,
        kalshiActive: 0,
      };
      map.set(r.categoryCode, entry);
    }
    const vol = parseFloat(r.volume24h) || 0;
    const liq = parseFloat(r.liquidity) || 0;
    const oi = parseFloat(r.openInterest) || 0;
    if (r.providerCode === "polymarket") {
      entry.polyVolume = vol;
      entry.polyLiquidity = liq;
      entry.polyOI = oi;
      entry.polyMarkets = r.marketCount;
      entry.polyActive = r.activeMarketCount;
    } else {
      entry.kalshiVolume = vol;
      entry.kalshiLiquidity = liq;
      entry.kalshiOI = oi;
      entry.kalshiMarkets = r.marketCount;
      entry.kalshiActive = r.activeMarketCount;
    }
  }

  return Array.from(map.values());
}

function LeaderBadge({ poly, kalshi }: { poly: number; kalshi: number }) {
  if (poly === 0 && kalshi === 0) return <span className="text-[var(--text-disabled)]">—</span>;
  const leader = poly > kalshi ? "polymarket" : poly < kalshi ? "kalshi" : null;
  if (!leader) return <span className="text-[var(--text-tertiary)]">Tied</span>;
  const ratio = Math.min(poly, kalshi) > 0
    ? (Math.max(poly, kalshi) / Math.min(poly, kalshi)).toFixed(1)
    : "∞";
  return (
    <span
      className="inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-semibold"
      style={{ backgroundColor: PROVIDER_COLORS[leader] + "22", color: PROVIDER_COLORS[leader] }}
    >
      {leader === "polymarket" ? "Poly" : "Kalshi"} {ratio}x
    </span>
  );
}

export function ComparisonTable({ data }: { data: ComparisonCategoryRow[] }) {
  const [sortKey, setSortKey] = useState<SortKey>("totalVolume");
  const paired = useMemo(() => pairData(data), [data]);

  const sorted = useMemo(() => {
    const copy = [...paired];
    copy.sort((a, b) => {
      switch (sortKey) {
        case "totalVolume":
          return (b.polyVolume + b.kalshiVolume) - (a.polyVolume + a.kalshiVolume);
        case "totalLiquidity":
          return (b.polyLiquidity + b.kalshiLiquidity) - (a.polyLiquidity + a.kalshiLiquidity);
        case "totalOI":
          return (b.polyOI + b.kalshiOI) - (a.polyOI + a.kalshiOI);
        case "totalMarkets":
          return (b.polyMarkets + b.kalshiMarkets) - (a.polyMarkets + a.kalshiMarkets);
      }
    });
    return copy;
  }, [paired, sortKey]);

  const hdr = (label: string, key: SortKey) => (
    <th
      className={`cursor-pointer px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider transition-colors hover:text-[var(--text-primary)] ${
        sortKey === key ? "text-[var(--color-primary)]" : "text-[var(--text-tertiary)]"
      }`}
      onClick={() => setSortKey(key)}
    >
      {label} {sortKey === key ? "↓" : ""}
    </th>
  );

  return (
    <div className="overflow-x-auto rounded-lg border border-[var(--bg-border)]">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-[var(--bg-border)] bg-[var(--bg-surface)]">
            <th className="px-3 py-2 text-left text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
              Category
            </th>
            <th className="px-3 py-2 text-center text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
              Leader
            </th>
            {hdr("Volume", "totalVolume")}
            <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>Poly Vol</th>
            <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.kalshi }}>Kalshi Vol</th>
            {hdr("Liquidity", "totalLiquidity")}
            <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>Poly Liq</th>
            <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.kalshi }}>Kalshi Liq</th>
            {hdr("OI", "totalOI")}
            {hdr("Markets", "totalMarkets")}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <tr
              key={row.categoryCode}
              className="border-b border-[var(--bg-border)] bg-[var(--bg-card)] transition-colors hover:bg-[var(--bg-card-hover)]"
            >
              <td className="px-3 py-2.5 font-medium text-[var(--text-primary)]">
                {row.categoryLabel}
              </td>
              <td className="px-3 py-2.5 text-center">
                <LeaderBadge poly={row.polyVolume} kalshi={row.kalshiVolume} />
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                {formatUsd(row.polyVolume + row.kalshiVolume)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                {formatUsd(row.polyVolume)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.kalshi }}>
                {formatUsd(row.kalshiVolume)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                {formatUsd(row.polyLiquidity + row.kalshiLiquidity)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                {formatUsd(row.polyLiquidity)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.kalshi }}>
                {formatUsd(row.kalshiLiquidity)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                {formatUsd(row.polyOI + row.kalshiOI)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                {(row.polyMarkets + row.kalshiMarkets).toLocaleString()}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
