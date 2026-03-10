"use client";

import { useMemo, useState } from "react";
import type { ComparisonTraderRow } from "@/lib/api/types";
import { formatUsd } from "@/lib/utils/format";

type Paired = {
  categoryCode: string;
  categoryLabel: string;
  polyTrades: number;
  kalshiTrades: number;
  polyAvgSize: number;
  kalshiAvgSize: number;
  polyP95: number;
  kalshiP95: number;
  polyWhales: number;
  kalshiWhales: number;
  polyUniqueTraders: number | null;
  polyNotional: number;
  kalshiNotional: number;
};

type SortKey = "trades" | "notional" | "avgSize" | "whales";

const PROVIDER_COLORS: Record<string, string> = {
  polymarket: "#5a8ed4",
  kalshi: "#5bba6f",
};

function pairData(rows: ComparisonTraderRow[]): Paired[] {
  const map = new Map<string, Paired>();

  for (const r of rows) {
    let entry = map.get(r.categoryCode);
    if (!entry) {
      entry = {
        categoryCode: r.categoryCode,
        categoryLabel: r.categoryLabel,
        polyTrades: 0,
        kalshiTrades: 0,
        polyAvgSize: 0,
        kalshiAvgSize: 0,
        polyP95: 0,
        kalshiP95: 0,
        polyWhales: 0,
        kalshiWhales: 0,
        polyUniqueTraders: null,
        polyNotional: 0,
        kalshiNotional: 0,
      };
      map.set(r.categoryCode, entry);
    }
    if (r.providerCode === "polymarket") {
      entry.polyTrades = r.tradeCount;
      entry.polyAvgSize = parseFloat(r.avgTradeSize) || 0;
      entry.polyP95 = parseFloat(r.p95TradeSize) || 0;
      entry.polyWhales = r.whaleTrades;
      entry.polyUniqueTraders = r.uniqueTraders;
      entry.polyNotional = parseFloat(r.totalNotional) || 0;
    } else {
      entry.kalshiTrades = r.tradeCount;
      entry.kalshiAvgSize = parseFloat(r.avgTradeSize) || 0;
      entry.kalshiP95 = parseFloat(r.p95TradeSize) || 0;
      entry.kalshiWhales = r.whaleTrades;
      entry.kalshiNotional = parseFloat(r.totalNotional) || 0;
    }
  }

  return Array.from(map.values());
}

export function ComparisonTraderTable({
  data,
}: {
  data: ComparisonTraderRow[];
}) {
  const [sortKey, setSortKey] = useState<SortKey>("trades");
  const paired = useMemo(() => pairData(data), [data]);

  const sorted = useMemo(() => {
    const copy = [...paired];
    copy.sort((a, b) => {
      switch (sortKey) {
        case "trades":
          return (b.polyTrades + b.kalshiTrades) - (a.polyTrades + a.kalshiTrades);
        case "notional":
          return (b.polyNotional + b.kalshiNotional) - (a.polyNotional + a.kalshiNotional);
        case "avgSize":
          return Math.max(b.polyAvgSize, b.kalshiAvgSize) - Math.max(a.polyAvgSize, a.kalshiAvgSize);
        case "whales":
          return (b.polyWhales + b.kalshiWhales) - (a.polyWhales + a.kalshiWhales);
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

  const polyTotals = paired.reduce(
    (acc, r) => ({
      trades: acc.trades + r.polyTrades,
      whales: acc.whales + r.polyWhales,
      notional: acc.notional + r.polyNotional,
      uniqueTraders: r.polyUniqueTraders !== null ? (acc.uniqueTraders ?? 0) + r.polyUniqueTraders : acc.uniqueTraders,
    }),
    { trades: 0, whales: 0, notional: 0, uniqueTraders: null as number | null },
  );

  const kalshiTotals = paired.reduce(
    (acc, r) => ({
      trades: acc.trades + r.kalshiTrades,
      whales: acc.whales + r.kalshiWhales,
      notional: acc.notional + r.kalshiNotional,
    }),
    { trades: 0, whales: 0, notional: 0 },
  );

  return (
    <div>
      <div className="mb-3 flex items-center gap-4 text-xs text-[var(--text-tertiary)]">
        <span>7-day window</span>
        <span className="text-[var(--text-disabled)]">|</span>
        <span>Whale threshold: $10K+</span>
        <span className="text-[var(--text-disabled)]">|</span>
        <span>
          Total trades:{" "}
          <span style={{ color: PROVIDER_COLORS.polymarket }}>{polyTotals.trades.toLocaleString()} Poly</span>
          {" / "}
          <span style={{ color: PROVIDER_COLORS.kalshi }}>{kalshiTotals.trades.toLocaleString()} Kalshi</span>
        </span>
        {polyTotals.uniqueTraders !== null && (
          <>
            <span className="text-[var(--text-disabled)]">|</span>
            <span style={{ color: PROVIDER_COLORS.polymarket }}>
              {polyTotals.uniqueTraders.toLocaleString()} unique Poly traders
            </span>
          </>
        )}
      </div>
      <div className="overflow-x-auto rounded-lg border border-[var(--bg-border)]">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[var(--bg-border)] bg-[var(--bg-surface)]">
              <th className="px-3 py-2 text-left text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">
                Category
              </th>
              {hdr("Trades", "trades")}
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>Poly</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.kalshi }}>Kalshi</th>
              {hdr("Notional", "notional")}
              {hdr("Avg Size", "avgSize")}
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>Poly Avg</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.kalshi }}>Kalshi Avg</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">Poly P95</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider text-[var(--text-tertiary)]">Kalshi P95</th>
              {hdr("Whales", "whales")}
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>Poly</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.kalshi }}>Kalshi</th>
              <th className="px-3 py-2 text-right text-[10px] font-medium uppercase tracking-wider" style={{ color: PROVIDER_COLORS.polymarket }}>
                Unique Traders
              </th>
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
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                  {(row.polyTrades + row.kalshiTrades).toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                  {row.polyTrades.toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.kalshi }}>
                  {row.kalshiTrades.toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                  {formatUsd(row.polyNotional + row.kalshiNotional)}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                  {formatUsd(Math.max(row.polyAvgSize, row.kalshiAvgSize))}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                  {formatUsd(row.polyAvgSize)}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.kalshi }}>
                  {formatUsd(row.kalshiAvgSize)}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-tertiary)]">
                  {formatUsd(row.polyP95)}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-tertiary)]">
                  {formatUsd(row.kalshiP95)}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums text-[var(--text-secondary)]">
                  {(row.polyWhales + row.kalshiWhales).toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                  {row.polyWhales.toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.kalshi }}>
                  {row.kalshiWhales.toLocaleString()}
                </td>
                <td className="px-3 py-2.5 text-right tabular-nums" style={{ color: PROVIDER_COLORS.polymarket }}>
                  {row.polyUniqueTraders !== null ? row.polyUniqueTraders.toLocaleString() : "N/A"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
