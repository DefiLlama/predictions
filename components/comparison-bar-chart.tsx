"use client";

import { useMemo, useState } from "react";
import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { BarChart } from "echarts/charts";
import {
  TooltipComponent,
  GridComponent,
  LegendComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { ComparisonCategoryRow } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";

echarts.use([
  BarChart,
  TooltipComponent,
  GridComponent,
  LegendComponent,
  CanvasRenderer,
]);

const PROVIDER_COLORS: Record<string, string> = {
  polymarket: "#5a8ed4",
  kalshi: "#5bba6f",
};

type Metric = "volume24h" | "liquidity" | "openInterest" | "marketCount";

const METRIC_OPTIONS: { value: Metric; label: string }[] = [
  { value: "volume24h", label: "Volume 24h" },
  { value: "liquidity", label: "Liquidity" },
  { value: "openInterest", label: "Open Interest" },
  { value: "marketCount", label: "Market Count" },
];

function formatValue(metric: Metric, value: number): string {
  if (metric === "marketCount") return value.toLocaleString();
  return formatUsd(value);
}

export function ComparisonBarChart({
  data,
}: {
  data: ComparisonCategoryRow[];
}) {
  const [metric, setMetric] = useState<Metric>("volume24h");

  const option = useMemo(() => {
    const categoryMap = new Map<
      string,
      { label: string; polymarket: number; kalshi: number }
    >();

    for (const row of data) {
      const val =
        metric === "marketCount"
          ? row.marketCount
          : parseFloat(row[metric]) || 0;
      const existing = categoryMap.get(row.categoryCode);
      if (existing) {
        if (row.providerCode === "polymarket") existing.polymarket = val;
        else existing.kalshi = val;
      } else {
        categoryMap.set(row.categoryCode, {
          label: row.categoryLabel,
          polymarket: row.providerCode === "polymarket" ? val : 0,
          kalshi: row.providerCode === "kalshi" ? val : 0,
        });
      }
    }

    const sorted = Array.from(categoryMap.values())
      .sort((a, b) => b.polymarket + b.kalshi - (a.polymarket + a.kalshi))
      .filter((c) => c.polymarket + c.kalshi > 0);

    const categories = sorted.map((c) => c.label);
    const polyData = sorted.map((c) => c.polymarket);
    const kalshiData = sorted.map((c) => c.kalshi);

    return {
      backgroundColor: "transparent",
      tooltip: {
        trigger: "axis" as const,
        axisPointer: { type: "shadow" as const },
        formatter: (params: Array<{ seriesName: string; value: number; marker: string; axisValueLabel: string }>) => {
          if (!params.length) return "";
          let html = `<b>${params[0].axisValueLabel}</b>`;
          for (const p of params) {
            html += `<br/>${p.marker} ${p.seriesName}: <b>${formatValue(metric, p.value)}</b>`;
          }
          const total = params.reduce((s, p) => s + p.value, 0);
          html += `<br/><span style="color:#aaa">Total: ${formatValue(metric, total)}</span>`;
          return html;
        },
      },
      legend: {
        data: ["Polymarket", "Kalshi"],
        textStyle: { color: "#a09888" },
        top: 0,
      },
      grid: {
        left: 10,
        right: 20,
        bottom: 10,
        top: 40,
        containLabel: true,
      },
      xAxis: {
        type: "value" as const,
        axisLabel: {
          color: "#a09888",
          formatter: (v: number) => formatValue(metric, v),
        },
        splitLine: { lineStyle: { color: "#2a2520", type: "dashed" as const } },
      },
      yAxis: {
        type: "category" as const,
        data: categories.slice().reverse(),
        axisLabel: { color: "#d4c8b8", fontSize: 12 },
        axisTick: { show: false },
        axisLine: { show: false },
      },
      series: [
        {
          name: "Polymarket",
          type: "bar" as const,
          stack: undefined,
          data: polyData.slice().reverse(),
          itemStyle: { color: PROVIDER_COLORS.polymarket, borderRadius: [0, 3, 3, 0] },
          barGap: "20%",
          barMaxWidth: 24,
        },
        {
          name: "Kalshi",
          type: "bar" as const,
          stack: undefined,
          data: kalshiData.slice().reverse(),
          itemStyle: { color: PROVIDER_COLORS.kalshi, borderRadius: [0, 3, 3, 0] },
          barMaxWidth: 24,
        },
      ],
    };
  }, [data, metric]);

  if (data.length === 0) {
    return (
      <div className="flex h-[300px] items-center justify-center text-sm text-[var(--text-tertiary)]">
        No comparison data available
      </div>
    );
  }

  return (
    <div>
      <div className="mb-3 flex items-center gap-1.5">
        {METRIC_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => setMetric(opt.value)}
            className={`rounded-md px-2.5 py-1 text-xs font-medium transition-colors ${
              metric === opt.value
                ? "bg-[var(--color-primary)] text-[var(--bg-app)]"
                : "text-[var(--text-tertiary)] hover:text-[var(--text-primary)] bg-[var(--bg-surface)]"
            }`}
          >
            {opt.label}
          </button>
        ))}
      </div>
      <ReactEChartsCore
        echarts={echarts}
        option={option}
        style={{ height: 420, width: "100%" }}
        notMerge
      />
    </div>
  );
}
