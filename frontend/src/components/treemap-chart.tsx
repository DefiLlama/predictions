"use client";

import { useMemo } from "react";
import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { TreemapChart as ETreemap } from "echarts/charts";
import { TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { TreemapEntry } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";

echarts.use([ETreemap, TooltipComponent, CanvasRenderer]);

const CATEGORY_COLORS = [
  "#6366f1", "#8b5cf6", "#a855f7", "#d946ef",
  "#ec4899", "#f43f5e", "#f97316", "#eab308",
  "#84cc16", "#22c55e", "#14b8a6", "#06b6d4",
  "#3b82f6", "#2563eb",
];

const PROVIDER_COLORS: Record<string, string> = {
  polymarket: "#3b82f6",
  kalshi: "#22c55e",
};

export function TreemapChartView({ data }: { data: TreemapEntry[] }) {
  const option = useMemo(() => {
    // Group entries by category, then split by provider
    const categoryMap = new Map<
      string,
      { label: string; entries: TreemapEntry[] }
    >();

    for (const d of data) {
      if (parseFloat(d.value) <= 0) continue;
      const existing = categoryMap.get(d.categoryCode);
      if (existing) {
        existing.entries.push(d);
      } else {
        categoryMap.set(d.categoryCode, {
          label: d.categoryLabel,
          entries: [d],
        });
      }
    }

    const children = Array.from(categoryMap.values()).map((cat, i) => {
      const total = cat.entries.reduce(
        (sum, e) => sum + parseFloat(e.value),
        0,
      );
      const totalMkts = cat.entries.reduce(
        (sum, e) => sum + e.marketCount,
        0,
      );

      return {
        name: cat.label,
        value: total,
        itemStyle: { color: CATEGORY_COLORS[i % CATEGORY_COLORS.length] },
        label: {
          show: true,
          color: "#fff",
          fontSize: 12,
          fontWeight: 600 as const,
          formatter: `{b}\n${totalMkts} mkts`,
        },
        children: cat.entries.map((e) => ({
          name: `${cat.label} / ${providerLabel(e.providerCode)}`,
          value: parseFloat(e.value),
          itemStyle: {
            color: PROVIDER_COLORS[e.providerCode] ?? CATEGORY_COLORS[i % CATEGORY_COLORS.length],
          },
          label: {
            show: true,
            color: "#fff",
            fontSize: 10,
            formatter: `${cat.label} / ${providerLabel(e.providerCode)}\n${formatUsd(e.value)}`,
          },
        })),
      };
    });

    return {
      backgroundColor: "transparent",
      tooltip: {
        formatter: (params: { name: string; value: number; treePathInfo?: { name: string }[] }) => {
          const path = params.treePathInfo?.map((p) => p.name).filter(Boolean).join(" / ") ?? params.name;
          return `<b>${path}</b><br/>${formatUsd(params.value)}`;
        },
      },
      series: [
        {
          type: "treemap",
          width: "100%",
          height: "100%",
          roam: false,
          nodeClick: false,
          breadcrumb: { show: false },
          levels: [
            {
              // Category level
              itemStyle: {
                borderColor: "#0d1117",
                borderWidth: 3,
                gapWidth: 3,
              },
              upperLabel: {
                show: true,
                height: 20,
                color: "#fff",
                fontSize: 11,
                fontWeight: 600 as const,
                backgroundColor: "transparent",
              },
            },
            {
              // Provider level
              itemStyle: {
                borderColor: "#0d1117",
                borderWidth: 1,
                gapWidth: 1,
              },
              label: {
                show: true,
                color: "#fff",
                fontSize: 10,
              },
            },
          ],
          data: children,
        },
      ],
    };
  }, [data]);

  if (data.length === 0) {
    return (
      <div className="flex h-[250px] items-center justify-center text-sm text-[var(--text-tertiary)]">
        No treemap data available
      </div>
    );
  }

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height: 350, width: "100%" }}
      notMerge
    />
  );
}
