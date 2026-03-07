"use client";

import { useMemo } from "react";
import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { LineChart } from "echarts/charts";
import { GridComponent, LegendComponent, TooltipComponent } from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { DashboardBenchmarkHistorySeries } from "@/lib/api/types";
import { formatUsd, providerLabel } from "@/lib/utils/format";

echarts.use([LineChart, GridComponent, LegendComponent, TooltipComponent, CanvasRenderer]);

const SERIES_COLORS: Record<string, string> = {
  polymarket: "#5a8ed4",
  kalshi: "#5bba6f",
};

export function BenchmarkHistoryChart({
  title,
  subtitle,
  series,
}: {
  title: string;
  subtitle: string;
  series: DashboardBenchmarkHistorySeries[];
}) {
  const option = useMemo(() => {
    return {
      backgroundColor: "transparent",
      color: series.map((entry) => SERIES_COLORS[entry.providerCode] ?? "#8a8478"),
      grid: {
        top: 44,
        right: 16,
        bottom: 28,
        left: 16,
        containLabel: true,
      },
      legend: {
        top: 8,
        right: 8,
        textStyle: {
          color: "rgba(200, 190, 170, 0.72)",
          fontSize: 11,
        },
        itemWidth: 10,
        itemHeight: 10,
      },
      tooltip: {
        trigger: "axis",
        backgroundColor: "rgba(26, 23, 19, 0.96)",
        borderColor: "rgba(200, 190, 170, 0.16)",
        textStyle: {
          color: "#e8e0d4",
        },
        valueFormatter: (value: number) => formatUsd(value),
      },
      xAxis: {
        type: "time",
        axisLine: {
          lineStyle: { color: "rgba(200, 190, 170, 0.18)" },
        },
        axisLabel: {
          color: "rgba(200, 190, 170, 0.52)",
          fontSize: 11,
          formatter: (value: number) =>
            new Date(value).toLocaleDateString("en-US", {
              month: "short",
              day: "numeric",
            }),
        },
        splitLine: {
          show: false,
        },
      },
      yAxis: {
        type: "value",
        axisLine: {
          show: false,
        },
        axisTick: {
          show: false,
        },
        axisLabel: {
          color: "rgba(200, 190, 170, 0.52)",
          fontSize: 11,
          formatter: (value: number) => formatUsd(value),
        },
        splitLine: {
          lineStyle: { color: "rgba(200, 190, 170, 0.06)" },
        },
      },
      series: series.map((entry) => ({
        name: providerLabel(entry.providerCode),
        type: "line",
        smooth: true,
        symbol: "none",
        lineStyle: {
          width: 2.5,
        },
        areaStyle: {
          opacity: 0.08,
        },
        emphasis: {
          focus: "series",
        },
        data: entry.points.map((point) => [point.ts, Number(point.value)]),
      })),
    };
  }, [series]);

  if (series.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
      <div className="mb-2">
        <h3 className="text-sm font-semibold text-[var(--text-primary)]">{title}</h3>
        <p className="text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">{subtitle}</p>
      </div>
      <ReactEChartsCore
        echarts={echarts}
        option={option}
        style={{ height: 280, width: "100%" }}
        notMerge
      />
    </div>
  );
}
