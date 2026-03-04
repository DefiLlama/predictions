"use client";

import { useMemo } from "react";
import ReactEChartsCore from "echarts-for-react/lib/core";
import * as echarts from "echarts/core";
import { LineChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
import type { PriceHistoryInstrument } from "@/lib/api/types";

echarts.use([LineChart, GridComponent, TooltipComponent, LegendComponent, CanvasRenderer]);

const COLORS = [
  "#6366f1", // indigo
  "#f43f5e", // rose
  "#10b981", // emerald
  "#f59e0b", // amber
  "#8b5cf6", // violet
  "#06b6d4", // cyan
];

export function PriceChart({
  instruments,
}: {
  instruments: PriceHistoryInstrument[];
}) {
  const option = useMemo(() => {
    const series = instruments.map((inst, i) => ({
      name: inst.outcomeLabel ?? inst.instrumentRef,
      type: "line" as const,
      showSymbol: false,
      sampling: "lttb" as const,
      lineStyle: { width: 2 },
      color: COLORS[i % COLORS.length],
      data: inst.points
        .map((p) => {
          const parsed = Number(p.close ?? p.price);
          if (!Number.isFinite(parsed)) {
            return null;
          }

          return [p.ts, parsed * 100] as [string, number];
        })
        .filter((point): point is [string, number] => point !== null),
    }));

    return {
      backgroundColor: "transparent",
      grid: { top: 40, right: 16, bottom: 40, left: 50 },
      tooltip: {
        trigger: "axis" as const,
        backgroundColor: "rgba(20,20,30,0.95)",
        borderColor: "rgba(255,255,255,0.1)",
        textStyle: { color: "#e5e5e5", fontSize: 12 },
        valueFormatter: (v: number) => `${v.toFixed(1)}%`,
      },
      legend: {
        show: instruments.length > 1,
        type: "scroll" as const,
        top: 4,
        textStyle: { color: "#a1a1aa", fontSize: 11 },
      },
      xAxis: {
        type: "time" as const,
        axisLine: { lineStyle: { color: "rgba(255,255,255,0.1)" } },
        axisLabel: { color: "#71717a", fontSize: 10 },
        splitLine: { show: false },
      },
      yAxis: {
        type: "value" as const,
        min: 0,
        max: 100,
        axisLabel: {
          color: "#71717a",
          fontSize: 10,
          formatter: "{value}%",
        },
        splitLine: { lineStyle: { color: "rgba(255,255,255,0.05)" } },
      },
      series,
    };
  }, [instruments]);

  if (instruments.length === 0 || instruments.every((i) => i.points.length === 0)) {
    return (
      <div className="flex h-[300px] items-center justify-center text-sm text-[var(--text-tertiary)]">
        No price history available
      </div>
    );
  }

  return (
    <ReactEChartsCore
      echarts={echarts}
      option={option}
      style={{ height: 300, width: "100%" }}
      notMerge
    />
  );
}
