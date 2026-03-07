import { env } from "../config/env";
import type { ProviderCode } from "../types/domain";
import { logger } from "../utils/logger";

const DEFILLAMA_BASE_URL = "https://pro-api.llama.fi";
const HISTORY_POINTS = 90;
const BENCHMARK_PROVIDERS = ["polymarket", "kalshi"] as const satisfies ProviderCode[];

type BenchmarkMetric = "volume" | "openInterest" | "fees" | "tvl";

interface DefiLlamaOverviewEntry {
  slug?: string;
  name?: string;
  category?: string;
  chains?: string[];
  total24h?: number | null;
  total7d?: number | null;
  total30d?: number | null;
  change_1d?: number | null;
  methodologyURL?: string | null;
}

interface DefiLlamaOverviewResponse {
  protocols?: DefiLlamaOverviewEntry[];
}

interface DefiLlamaSummaryResponse extends DefiLlamaOverviewEntry {
  totalAllTime?: number | null;
  totalDataChart?: Array<[number, number]>;
}

interface DefiLlamaProtocolListEntry {
  slug?: string;
  category?: string;
  tvl?: number | null;
}

interface DefiLlamaProtocolResponse {
  tvl?: Array<{ date: number; totalLiquidityUSD: number }>;
}

export interface DashboardBenchmarkProvider {
  providerCode: ProviderCode;
  chainLabel: string | null;
  volume24h: string | null;
  volume7d: string | null;
  volume30d: string | null;
  openInterest24h: string | null;
  openInterest7d: string | null;
  openInterest30d: string | null;
  volumeShare24h: number | null;
  openInterestShare24h: number | null;
  volumeChange1d: number | null;
  openInterestChange1d: number | null;
  fees24h: string | null;
  fees7d: string | null;
  fees30d: string | null;
  feesChange1d: number | null;
  tvl: string | null;
  tvlShare: number | null;
  methodologyUrls: {
    volume: string | null;
    openInterest: string | null;
    fees: string | null;
    tvl: string | null;
  };
}

export interface DashboardBenchmarkHistoryPoint {
  ts: string;
  value: string;
}

export interface DashboardBenchmarkHistorySeries {
  providerCode: ProviderCode;
  metric: BenchmarkMetric;
  label: string;
  points: DashboardBenchmarkHistoryPoint[];
}

export interface DashboardBenchmarksData {
  available: boolean;
  source: string;
  note: string | null;
  providers: DashboardBenchmarkProvider[];
  history: DashboardBenchmarkHistorySeries[];
}

function hasDefiLlamaKey(): boolean {
  return typeof env.DEFILLAMA_PRO_API_KEY === "string" && env.DEFILLAMA_PRO_API_KEY.length > 0;
}

function toDecimalString(value: number | null | undefined): string | null {
  if (value == null || !Number.isFinite(value)) {
    return null;
  }

  return value.toFixed(2);
}

function toPercent(value: number, total: number): number | null {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) {
    return null;
  }

  return Number(((value / total) * 100).toFixed(1));
}

function normalizeHistoryPoint(timestamp: number, value: number): DashboardBenchmarkHistoryPoint {
  return {
    ts: new Date(timestamp * 1000).toISOString(),
    value: value.toFixed(2),
  };
}

function clipDimensionHistory(
  chart: Array<[number, number]> | undefined,
): DashboardBenchmarkHistoryPoint[] {
  if (!Array.isArray(chart)) {
    return [];
  }

  return chart
    .slice(-HISTORY_POINTS)
    .filter((point) => Array.isArray(point) && point.length >= 2)
    .map(([timestamp, value]) => normalizeHistoryPoint(timestamp, value));
}

function clipTvlHistory(
  chart: Array<{ date: number; totalLiquidityUSD: number }> | undefined,
): DashboardBenchmarkHistoryPoint[] {
  if (!Array.isArray(chart)) {
    return [];
  }

  return chart
    .slice(-HISTORY_POINTS)
    .filter((point) => Number.isFinite(point.date) && Number.isFinite(point.totalLiquidityUSD))
    .map((point) => normalizeHistoryPoint(point.date, point.totalLiquidityUSD));
}

async function fetchDefiLlamaJson<T>(path: string): Promise<T> {
  const apiKey = env.DEFILLAMA_PRO_API_KEY;
  if (!apiKey) {
    throw new Error("Missing DEFILLAMA_PRO_API_KEY");
  }

  const response = await fetch(`${DEFILLAMA_BASE_URL}/${apiKey}${path}`, {
    headers: { accept: "application/json" },
    cache: "no-store",
  });

  if (!response.ok) {
    throw new Error(`DefiLlama ${response.status} for ${path}`);
  }

  return response.json() as Promise<T>;
}

async function fetchOptionalDefiLlamaJson<T>(path: string): Promise<T | null> {
  try {
    return await fetchDefiLlamaJson<T>(path);
  } catch (error) {
    logger.warn({ error, path }, "Optional DefiLlama request failed");
    return null;
  }
}

function findBySlug<T extends { slug?: string }>(rows: T[], slug: ProviderCode): T | undefined {
  return rows.find((row) => row.slug === slug);
}

function getOverviewProtocols(response: DefiLlamaOverviewResponse): DefiLlamaOverviewEntry[] {
  return Array.isArray(response.protocols) ? response.protocols : [];
}

function buildProviderRow(
  providerCode: ProviderCode,
  volume: DefiLlamaSummaryResponse | undefined,
  openInterest: DefiLlamaSummaryResponse | undefined,
  fees: DefiLlamaSummaryResponse | null,
  currentTvl: number | null,
  totalPredictionMarketTvl: number,
  totalPredictionMarketVolume24h: number,
  totalPredictionMarketOi24h: number,
): DashboardBenchmarkProvider {
  const volume24h = volume?.total24h ?? null;
  const openInterest24h = openInterest?.total24h ?? null;

  return {
    providerCode,
    chainLabel: volume?.chains?.[0] ?? openInterest?.chains?.[0] ?? null,
    volume24h: toDecimalString(volume24h),
    volume7d: toDecimalString(volume?.total7d ?? null),
    volume30d: toDecimalString(volume?.total30d ?? null),
    openInterest24h: toDecimalString(openInterest24h),
    openInterest7d: toDecimalString(openInterest?.total7d ?? null),
    openInterest30d: toDecimalString(openInterest?.total30d ?? null),
    volumeShare24h: volume24h == null ? null : toPercent(volume24h, totalPredictionMarketVolume24h),
    openInterestShare24h: openInterest24h == null ? null : toPercent(openInterest24h, totalPredictionMarketOi24h),
    volumeChange1d: volume?.change_1d ?? null,
    openInterestChange1d: openInterest?.change_1d ?? null,
    fees24h: toDecimalString(fees?.total24h ?? null),
    fees7d: toDecimalString(fees?.total7d ?? null),
    fees30d: toDecimalString(fees?.total30d ?? null),
    feesChange1d: fees?.change_1d ?? null,
    tvl: toDecimalString(currentTvl),
    tvlShare: currentTvl == null ? null : toPercent(currentTvl, totalPredictionMarketTvl),
    methodologyUrls: {
      volume: volume?.methodologyURL ?? null,
      openInterest: openInterest?.methodologyURL ?? null,
      fees: fees?.methodologyURL ?? null,
      tvl: providerCode === "polymarket"
        ? "https://github.com/DefiLlama/DefiLlama-Adapters/blob/main/projects/polymarket.js"
        : null,
    },
  };
}

export async function getDashboardBenchmarks(
  providerCode?: ProviderCode,
): Promise<DashboardBenchmarksData> {
  if (!hasDefiLlamaKey()) {
    return {
      available: false,
      source: "DefiLlama",
      note: "Set DEFILLAMA_PRO_API_KEY to enable live protocol benchmarks.",
      providers: [],
      history: [],
    };
  }

  try {
    const [
      protocols,
      overviewDexs,
      overviewOpenInterest,
      polymarketProtocol,
      polymarketVolume,
      kalshiVolume,
      polymarketOpenInterest,
      kalshiOpenInterest,
      polymarketFees,
    ] = await Promise.all([
      fetchDefiLlamaJson<DefiLlamaProtocolListEntry[]>("/api/protocols"),
      fetchDefiLlamaJson<DefiLlamaOverviewResponse>("/api/overview/dexs"),
      fetchDefiLlamaJson<DefiLlamaOverviewResponse>("/api/overview/open-interest?excludeTotalDataChart=true"),
      fetchDefiLlamaJson<DefiLlamaProtocolResponse>("/api/protocol/polymarket"),
      fetchDefiLlamaJson<DefiLlamaSummaryResponse>("/api/summary/dexs/polymarket"),
      fetchDefiLlamaJson<DefiLlamaSummaryResponse>("/api/summary/dexs/kalshi"),
      fetchDefiLlamaJson<DefiLlamaSummaryResponse>("/api/summary/open-interest/polymarket"),
      fetchDefiLlamaJson<DefiLlamaSummaryResponse>("/api/summary/open-interest/kalshi"),
      fetchOptionalDefiLlamaJson<DefiLlamaSummaryResponse>("/api/summary/fees/polymarket"),
    ]);

    const dexOverviewProtocols = getOverviewProtocols(overviewDexs);
    const openInterestOverviewProtocols = getOverviewProtocols(overviewOpenInterest);

    const predictionMarketProtocols = protocols.filter(
      (protocol) => protocol.category === "Prediction Market" && Number.isFinite(protocol.tvl),
    );
    const totalPredictionMarketTvl = predictionMarketProtocols.reduce(
      (sum, protocol) => sum + (protocol.tvl ?? 0),
      0,
    );
    const polymarketTvl = findBySlug(predictionMarketProtocols, "polymarket")?.tvl ?? null;

    const predictionMarketDexs = dexOverviewProtocols.filter(
      (protocol) => protocol.category === "Prediction Market" && Number.isFinite(protocol.total24h),
    );
    const predictionMarketOpenInterestRows = openInterestOverviewProtocols.filter(
      (protocol) => protocol.category === "Prediction Market" && Number.isFinite(protocol.total24h),
    );

    const totalPredictionMarketVolume24h = predictionMarketDexs.reduce(
      (sum, protocol) => sum + (protocol.total24h ?? 0),
      0,
    );
    const totalPredictionMarketOi24h = predictionMarketOpenInterestRows.reduce(
      (sum, protocol) => sum + (protocol.total24h ?? 0),
      0,
    );

    const volumeByProvider = {
      polymarket: polymarketVolume,
      kalshi: kalshiVolume,
    } satisfies Record<ProviderCode, DefiLlamaSummaryResponse>;

    const openInterestByProvider = {
      polymarket: polymarketOpenInterest,
      kalshi: kalshiOpenInterest,
    } satisfies Record<ProviderCode, DefiLlamaSummaryResponse>;

    const rows = BENCHMARK_PROVIDERS.map((code) =>
      buildProviderRow(
        code,
        volumeByProvider[code],
        openInterestByProvider[code],
        code === "polymarket" ? polymarketFees : null,
        code === "polymarket" ? polymarketTvl : null,
        totalPredictionMarketTvl,
        totalPredictionMarketVolume24h,
        totalPredictionMarketOi24h,
      ),
    );

    const history = [
      {
        providerCode: "polymarket",
        metric: "volume",
        label: "24h volume",
        points: clipDimensionHistory(polymarketVolume.totalDataChart),
      },
      {
        providerCode: "kalshi",
        metric: "volume",
        label: "24h volume",
        points: clipDimensionHistory(kalshiVolume.totalDataChart),
      },
      {
        providerCode: "polymarket",
        metric: "openInterest",
        label: "Open interest",
        points: clipDimensionHistory(polymarketOpenInterest.totalDataChart),
      },
      {
        providerCode: "kalshi",
        metric: "openInterest",
        label: "Open interest",
        points: clipDimensionHistory(kalshiOpenInterest.totalDataChart),
      },
      {
        providerCode: "polymarket",
        metric: "fees",
        label: "Fees",
        points: clipDimensionHistory(polymarketFees?.totalDataChart),
      },
      {
        providerCode: "polymarket",
        metric: "tvl",
        label: "TVL",
        points: clipTvlHistory(polymarketProtocol.tvl),
      },
    ] satisfies DashboardBenchmarkHistorySeries[];

    const populatedHistory = history.filter((series) => series.points.length > 0);

    const filteredProviders = providerCode
      ? rows.filter((row) => row.providerCode === providerCode)
      : rows;
    const filteredHistory = providerCode
      ? populatedHistory.filter((series) => series.providerCode === providerCode)
      : populatedHistory;

    return {
      available: true,
      source: "DefiLlama",
      note: "Protocol-level benchmarks complement the event and market data from your internal ingestors.",
      providers: filteredProviders,
      history: filteredHistory,
    };
  } catch (error) {
    logger.error(
      {
        error,
        errorMessage: error instanceof Error ? error.message : String(error),
      },
      "Failed to load DefiLlama benchmark data",
    );
    return {
      available: false,
      source: "DefiLlama",
      note: "Live benchmark data is temporarily unavailable.",
      providers: [],
      history: [],
    };
  }
}
