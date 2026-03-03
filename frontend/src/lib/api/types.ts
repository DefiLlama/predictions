/* ── Response envelopes ── */

export interface ApiEnvelope<T> {
  data: T;
  timestamp: string;
}

export interface PaginatedEnvelope<T> {
  data: T;
  pagination: { limit: number; offset: number };
  timestamp: string;
}

/* ── Dashboard ── */

export interface DashboardKpi {
  providerCode: string;
  scopedMarkets: number;
  totalMarkets: number;
  totalInstruments: number;
  latestPriceTs: string | null;
  latestOrderbookTs: string | null;
}

export interface DashboardInstrument {
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  latestPriceTs: string | null;
  latestPrice: string | null;
  previousPrice24h: string | null;
  delta24h: string | null;
  latestOrderbookTs: string | null;
  bestBid: string | null;
  bestAsk: string | null;
  spread: string | null;
  bidDepthTop5: string | null;
  askDepthTop5: string | null;
}

export interface DashboardMarket {
  marketUid: string;
  providerCode: string;
  marketRef: string;
  title: string | null;
  status: string;
  closeTime: string | null;
  volume24h: string | null;
  liquidity: string | null;
  instruments: DashboardInstrument[];
}

export interface DashboardEvent {
  eventUid: string;
  providerCode: string;
  eventRef: string;
  title: string | null;
  category: string | null;
  startTime: string | null;
  endTime: string | null;
  status: string | null;
  marketCount: number;
  activeMarketCount: number;
  volume24h: string;
  liquidity: string;
  latestMarketCloseTime: string | null;
  maxAbsDelta24h: string | null;
  markets: DashboardMarket[];
}

export interface DashboardMainData {
  kpis: DashboardKpi[];
  events: DashboardEvent[];
}

/* ── Treemap ── */

export interface TreemapEntry {
  providerCode: string;
  groupBy: "sector" | "providerCategory";
  sourceKind: string | null;
  categoryCode: string;
  categoryLabel: string;
  bucketTs: string;
  value: string;
  marketCount: number;
  activeMarketCount: number;
  selectedMarketCount: number;
}

/* ── Markets ── */

export interface MarketSummary {
  marketUid: string;
  providerCode: string;
  marketRef: string;
  title: string | null;
  status: string;
  closeTime: string | null;
  volume24h: string | null;
  liquidity: string | null;
}

export interface InstrumentSnapshot {
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  latestPriceTs: string | null;
  latestPrice: string | null;
  latestOrderbookTs: string | null;
  bestBid: string | null;
  bestAsk: string | null;
  spread: string | null;
  bidDepthTop5: string | null;
  askDepthTop5: string | null;
}

export interface MarketDetailData {
  market: MarketSummary & {
    eventRef: string | null;
    eventTitle: string | null;
  };
  instruments: InstrumentSnapshot[];
}

/* ── Events ── */

export interface EventInfo {
  eventUid: string;
  providerCode: string;
  eventRef: string;
  title: string | null;
  category: string | null;
  startTime: string | null;
  endTime: string | null;
  status: string | null;
}

export interface EventMarketDetail {
  marketUid: string;
  providerCode: string;
  marketRef: string;
  title: string | null;
  status: string;
  closeTime: string | null;
  volume24h: string | null;
  liquidity: string | null;
  eventRef: string | null;
  eventTitle: string | null;
  instruments: InstrumentSnapshot[];
}

export interface EventDetailData {
  event: EventInfo;
  markets: EventMarketDetail[];
}

/* ── Price History ── */

export interface OhlcPoint {
  ts: string;
  price: string;
  open: string;
  high: string;
  low: string;
  close: string;
  points: number;
}

export interface PriceHistoryInstrument {
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  points: OhlcPoint[];
}

export interface PriceHistoryData {
  market: {
    marketUid: string;
    providerCode: string;
    marketRef: string;
    title: string | null;
    status: string;
    closeTime: string | null;
  };
  interval: string;
  from: string;
  to: string;
  instruments: PriceHistoryInstrument[];
}

export interface EventPriceSeries {
  marketUid: string;
  marketRef: string;
  marketTitle: string | null;
  instrumentRef: string;
  outcomeLabel: string | null;
  points: OhlcPoint[];
}

export interface EventPriceHistoryData {
  event: EventInfo;
  interval: string;
  from: string;
  to: string;
  series: EventPriceSeries[];
}
