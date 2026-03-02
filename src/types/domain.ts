export type ProviderCode = "polymarket" | "kalshi";

export type MarketStatus = "active" | "closed" | "archived" | "unknown";

export interface NormalizedEvent {
  eventRef: string;
  title: string | null;
  category: string | null;
  startTime: Date | null;
  endTime: Date | null;
  status: string | null;
  rawJson: Record<string, unknown>;
}

export interface NormalizedMarket {
  marketRef: string;
  eventRef: string | null;
  title: string | null;
  status: MarketStatus;
  closeTime: Date | null;
  volume24h: number | null;
  liquidity: number | null;
  rawJson: Record<string, unknown>;
}

export interface NormalizedInstrument {
  marketRef: string;
  instrumentRef: string;
  outcomeLabel: string | null;
  outcomeIndex: number | null;
  isPrimary: boolean;
  rawJson: Record<string, unknown>;
}

export interface AdapterInstrumentInput {
  marketRef: string;
  instrumentRef: string;
}

export interface AdapterMarketInput {
  marketRef: string;
}

export interface NormalizedPricePoint {
  instrumentRef: string;
  ts: Date;
  price: number;
  source: string;
}

export interface NormalizedOrderbookTop {
  instrumentRef: string;
  ts: Date;
  bestBid: number | null;
  bestAsk: number | null;
  spread: number | null;
  bidDepthTop5: number | null;
  askDepthTop5: number | null;
  rawJson: Record<string, unknown>;
}

export interface PriceWindow {
  startTs: number;
  endTs: number;
}

export interface NormalizedTradeEvent {
  tradeRef: string;
  marketRef: string;
  instrumentRef: string | null;
  ts: Date;
  side: string | null;
  price: number | null;
  qty: number | null;
  notionalUsd: number | null;
  traderRef: string | null;
  rawJson: Record<string, unknown>;
  source: string;
}

export interface NormalizedOpenInterestPoint {
  marketRef: string;
  ts: Date;
  value: number;
  unit: string;
  rawJson: Record<string, unknown>;
  source: string;
}
