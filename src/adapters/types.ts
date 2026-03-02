import type {
  AdapterMarketInput,
  AdapterInstrumentInput,
  NormalizedEvent,
  NormalizedInstrument,
  NormalizedMarket,
  NormalizedOpenInterestPoint,
  NormalizedOrderbookTop,
  NormalizedPricePoint,
  NormalizedTradeEvent,
  PriceWindow,
  ProviderCode
} from "../types/domain.js";

export interface ProviderAdapter {
  readonly providerCode: ProviderCode;
  listEvents(): Promise<NormalizedEvent[]>;
  listMarkets(): Promise<NormalizedMarket[]>;
  listInstruments(markets: NormalizedMarket[]): Promise<NormalizedInstrument[]>;
  listPricePoints(instruments: AdapterInstrumentInput[], window: PriceWindow): Promise<NormalizedPricePoint[]>;
  listOrderbookTop(instruments: AdapterInstrumentInput[]): Promise<NormalizedOrderbookTop[]>;
  listTrades(markets: AdapterMarketInput[], window: PriceWindow): Promise<NormalizedTradeEvent[]>;
  listOpenInterest(markets: AdapterMarketInput[], window: PriceWindow): Promise<NormalizedOpenInterestPoint[]>;
  normalizeMarketRef(raw: string): string;
  normalizeInstrumentRef(raw: string): string;
}
