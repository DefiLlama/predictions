import type { ProviderCode } from "../types/domain.js";
import { KalshiAdapter } from "./kalshi-adapter.js";
import { PolymarketAdapter } from "./polymarket-adapter.js";
import type { ProviderAdapter } from "./types.js";

const polymarketAdapter = new PolymarketAdapter();
const kalshiAdapter = new KalshiAdapter();

const adapterMap: Record<ProviderCode, ProviderAdapter> = {
  polymarket: polymarketAdapter,
  kalshi: kalshiAdapter
};

export function getAdapter(providerCode: ProviderCode): ProviderAdapter {
  return adapterMap[providerCode];
}

export function listAdapters(): ProviderAdapter[] {
  return Object.values(adapterMap);
}
