export const JOB_NAMES = {
  CRON_TOPN_LIVE: "cron:topn-live",
  CRON_FULL_CATALOG: "cron:full-catalog",
  CRON_FULL_CATALOG_RESUME: "cron:full-catalog:resume",

  SCOPE_REBUILD: "scope:rebuild",
  MARKET_RELINK_EVENTS: "market:relink:events",
  CATEGORY_ASSIGN_MARKETS: "analytics:category:assign:markets",
  ANALYTICS_ROLLUP_PRICE_1H: "analytics:rollup:price:1h",
  ANALYTICS_ROLLUP_LIQUIDITY_1H: "analytics:rollup:liquidity:1h",
  ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H: "analytics:rollup:provider-category:1h",

  POLYMARKET_SYNC_METADATA: "polymarket:sync:metadata",
  POLYMARKET_SYNC_METADATA_BACKFILL_FULL: "polymarket:sync:metadata:backfill_full",
  POLYMARKET_SYNC_PRICES: "polymarket:sync:prices",
  POLYMARKET_SYNC_PRICES_FULL_CATALOG: "polymarket:sync:prices:full_catalog",
  POLYMARKET_SYNC_ORDERBOOK: "polymarket:sync:orderbook",
  POLYMARKET_SYNC_ORDERBOOK_FULL_CATALOG: "polymarket:sync:orderbook:full_catalog",
  POLYMARKET_SYNC_TRADES: "polymarket:sync:trades",
  POLYMARKET_SYNC_TRADES_FULL_CATALOG: "polymarket:sync:trades:full_catalog",
  POLYMARKET_SYNC_OI: "polymarket:sync:oi",
  POLYMARKET_SYNC_OI_FULL_CATALOG: "polymarket:sync:oi:full_catalog",

  KALSHI_SYNC_METADATA: "kalshi:sync:metadata",
  KALSHI_SYNC_PRICES: "kalshi:sync:prices",
  KALSHI_SYNC_PRICES_FULL_CATALOG: "kalshi:sync:prices:full_catalog",
  KALSHI_SYNC_ORDERBOOK: "kalshi:sync:orderbook",
  KALSHI_SYNC_ORDERBOOK_FULL_CATALOG: "kalshi:sync:orderbook:full_catalog",
  KALSHI_SYNC_TRADES: "kalshi:sync:trades",
  KALSHI_SYNC_TRADES_FULL_CATALOG: "kalshi:sync:trades:full_catalog",
  KALSHI_SYNC_OI: "kalshi:sync:oi",
  KALSHI_SYNC_OI_FULL_CATALOG: "kalshi:sync:oi:full_catalog"
} as const;

export type JobName = (typeof JOB_NAMES)[keyof typeof JOB_NAMES];

export const ACTIVE_WORKER_JOB_NAMES: JobName[] = Object.values(JOB_NAMES);

export const ALL_JOB_NAMES: JobName[] = [...ACTIVE_WORKER_JOB_NAMES];
