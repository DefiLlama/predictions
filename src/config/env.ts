import "dotenv/config";

import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error", "fatal"]).default("info"),
  DATABASE_URL: z.string().url().default("postgres://postgres:postgres@localhost:5432/prediction_markets"),
  CORS_ORIGIN: z.string().default("*"),

  POLYMARKET_GAMMA_BASE_URL: z.string().url().default("https://gamma-api.polymarket.com"),
  POLYMARKET_CLOB_BASE_URL: z.string().url().default("https://clob.polymarket.com"),
  POLYMARKET_DATA_BASE_URL: z.string().url().default("https://data-api.polymarket.com"),
  POLYMARKET_PRICE_FIDELITY_MINUTES: z.coerce.number().int().positive().default(60),
  POLYMARKET_PRICE_CONCURRENCY: z.coerce.number().int().positive().default(24),
  POLYMARKET_ORDERBOOK_CONCURRENCY: z.coerce.number().int().positive().default(24),
  POLYMARKET_TRADES_MARKET_CONCURRENCY: z.coerce.number().int().positive().default(6),
  POLYMARKET_OI_MARKET_CONCURRENCY: z.coerce.number().int().positive().default(12),
  POLYMARKET_SCOPE_TOP_N: z.coerce.number().int().positive().default(200),
  POLYMARKET_METADATA_RUN_BUDGET_MS: z.coerce.number().int().positive().default(10 * 60 * 1000),
  POLYMARKET_METADATA_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(150),
  POLYMARKET_TRADES_RUN_BUDGET_MS: z.coerce.number().int().positive().default(60 * 1000),
  POLYMARKET_TRADES_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(7),
  POLYMARKET_BACKFILL_RUN_BUDGET_MS: z.coerce.number().int().positive().default(20 * 60 * 1000),
  POLYMARKET_BACKFILL_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(300),
  POLYMARKET_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  PRICE_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  POLYMARKET_LARGE_BET_USD_THRESHOLD: z.coerce.number().positive().default(10000),
  POLYMARKET_TRADES_FILTER_MODE: z.enum(["all", "large_cash"]).default("all"),
  POLYMARKET_METADATA_MODE: z.enum(["incremental", "backfill"]).default("incremental"),

  KALSHI_BASE_URL: z.string().url().default("https://api.elections.kalshi.com/trade-api/v2"),
  KALSHI_PRICE_BATCH_CONCURRENCY: z.coerce.number().int().positive().default(6),
  KALSHI_EVENT_LOOKUP_CONCURRENCY: z.coerce.number().int().positive().default(6),
  KALSHI_ORDERBOOK_CONCURRENCY: z.coerce.number().int().positive().default(6),
  KALSHI_TRADES_MARKET_CONCURRENCY: z.coerce.number().int().positive().default(6),
  KALSHI_OI_MARKET_CONCURRENCY: z.coerce.number().int().positive().default(6),
  KALSHI_METADATA_RUN_BUDGET_MS: z.coerce.number().int().positive().default(10 * 60 * 1000),
  KALSHI_METADATA_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(100),
  KALSHI_MULTIVARIATE_RUN_BUDGET_MS: z.coerce.number().int().positive().default(10 * 60 * 1000),
  KALSHI_MULTIVARIATE_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(60),
  KALSHI_TRADES_RUN_BUDGET_MS: z.coerce.number().int().positive().default(60 * 1000),
  KALSHI_TRADES_MAX_REQUESTS_PER_RUN: z.coerce.number().int().positive().default(30),
  KALSHI_EVENT_FALLBACK_MAX_PER_RUN: z.coerce.number().int().positive().default(2000),
  TRADES_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  OI_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  INGEST_INCREMENTAL_OVERLAP_SECONDS: z.coerce.number().int().nonnegative().default(5 * 60),
  MARKET_RELINK_MAX_MARKETS_PER_RUN: z.coerce.number().int().positive().default(50000),
  CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN: z.coerce.number().int().positive().default(5000),
  FULL_CATALOG_MARKET_BATCH_SIZE: z.coerce.number().int().positive().default(400),
  FULL_CATALOG_INSTRUMENT_BATCH_SIZE: z.coerce.number().int().positive().default(800),

  CRON_TOPN_LOCK_KEY: z.coerce.bigint().default(161001n),
  CRON_FULLCAT_LOCK_KEY: z.coerce.bigint().default(161002n),
  CRON_FULLCAT_PROVIDER_CONCURRENCY: z.coerce.number().int().positive().default(2),
  CRON_LOCK_TIMEOUT_MS: z.coerce.number().int().nonnegative().default(0),
  CRON_STEP_TIMEOUT_MS: z.coerce.number().int().positive().default(20 * 60 * 1000),
  FULL_CATALOG_RESUME_INVOCATION_BUDGET_MS: z.coerce.number().int().positive().default(55 * 60 * 1000),
  FULL_CATALOG_RESUME_STEP_BUDGET_MS: z.coerce.number().int().positive().default(15 * 60 * 1000),
  FULL_CATALOG_CYCLE_TTL_MS: z.coerce.number().int().positive().default(24 * 60 * 60 * 1000),
  FULL_CATALOG_MAX_STEP_RETRIES: z.coerce.number().int().positive().default(16),
  FULL_CATALOG_MAX_STEPS_PER_INVOCATION: z.coerce.number().int().positive().default(1),
  FULL_CATALOG_MAX_STEPS_PER_PROVIDER_PER_INVOCATION: z.coerce.number().int().positive().default(1),
  JOB_RUN_STALE_AFTER_MS: z.coerce.number().int().positive().default(60 * 60 * 1000)
});

export type AppEnv = z.infer<typeof envSchema>;

export const env: AppEnv = envSchema.parse(process.env);
