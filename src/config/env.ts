import "dotenv/config";

import { z } from "zod";

const envBoolean = z.preprocess((value) => {
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") {
      return true;
    }
    if (normalized === "false") {
      return false;
    }
  }
  return value;
}, z.boolean());

const envSchema = z.object({
  NODE_ENV: z.enum(["development", "test", "production"]).default("development"),
  PORT: z.coerce.number().int().positive().default(3000),
  LOG_LEVEL: z.enum(["trace", "debug", "info", "warn", "error", "fatal"]).default("info"),
  DATABASE_URL: z.string().url().default("postgres://postgres:postgres@localhost:5432/prediction_markets"),
  PG_BOSS_SCHEMA: z.string().min(1).default("ops"),

  POLYMARKET_GAMMA_BASE_URL: z.string().url().default("https://gamma-api.polymarket.com"),
  POLYMARKET_CLOB_BASE_URL: z.string().url().default("https://clob.polymarket.com"),
  POLYMARKET_DATA_BASE_URL: z.string().url().default("https://data-api.polymarket.com"),
  POLYMARKET_SCOPE_TOP_N: z.coerce.number().int().positive().default(200),
  POLYMARKET_MAX_PAGES: z.coerce.number().int().positive().default(100),
  POLYMARKET_TRADES_MAX_PAGES: z.coerce.number().int().positive().default(20),
  POLYMARKET_BACKFILL_MAX_PAGES_PER_RUN: z.coerce.number().int().positive().default(200),
  POLYMARKET_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  PRICE_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  POLYMARKET_LARGE_BET_USD_THRESHOLD: z.coerce.number().positive().default(10000),
  POLYMARKET_TRADES_FILTER_MODE: z.enum(["all", "large_cash"]).default("all"),
  POLYMARKET_METADATA_MODE: z.enum(["incremental", "backfill"]).default("incremental"),

  KALSHI_BASE_URL: z.string().url().default("https://api.elections.kalshi.com/trade-api/v2"),
  KALSHI_MAX_PAGES: z.coerce.number().int().positive().default(60),
  KALSHI_MULTIVARIATE_MAX_PAGES: z.coerce.number().int().positive().default(30),
  KALSHI_TRADES_MAX_PAGES: z.coerce.number().int().positive().default(20),
  KALSHI_EVENT_FALLBACK_MAX_PER_RUN: z.coerce.number().int().positive().default(2000),
  TRADES_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  OI_LOOKBACK_DAYS: z.coerce.number().int().positive().default(7),
  INGEST_INCREMENTAL_OVERLAP_SECONDS: z.coerce.number().int().nonnegative().default(5 * 60),
  MARKET_RELINK_MAX_MARKETS_PER_RUN: z.coerce.number().int().positive().default(50000),
  CATEGORY_BACKFILL_MAX_MARKETS_PER_RUN: z.coerce.number().int().positive().default(5000),

  ENABLE_SCHEDULER: envBoolean.default(false),
  ACTIVE_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(5 * 60 * 1000),
  CLOSED_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(30 * 60 * 1000),
  JOB_RUN_STALE_AFTER_MS: z.coerce.number().int().positive().default(60 * 60 * 1000)
});

export type AppEnv = z.infer<typeof envSchema>;

export const env: AppEnv = envSchema.parse(process.env);
