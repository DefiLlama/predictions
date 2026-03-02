import type { ProviderCode } from "../types/domain.js";

export type CategoryConfidence = "high" | "medium" | "low";
export type CategorySourceKind =
  | "event_category"
  | "event_tag"
  | "product_metadata_scope"
  | "product_metadata_competition"
  | "title_keyword"
  | "fallback_unknown";

export interface CanonicalSector {
  code: string;
  label: string;
}

export interface ProviderCategoryMapSeed {
  providerCode: ProviderCode;
  sourceKind: CategorySourceKind;
  sourceCode: string;
  sourceLabel: string;
  canonicalCode: string;
  priority: number;
  notes?: string;
}

export const CANONICAL_SECTORS: CanonicalSector[] = [
  { code: "politics", label: "Politics" },
  { code: "macro", label: "Macro" },
  { code: "crypto", label: "Crypto" },
  { code: "sports", label: "Sports" },
  { code: "culture", label: "Culture" },
  { code: "science_tech", label: "Science & Tech" },
  { code: "business", label: "Business" },
  { code: "health", label: "Health" },
  { code: "weather_climate", label: "Weather & Climate" },
  { code: "world", label: "World" },
  { code: "mentions", label: "Mentions" },
  { code: "other", label: "Other" },
  { code: "unknown", label: "Unknown" }
];

export const POLYMARKET_TAG_NOISE_CODES = new Set([
  "hide_from_new",
  "recurring",
  "5m",
  "15m",
  "1h",
  "4h",
  "up_or_down",
  "all",
  "parent_for_derivative",
  "earn_4",
  "earn_4_percent"
]);

const POLYMARKET_CATEGORY_SEEDS: ProviderCategoryMapSeed[] = [
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "politics", sourceLabel: "Politics", canonicalCode: "politics", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "sports", sourceLabel: "Sports", canonicalCode: "sports", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "crypto", sourceLabel: "Crypto", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "business", sourceLabel: "Business", canonicalCode: "business", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "science", sourceLabel: "Science", canonicalCode: "science_tech", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "coronavirus", sourceLabel: "Coronavirus", canonicalCode: "health", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "health", sourceLabel: "Health", canonicalCode: "health", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "world", sourceLabel: "World", canonicalCode: "world", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "us_current_affairs", sourceLabel: "US Current Affairs", canonicalCode: "politics", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_category", sourceCode: "pop_culture", sourceLabel: "Pop Culture", canonicalCode: "culture", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "politics", sourceLabel: "Politics", canonicalCode: "politics", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "elections", sourceLabel: "Elections", canonicalCode: "politics", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "us_election", sourceLabel: "US Election", canonicalCode: "politics", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "world_elections", sourceLabel: "World Elections", canonicalCode: "politics", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "global_elections", sourceLabel: "Global Elections", canonicalCode: "politics", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "geopolitics", sourceLabel: "Geopolitics", canonicalCode: "world", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "foreign_policy", sourceLabel: "Foreign Policy", canonicalCode: "world", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "sports", sourceLabel: "Sports", canonicalCode: "sports", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "soccer", sourceLabel: "Soccer", canonicalCode: "sports", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "basketball", sourceLabel: "Basketball", canonicalCode: "sports", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "nba", sourceLabel: "NBA", canonicalCode: "sports", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "ncaa", sourceLabel: "NCAA", canonicalCode: "sports", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "tennis", sourceLabel: "Tennis", canonicalCode: "sports", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "golf", sourceLabel: "Golf", canonicalCode: "sports", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "crypto", sourceLabel: "Crypto", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "crypto_prices", sourceLabel: "Crypto Prices", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "bitcoin", sourceLabel: "Bitcoin", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "ethereum", sourceLabel: "Ethereum", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "solana", sourceLabel: "Solana", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "xrp", sourceLabel: "XRP", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "ripple", sourceLabel: "Ripple", canonicalCode: "crypto", priority: 10 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "culture", sourceLabel: "Culture", canonicalCode: "culture", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "music", sourceLabel: "Music", canonicalCode: "culture", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "celebrities", sourceLabel: "Celebrities", canonicalCode: "culture", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "awards", sourceLabel: "Awards", canonicalCode: "culture", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "finance", sourceLabel: "Finance", canonicalCode: "business", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "economy", sourceLabel: "Economy", canonicalCode: "macro", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "fed", sourceLabel: "Fed", canonicalCode: "macro", priority: 12 },
  { providerCode: "polymarket", sourceKind: "event_tag", sourceCode: "fed_rates", sourceLabel: "Fed Rates", canonicalCode: "macro", priority: 10 }
];

const KALSHI_CATEGORY_SEEDS: ProviderCategoryMapSeed[] = [
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "sports", sourceLabel: "Sports", canonicalCode: "sports", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "politics", sourceLabel: "Politics", canonicalCode: "politics", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "entertainment", sourceLabel: "Entertainment", canonicalCode: "culture", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "crypto", sourceLabel: "Crypto", canonicalCode: "crypto", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "companies", sourceLabel: "Companies", canonicalCode: "business", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "economics", sourceLabel: "Economics", canonicalCode: "macro", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "climate_and_weather", sourceLabel: "Climate and Weather", canonicalCode: "weather_climate", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "elections", sourceLabel: "Elections", canonicalCode: "politics", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "mentions", sourceLabel: "Mentions", canonicalCode: "mentions", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "science_and_technology", sourceLabel: "Science and Technology", canonicalCode: "science_tech", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "financials", sourceLabel: "Financials", canonicalCode: "business", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "social", sourceLabel: "Social", canonicalCode: "culture", priority: 15 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "world", sourceLabel: "World", canonicalCode: "world", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "health", sourceLabel: "Health", canonicalCode: "health", priority: 10 },
  { providerCode: "kalshi", sourceKind: "event_category", sourceCode: "transportation", sourceLabel: "Transportation", canonicalCode: "business", priority: 15 },
  { providerCode: "kalshi", sourceKind: "product_metadata_scope", sourceCode: "game", sourceLabel: "Game", canonicalCode: "sports", priority: 10 },
  { providerCode: "kalshi", sourceKind: "product_metadata_scope", sourceCode: "mentions", sourceLabel: "Mentions", canonicalCode: "mentions", priority: 10 },
  { providerCode: "kalshi", sourceKind: "product_metadata_scope", sourceCode: "event", sourceLabel: "Event", canonicalCode: "other", priority: 20 },
  { providerCode: "kalshi", sourceKind: "product_metadata_competition", sourceCode: "ufc", sourceLabel: "UFC", canonicalCode: "sports", priority: 12 },
  { providerCode: "kalshi", sourceKind: "product_metadata_competition", sourceCode: "nba", sourceLabel: "NBA", canonicalCode: "sports", priority: 12 },
  { providerCode: "kalshi", sourceKind: "product_metadata_competition", sourceCode: "nfl", sourceLabel: "NFL", canonicalCode: "sports", priority: 12 }
];

export const PROVIDER_CATEGORY_MAP_SEED: ProviderCategoryMapSeed[] = [...POLYMARKET_CATEGORY_SEEDS, ...KALSHI_CATEGORY_SEEDS];

interface RegexRule {
  canonicalCode: string;
  pattern: RegExp;
}

const TEXT_RULES: RegexRule[] = [
  { canonicalCode: "sports", pattern: /\b(nba|nfl|mlb|nhl|soccer|football|basketball|tennis|golf|ufc|fifa|champions league|match)\b/i },
  { canonicalCode: "crypto", pattern: /\b(crypto|bitcoin|ethereum|solana|xrp|ripple|btc|eth)\b/i },
  { canonicalCode: "politics", pattern: /\b(election|president|congress|senate|policy|white house|trump|biden|vote)\b/i },
  { canonicalCode: "macro", pattern: /\b(inflation|fed|rates|gdp|cpi|economy|recession|unemployment)\b/i },
  { canonicalCode: "business", pattern: /\b(company|companies|earnings|stock|shares|market cap|financial)\b/i },
  { canonicalCode: "culture", pattern: /\b(movie|music|celebrity|awards|entertainment|tv|festival)\b/i },
  { canonicalCode: "science_tech", pattern: /\b(ai|artificial intelligence|science|technology|space|nasa|chip|software)\b/i },
  { canonicalCode: "health", pattern: /\b(covid|health|disease|virus|vaccine|hospital|medical)\b/i },
  { canonicalCode: "weather_climate", pattern: /\b(weather|climate|hurricane|storm|rainfall|temperature)\b/i },
  { canonicalCode: "world", pattern: /\b(world|geopolitics|ukraine|russia|china|israel|iran|war)\b/i },
  { canonicalCode: "mentions", pattern: /\b(mentions)\b/i }
];

const sectorByCode = new Map(CANONICAL_SECTORS.map((sector) => [sector.code, sector]));

export function normalizeCategoryCode(value: string | null | undefined): string {
  if (!value) {
    return "unknown";
  }

  const normalized = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

  return normalized.length > 0 ? normalized.slice(0, 128) : "unknown";
}

export function normalizeCategoryLabel(value: string | null | undefined, fallbackCode?: string): string {
  const trimmed = value?.trim();
  if (trimmed && trimmed.length > 0) {
    return trimmed.slice(0, 128);
  }

  const code = fallbackCode ?? "unknown";
  return code
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ")
    .slice(0, 128);
}

export function getCanonicalSector(code: string): CanonicalSector {
  return sectorByCode.get(code) ?? sectorByCode.get("unknown")!;
}

export function getProviderSeedMap(providerCode: ProviderCode, sourceKind: CategorySourceKind): Map<string, ProviderCategoryMapSeed> {
  const rows = PROVIDER_CATEGORY_MAP_SEED.filter((row) => row.providerCode === providerCode && row.sourceKind === sourceKind);
  return new Map(rows.map((row) => [normalizeCategoryCode(row.sourceCode), row]));
}

export function mapCategoryByText(text: string | null | undefined): string | null {
  if (!text) {
    return null;
  }

  for (const rule of TEXT_RULES) {
    if (rule.pattern.test(text)) {
      return rule.canonicalCode;
    }
  }

  return null;
}
