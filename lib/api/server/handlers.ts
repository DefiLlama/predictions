import { sql } from "drizzle-orm";

import {
  getCachedDashboardBenchmarks,
  getCachedDashboardMain,
  getCachedDashboardTreemap,
  getCachedTopTrades,
} from "@/lib/api/server/dashboard-data";
import { db } from "@/src/db/client";
import {
  getCategoryQualityMeta,
  getCoverageMeta,
  getDataFreshness,
  getEventDetail,
  getEventLatestTrades,
  getEventPriceHistory,
  getIngestHealth,
  getMarketDetail,
  getMarketPriceHistory,
  getProvidersMeta,
  listMarkets,
} from "@/src/services/query-service";
import type { ProviderCode } from "@/src/types/domain";
import { logger } from "@/src/utils/logger";

import { badRequest, notFound } from "./errors";
import { jsonWithCors } from "./response";
import {
  isProviderCode,
  parseBooleanFlag,
  parseIntervalRange,
  parseOptionalPositiveInt,
  parsePagination,
} from "./validation";

function nowIso(): string {
  return new Date().toISOString();
}

function dataEnvelope(request: Request, data: unknown, init?: ResponseInit): Response {
  return jsonWithCors(
    request,
    {
      data,
      timestamp: nowIso(),
    },
    init,
  );
}

export async function handleHealthz(request: Request): Promise<Response> {
  return jsonWithCors(request, { status: "ok", timestamp: nowIso() });
}

export async function handleReadyz(request: Request): Promise<Response> {
  try {
    await db.execute(sql`select 1`);
    return jsonWithCors(request, { status: "ready", timestamp: nowIso() });
  } catch (error) {
    logger.error({ error }, "Readiness check failed");
    return jsonWithCors(
      request,
      {
        status: "not_ready",
        timestamp: nowIso(),
      },
      { status: 503 },
    );
  }
}

export async function handleMetaProviders(request: Request): Promise<Response> {
  const providers = await getProvidersMeta();
  return dataEnvelope(request, providers);
}

export async function handleMetaCoverage(request: Request): Promise<Response> {
  const coverage = await getCoverageMeta();
  return dataEnvelope(request, coverage);
}

export async function handleMetaIngestHealth(request: Request): Promise<Response> {
  const health = await getIngestHealth();
  return dataEnvelope(request, health);
}

export async function handleMetaDataFreshness(request: Request): Promise<Response> {
  const freshness = await getDataFreshness();
  return dataEnvelope(request, freshness);
}

export async function handleMetaCategoryQuality(request: Request): Promise<Response> {
  const quality = await getCategoryQualityMeta();
  return dataEnvelope(request, quality);
}

export async function handleDashboardMain(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const provider = searchParams.get("provider") ?? undefined;

  if (provider && !isProviderCode(provider)) {
    return badRequest(request, "Invalid provider. Use polymarket or kalshi.");
  }

  const limit = parseOptionalPositiveInt(searchParams.get("limit"), { max: 100 });
  if (limit === null) {
    return badRequest(request, "Invalid limit. Use an integer between 1 and 100.");
  }

  const marketLimitPerEvent = parseOptionalPositiveInt(searchParams.get("marketLimitPerEvent"), {
    max: 20,
  });
  if (marketLimitPerEvent === null) {
    return badRequest(
      request,
      "Invalid marketLimitPerEvent. Use an integer between 1 and 20.",
    );
  }

  const includeNestedRaw = searchParams.get("includeNested");
  const includeNested =
    includeNestedRaw === null ? true : parseBooleanFlag(includeNestedRaw);

  const dashboard = await getCachedDashboardMain({
    providerCode: provider as ProviderCode | undefined,
    limit,
    marketLimitPerEvent,
    includeNested,
  });

  return dataEnvelope(request, dashboard);
}

export async function handleDashboardBenchmarks(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const provider = searchParams.get("provider") ?? undefined;

  if (provider && !isProviderCode(provider)) {
    return badRequest(request, "Invalid provider. Use polymarket or kalshi.");
  }

  const benchmarks = await getCachedDashboardBenchmarks(provider as ProviderCode | undefined);
  return dataEnvelope(request, benchmarks);
}

export async function handleDashboardTreemap(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const provider = searchParams.get("provider") ?? undefined;

  if (provider && !isProviderCode(provider)) {
    return badRequest(request, "Invalid provider. Use polymarket or kalshi.");
  }

  const coverage = searchParams.get("coverage") ?? "all";
  if (coverage !== "all" && coverage !== "scope") {
    return badRequest(request, "Invalid coverage. Use all or scope.");
  }

  const treemap = await getCachedDashboardTreemap({
    providerCode: provider as ProviderCode | undefined,
    coverage: coverage as "all" | "scope",
  });

  return dataEnvelope(request, treemap);
}

export async function handleMarkets(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const provider = searchParams.get("provider") ?? undefined;
  const status = searchParams.get("status") ?? "active";

  if (provider && !isProviderCode(provider)) {
    return badRequest(request, "Invalid provider. Use polymarket or kalshi.");
  }
  if (status !== "active" && status !== "all") {
    return badRequest(request, "Invalid status. Use active or all.");
  }

  const pagination = parsePagination(searchParams.get("limit"), searchParams.get("offset"), {
    defaultLimit: 50,
    maxLimit: 500,
  });

  if (!pagination) {
    return badRequest(request, "Invalid pagination values.");
  }

  const markets = await listMarkets({
    providerCode: provider as ProviderCode | undefined,
    status: status as "active" | "all",
    limit: pagination.limit,
    offset: pagination.offset,
  });

  return jsonWithCors(request, {
    data: markets,
    pagination,
    timestamp: nowIso(),
  });
}

export async function handleTopTrades(request: Request): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const window = searchParams.get("window") ?? "24h";

  if (window !== "24h" && window !== "7d" && window !== "30d") {
    return badRequest(request, "Invalid window. Use 24h, 7d, or 30d.");
  }

  const provider = searchParams.get("provider") ?? undefined;
  if (provider && !isProviderCode(provider)) {
    return badRequest(request, "Invalid provider. Use polymarket or kalshi.");
  }

  const pagination = parsePagination(searchParams.get("limit"), searchParams.get("offset"), {
    defaultLimit: 50,
    maxLimit: 200,
  });

  if (!pagination) {
    return badRequest(request, "Invalid pagination values.");
  }

  const summaryOnly = parseBooleanFlag(searchParams.get("summaryOnly"));

  const result = await getCachedTopTrades({
    window: window as "24h" | "7d" | "30d",
    providerCode: provider as ProviderCode | undefined,
    limit: pagination.limit,
    offset: pagination.offset,
    summaryOnly,
  });

  return dataEnvelope(request, result);
}

export async function handleMarketDetail(request: Request, marketUid: string): Promise<Response> {
  const detail = await getMarketDetail(marketUid);
  if (!detail) {
    return notFound(request, "Market not found");
  }

  return dataEnvelope(request, detail);
}

export async function handleEventDetail(request: Request, eventUid: string): Promise<Response> {
  const detail = await getEventDetail(eventUid);
  if (!detail) {
    return notFound(request, "Event not found");
  }

  return dataEnvelope(request, detail);
}

export async function handleEventTrades(request: Request, eventUid: string): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const limitRaw = searchParams.get("limit");

  let limit: number | undefined;
  if (limitRaw !== null) {
    const parsedLimit = Number(limitRaw);
    if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
      return badRequest(request, "Invalid limit. Use an integer between 1 and 100.");
    }
    limit = parsedLimit;
  }

  const trades = await getEventLatestTrades({ eventUid, limit });
  if (!trades) {
    return notFound(request, "Event not found");
  }

  return dataEnvelope(request, trades);
}

export async function handleEventPriceHistory(request: Request, eventUid: string): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const range = parseIntervalRange({
    intervalRaw: searchParams.get("interval"),
    fromRaw: searchParams.get("from"),
    toRaw: searchParams.get("to"),
  });

  if ("error" in range) {
    return badRequest(request, range.error);
  }

  const history = await getEventPriceHistory({
    eventUid,
    from: range.from,
    to: range.to,
    interval: "1h",
  });

  if (!history) {
    return notFound(request, "Event not found");
  }

  return dataEnvelope(request, history);
}

export async function handleMarketPriceHistory(request: Request, marketUid: string): Promise<Response> {
  const searchParams = new URL(request.url).searchParams;
  const range = parseIntervalRange({
    intervalRaw: searchParams.get("interval"),
    fromRaw: searchParams.get("from"),
    toRaw: searchParams.get("to"),
  });

  if ("error" in range) {
    return badRequest(request, range.error);
  }

  const history = await getMarketPriceHistory({
    marketUid,
    from: range.from,
    to: range.to,
    interval: "1h",
  });

  if (!history) {
    return notFound(request, "Market not found");
  }

  return dataEnvelope(request, history);
}
