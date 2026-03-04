import Fastify from "fastify";
import cors from "@fastify/cors";
import { sql } from "drizzle-orm";

import { env } from "../config/env.js";
import { db, closeDb } from "../db/client.js";
import {
  getCoverageMeta,
  getCategoryQualityMeta,
  getDataFreshness,
  getDashboardMain,
  getDashboardTreemap,
  getEventDetail,
  getEventLatestTrades,
  getEventPriceHistory,
  getIngestHealth,
  getMarketDetail,
  getMarketPriceHistory,
  getProvidersMeta,
  getTopTrades,
  listMarkets
} from "../services/query-service.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";

function isProviderCode(value: string | undefined): value is ProviderCode {
  return value === "polymarket" || value === "kalshi";
}

function resolveCorsOrigin(originValue: string): true | string | string[] {
  const normalized = originValue.trim();
  if (normalized === "" || normalized === "*") {
    return true;
  }

  const origins = normalized
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);

  if (origins.length <= 1) {
    return origins[0] ?? true;
  }

  return origins;
}

export async function createServer(): Promise<ReturnType<typeof Fastify>> {
  const app = Fastify({ logger: false });
  await app.register(cors, {
    origin: resolveCorsOrigin(env.CORS_ORIGIN),
    methods: ["GET", "HEAD", "OPTIONS"]
  });

  app.get("/healthz", async () => ({ status: "ok", timestamp: new Date().toISOString() }));

  app.get("/readyz", async (_request, reply) => {
    try {
      await db.execute(sql`select 1`);
      return { status: "ready", timestamp: new Date().toISOString() };
    } catch (error) {
      logger.error({ error }, "Readiness check failed");
      return reply.status(503).send({ status: "not_ready", timestamp: new Date().toISOString() });
    }
  });

  app.get("/v1/meta/providers", async () => {
    const providers = await getProvidersMeta();
    return {
      data: providers,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/meta/coverage", async () => {
    const coverage = await getCoverageMeta();
    return {
      data: coverage,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/meta/ingest-health", async () => {
    const health = await getIngestHealth();
    return {
      data: health,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/meta/data-freshness", async () => {
    const freshness = await getDataFreshness();
    return {
      data: freshness,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/meta/category-quality", async () => {
    const quality = await getCategoryQualityMeta();
    return {
      data: quality,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/dashboard/main", async (request, reply) => {
    const query = request.query as {
      provider?: string;
    };

    if (query.provider && !isProviderCode(query.provider)) {
      return reply.status(400).send({ error: "Invalid provider. Use polymarket or kalshi." });
    }

    const dashboard = await getDashboardMain({
      providerCode: query.provider as ProviderCode | undefined
    });

    return {
      data: dashboard,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/dashboard/treemap", async (request, reply) => {
    const query = request.query as {
      provider?: string;
      metric?: string;
      coverage?: string;
    };

    if (query.provider && !isProviderCode(query.provider)) {
      return reply.status(400).send({ error: "Invalid provider. Use polymarket or kalshi." });
    }

    const metric = query.metric ?? "volume24h";
    const coverage = query.coverage ?? "all";

    if (metric !== "volume24h" && metric !== "liquidity") {
      return reply.status(400).send({ error: "Invalid metric. Use volume24h or liquidity." });
    }

    if (coverage !== "all" && coverage !== "scope") {
      return reply.status(400).send({ error: "Invalid coverage. Use all or scope." });
    }

    const treemap = await getDashboardTreemap({
      providerCode: query.provider as ProviderCode | undefined,
      metric: metric as "volume24h" | "liquidity",
      coverage: coverage as "all" | "scope"
    });

    return {
      data: treemap,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/markets", async (request, reply) => {
    const query = request.query as {
      provider?: string;
      limit?: string;
      offset?: string;
    };

    if (query.provider && !isProviderCode(query.provider)) {
      return reply.status(400).send({ error: "Invalid provider. Use polymarket or kalshi." });
    }

    const limit = Math.min(Number(query.limit ?? 50), 500);
    const offset = Number(query.offset ?? 0);

    if (!Number.isFinite(limit) || limit <= 0 || !Number.isFinite(offset) || offset < 0) {
      return reply.status(400).send({ error: "Invalid pagination values." });
    }

    const markets = await listMarkets({
      providerCode: query.provider as ProviderCode | undefined,
      limit,
      offset
    });

    return {
      data: markets,
      pagination: { limit, offset },
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/trades/top", async (request, reply) => {
    const query = request.query as {
      window?: string;
      provider?: string;
      limit?: string;
      offset?: string;
    };

    const window = query.window ?? "24h";
    if (window !== "24h" && window !== "7d" && window !== "30d") {
      return reply.status(400).send({ error: "Invalid window. Use 24h, 7d, or 30d." });
    }

    if (query.provider && !isProviderCode(query.provider)) {
      return reply.status(400).send({ error: "Invalid provider. Use polymarket or kalshi." });
    }

    const limit = Math.min(Number(query.limit ?? 50), 200);
    const offset = Number(query.offset ?? 0);

    if (!Number.isFinite(limit) || limit <= 0 || !Number.isFinite(offset) || offset < 0) {
      return reply.status(400).send({ error: "Invalid pagination values." });
    }

    const result = await getTopTrades({
      window: window as "24h" | "7d" | "30d",
      providerCode: query.provider as ProviderCode | undefined,
      limit,
      offset
    });

    return {
      data: result,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/markets/:marketUid", async (request, reply) => {
    const { marketUid } = request.params as { marketUid: string };

    const detail = await getMarketDetail(marketUid);
    if (!detail) {
      return reply.status(404).send({ error: "Market not found" });
    }

    return {
      data: detail,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/events/:eventUid", async (request, reply) => {
    const { eventUid } = request.params as { eventUid: string };

    const detail = await getEventDetail(eventUid);
    if (!detail) {
      return reply.status(404).send({ error: "Event not found" });
    }

    return {
      data: detail,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/events/:eventUid/trades", async (request, reply) => {
    const { eventUid } = request.params as { eventUid: string };
    const query = request.query as {
      limit?: string;
    };

    let limit: number | undefined;
    if (query.limit !== undefined) {
      const parsedLimit = Number(query.limit);
      if (!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
        return reply.status(400).send({ error: "Invalid limit. Use an integer between 1 and 100." });
      }
      limit = parsedLimit;
    }

    const trades = await getEventLatestTrades({ eventUid, limit });
    if (!trades) {
      return reply.status(404).send({ error: "Event not found" });
    }

    return {
      data: trades,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/events/:eventUid/price-history", async (request, reply) => {
    const { eventUid } = request.params as { eventUid: string };
    const query = request.query as {
      from?: string;
      to?: string;
      interval?: string;
    };

    const interval = query.interval ?? "1h";
    if (interval !== "1h") {
      return reply.status(400).send({ error: "Invalid interval. Only 1h is supported." });
    }

    const to = query.to ? new Date(query.to) : new Date();
    const from = query.from ? new Date(query.from) : new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
      return reply.status(400).send({ error: "Invalid from/to datetime. Use ISO-8601 timestamps." });
    }

    if (from > to) {
      return reply.status(400).send({ error: "Invalid range: from must be less than or equal to to." });
    }

    const history = await getEventPriceHistory({
      eventUid,
      from,
      to,
      interval: "1h"
    });

    if (!history) {
      return reply.status(404).send({ error: "Event not found" });
    }

    return {
      data: history,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/markets/:marketUid/price-history", async (request, reply) => {
    const { marketUid } = request.params as { marketUid: string };
    const query = request.query as {
      from?: string;
      to?: string;
      interval?: string;
    };

    const interval = query.interval ?? "1h";
    if (interval !== "1h") {
      return reply.status(400).send({ error: "Invalid interval. Only 1h is supported." });
    }

    const to = query.to ? new Date(query.to) : new Date();
    const from = query.from ? new Date(query.from) : new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

    if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
      return reply.status(400).send({ error: "Invalid from/to datetime. Use ISO-8601 timestamps." });
    }

    if (from > to) {
      return reply.status(400).send({ error: "Invalid range: from must be less than or equal to to." });
    }

    const history = await getMarketPriceHistory({
      marketUid,
      from,
      to,
      interval: "1h"
    });

    if (!history) {
      return reply.status(404).send({ error: "Market not found" });
    }

    return {
      data: history,
      timestamp: new Date().toISOString()
    };
  });

  return app;
}

export async function startServer(): Promise<void> {
  const app = await createServer();

  const onShutdown = async () => {
    logger.info("Shutting down server");
    await app.close();
    await closeDb();
    process.exit(0);
  };

  process.on("SIGINT", () => {
    void onShutdown();
  });

  process.on("SIGTERM", () => {
    void onShutdown();
  });

  await app.listen({
    host: "0.0.0.0",
    port: env.PORT
  });

  logger.info({ port: env.PORT }, "Prediction markets API started");
}
