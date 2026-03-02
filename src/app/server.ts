import Fastify from "fastify";
import { sql } from "drizzle-orm";

import { env } from "../config/env.js";
import { db, closeDb } from "../db/client.js";
import {
  getCoverageMeta,
  getCategoryQualityMeta,
  getDataFreshness,
  getDashboardMain,
  getDashboardTreemap,
  getIngestHealth,
  getMarketDetail,
  getMarketPriceHistory,
  getProvidersMeta,
  listMarkets
} from "../services/query-service.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";

function isProviderCode(value: string | undefined): value is ProviderCode {
  return value === "polymarket" || value === "kalshi";
}

export async function createServer(): Promise<ReturnType<typeof Fastify>> {
  const app = Fastify({ logger: false });

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

  app.get("/v1/dashboard/main", async () => {
    const dashboard = await getDashboardMain();
    return {
      data: dashboard,
      timestamp: new Date().toISOString()
    };
  });

  app.get("/v1/dashboard/treemap", async (request, reply) => {
    const query = request.query as {
      provider?: string;
      metric?: string;
      status?: string;
      groupBy?: string;
    };

    if (query.provider && !isProviderCode(query.provider)) {
      return reply.status(400).send({ error: "Invalid provider. Use polymarket or kalshi." });
    }

    const metric = query.metric ?? "volume24h";
    const status = query.status ?? "all";
    const groupBy = query.groupBy ?? "sector";

    if (metric !== "volume24h" && metric !== "oi") {
      return reply.status(400).send({ error: "Invalid metric. Use volume24h or oi." });
    }

    if (status !== "all" && status !== "active") {
      return reply.status(400).send({ error: "Invalid status. Use all or active." });
    }

    if (groupBy !== "sector" && groupBy !== "providerCategory") {
      return reply.status(400).send({ error: "Invalid groupBy. Use sector or providerCategory." });
    }

    const treemap = await getDashboardTreemap({
      providerCode: query.provider as ProviderCode | undefined,
      metric: metric as "volume24h" | "oi",
      status: status as "all" | "active",
      groupBy: groupBy as "sector" | "providerCategory"
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
