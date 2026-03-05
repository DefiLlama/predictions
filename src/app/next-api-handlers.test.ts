import assert from "node:assert/strict";
import { describe, test } from "node:test";

import { applyCorsHeaders, parseCorsPolicy } from "../../lib/api/server/cors";
import {
  handleDashboardMain,
  handleDashboardTreemap,
  handleEventPriceHistory,
  handleHealthz,
  handleMarkets,
  handleTopTrades,
} from "../../lib/api/server/handlers";

describe("Next API handlers", () => {
  test("healthz returns liveness payload", async () => {
    const response = await handleHealthz(new Request("http://127.0.0.1:3000/healthz"));
    assert.equal(response.status, 200);

    const payload = (await response.json()) as { status: string; timestamp: string };
    assert.equal(payload.status, "ok");
    assert.equal(typeof payload.timestamp, "string");
  });

  test("dashboard main rejects invalid provider", async () => {
    const response = await handleDashboardMain(
      new Request("http://127.0.0.1:3000/v1/dashboard/main?provider=invalid"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid provider. Use polymarket or kalshi.");
  });

  test("dashboard main rejects invalid limit", async () => {
    const response = await handleDashboardMain(
      new Request("http://127.0.0.1:3000/v1/dashboard/main?limit=0"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid limit. Use an integer between 1 and 100.");
  });

  test("dashboard main rejects invalid market limit", async () => {
    const response = await handleDashboardMain(
      new Request("http://127.0.0.1:3000/v1/dashboard/main?marketLimitPerEvent=21"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(
      payload.error,
      "Invalid marketLimitPerEvent. Use an integer between 1 and 20.",
    );
  });

  test("dashboard treemap rejects invalid coverage", async () => {
    const response = await handleDashboardTreemap(
      new Request("http://127.0.0.1:3000/v1/dashboard/treemap?coverage=bad"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid coverage. Use all or scope.");
  });

  test("markets rejects invalid pagination", async () => {
    const response = await handleMarkets(
      new Request("http://127.0.0.1:3000/v1/markets?limit=0&offset=-1"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid pagination values.");
  });

  test("markets rejects invalid status", async () => {
    const response = await handleMarkets(
      new Request("http://127.0.0.1:3000/v1/markets?status=closed"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid status. Use active or all.");
  });

  test("top trades rejects invalid window", async () => {
    const response = await handleTopTrades(
      new Request("http://127.0.0.1:3000/v1/trades/top?window=1d"),
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid window. Use 24h, 7d, or 30d.");
  });

  test("event price history rejects invalid interval", async () => {
    const response = await handleEventPriceHistory(
      new Request("http://127.0.0.1:3000/v1/events/polymarket:abc/price-history?interval=5m"),
      "polymarket:abc",
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid interval. Only 1h is supported.");
  });

  test("event price history rejects inverted date range", async () => {
    const response = await handleEventPriceHistory(
      new Request(
        "http://127.0.0.1:3000/v1/events/polymarket:abc/price-history?from=2026-03-01T00:00:00.000Z&to=2026-02-01T00:00:00.000Z",
      ),
      "polymarket:abc",
    );

    assert.equal(response.status, 400);
    const payload = (await response.json()) as { error: string };
    assert.equal(payload.error, "Invalid range: from must be less than or equal to to.");
  });
});

describe("CORS helpers", () => {
  test("wildcard policy returns '*'", () => {
    const policy = parseCorsPolicy("*");
    const headers = new Headers();

    applyCorsHeaders(new Request("http://127.0.0.1:3000/v1/markets"), headers, policy);

    assert.equal(headers.get("access-control-allow-origin"), "*");
    assert.equal(headers.get("access-control-allow-methods"), "GET,HEAD,OPTIONS");
  });

  test("allowlist policy reflects allowed origin", () => {
    const policy = parseCorsPolicy("https://allowed.example,https://other.example");
    const headers = new Headers();

    applyCorsHeaders(
      new Request("http://127.0.0.1:3000/v1/markets", {
        headers: { origin: "https://allowed.example" },
      }),
      headers,
      policy,
    );

    assert.equal(headers.get("access-control-allow-origin"), "https://allowed.example");
    assert.equal(headers.get("vary"), "Origin");
  });

  test("allowlist policy omits disallowed origin", () => {
    const policy = parseCorsPolicy("https://allowed.example");
    const headers = new Headers();

    applyCorsHeaders(
      new Request("http://127.0.0.1:3000/v1/markets", {
        headers: { origin: "https://blocked.example" },
      }),
      headers,
      policy,
    );

    assert.equal(headers.get("access-control-allow-origin"), null);
    assert.equal(headers.get("vary"), "Origin");
  });
});
