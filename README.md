# Prediction Markets

Single-project Next.js application that serves both:

- Web frontend (App Router)
- API endpoints (`/healthz`, `/readyz`, `/v1/...`)

Backend domain/DB/ingestion code remains in `src/` and is reused by Next route handlers.

## Stack

- Node 22 runtime
- npm package manager
- TypeScript + Next.js 16 (App Router)
- PostgreSQL + Drizzle ORM/migrations
- Cron-driven ingestion pipelines

## Local Setup

1. Install dependencies:

```bash
npm install
```

2. Copy env file and adjust values as needed:

```bash
cp .env.example .env
```

- `CORS_ORIGIN` controls API CORS allowlist (`*` for open, or comma-separated origins).
- `PREDICTION_API_URL` is optional; defaults to `http://127.0.0.1:${PORT}` (or `3000`).

3. Create DB, run migrations, seed providers:

```bash
npm run db:prepare
```

## Run Commands

Local dev (web + API in one process):

```bash
npm run dev
```

Production web service:

```bash
npm run build
npm run start
```

Cron pipelines:

```bash
npm run cron:topn-live
npm run cron:full-catalog
npm run cron:full-catalog:resume
```

Recommended Coolify cron entries:

```cron
*/5 * * * * npm run cron:topn-live
*/15 * * * * npm run cron:full-catalog:resume
```

`cron:full-catalog` remains available as a monolithic/manual run. For environments with strict timeout caps, prefer `cron:full-catalog:resume`.

Manual ingestion (direct execution, no queue):

```bash
npm run ingest:metadata
npm run ingest:metadata:backfill
npm run ingest:prices
npm run ingest:orderbook
npm run ingest:trades
npm run ingest:oi

npm run ingest:kalshi:metadata
npm run ingest:kalshi:prices
npm run ingest:kalshi:orderbook
npm run ingest:kalshi:trades
npm run ingest:kalshi:oi

npm run ingest:categories
npm run ingest:categories:backfill
npm run ingest:rollups
npm run ingest:all
```

Verification and tests:

```bash
npm run verify:summary
npm run verify:summary:strict
npm run typecheck
npm test
```

## Coolify Deployment

Run as a single web service:

- Build command: `npm ci && npm run build`
- Start command: `npm run start`
- Port: `3000`
- Health check path: `/healthz`

Use separate Coolify cron jobs from this same repository for ingestion:

- `npm run cron:topn-live`
- `npm run cron:full-catalog:resume`

## Ingestion Modes

- `topN_live`: `core.market_scope`-based prioritized ingestion, designed for 5-minute cadence with seed top-N event-expanded scope selection.
- `full_catalog`: active-market full-universe ingestion using batched selectors from `core.market`/`core.instrument`, designed for low-frequency cadence.
- `full_catalog_resume`: full-catalog state machine that checkpoints provider/step progress (`ops.ingest_checkpoint`) and resumes from the latest step on the next invocation.

## API Endpoints

- `GET /healthz`
- `GET /readyz`
- `GET /v1/meta/providers`
- `GET /v1/meta/coverage`
- `GET /v1/meta/ingest-health`
- `GET /v1/meta/data-freshness`
- `GET /v1/meta/category-quality`
- `GET /v1/markets?provider=polymarket|kalshi&limit=&offset=`
- `GET /v1/markets/:marketUid`
- `GET /v1/events/:eventUid`
- `GET /v1/events/:eventUid/trades?limit=50`
- `GET /v1/events/:eventUid/price-history?from=&to=&interval=1h`
- `GET /v1/markets/:marketUid/price-history?from=&to=&interval=1h`
- `GET /v1/dashboard/main?provider=polymarket|kalshi`
- `GET /v1/dashboard/treemap?provider=polymarket|kalshi&coverage=all|scope`
- `GET /v1/trades/top?window=24h|7d|30d&provider=polymarket|kalshi&limit=&offset=`

## Notes

- Public market IDs are `provider:marketRef`.
- Public event IDs are `provider:eventRef`.
- Canonical probability scale in storage is `0..1`.
- `ops.job_run_log` is the canonical run history (step-level + cron skip outcomes).
- `ops.ingest_checkpoint` stores incremental windows and full-catalog cursor state.
- `/v1/dashboard/main` returns provider KPIs plus event-level rows with nested markets/instruments sourced from `core.market_scope`.
