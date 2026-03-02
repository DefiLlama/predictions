# Prediction Markets Ingestion Service

Provider-agnostic backend scaffold with Polymarket and Kalshi ingestion adapters.

## Stack

- Node 22 runtime
- npm package manager
- TypeScript + Fastify
- PostgreSQL + Drizzle ORM/migrations
- Cron-driven ingestion pipelines

## Local Setup

1. Install dependencies:

```bash
npm install
```

2. Copy env file and adjust if needed:

```bash
cp .env.example .env
```

3. Create DB, run migrations, seed providers:

```bash
npm run db:prepare
```

## Run Commands

Start API server (API only):

```bash
npm run start
```

Cron pipelines:

```bash
npm run cron:topn-live
npm run cron:full-catalog
```

Recommended Coolify cron entries:

```cron
*/5 * * * * npm run cron:topn-live
0 */4 * * * npm run cron:full-catalog
```

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

Verification:

```bash
npm run verify:summary
npm run verify:summary:strict
```

Tests:

```bash
npm test
```

## Ingestion Modes

- `topN_live`: `core.market_scope`-based prioritized ingestion, designed for 5-minute cadence.
- `full_catalog`: active-market full-universe ingestion using batched selectors from `core.market`/`core.instrument`, designed for low-frequency cadence.

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
- `GET /v1/dashboard/main`
- `GET /v1/dashboard/treemap?provider=polymarket|kalshi&metric=volume24h|oi&status=all|active&groupBy=sector|providerCategory`

## Notes

- Public market IDs are `provider:marketRef`.
- Canonical probability scale in storage is `0..1`.
- `ops.job_run_log` is the canonical run history (step-level + cron skip outcomes).
- `ops.ingest_checkpoint` stores incremental windows and full-catalog cursor state.
