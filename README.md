# Prediction Markets Ingestion Service

Provider-agnostic backend scaffold with Polymarket and Kalshi ingestion adapters.

## Stack

- Node 22 runtime
- npm package manager
- TypeScript + Fastify
- PostgreSQL + Drizzle ORM/migrations
- pg-boss job orchestration

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

Start API server (also starts workers; scheduler off by default):

```bash
npm run start
```

Start dedicated worker with scheduler enabled:

```bash
npm run worker
```

Polymarket ingestion:

```bash
npm run ingest:metadata
npm run ingest:metadata:backfill
npm run ingest:prices
npm run ingest:orderbook
npm run ingest:trades
npm run ingest:oi
```

Kalshi ingestion:

```bash
npm run ingest:kalshi:metadata
npm run ingest:kalshi:prices
npm run ingest:kalshi:orderbook
npm run ingest:kalshi:trades
npm run ingest:kalshi:oi
npm run ingest:categories
npm run ingest:categories:backfill
```

Hourly rollups (price + liquidity, both providers):

```bash
npm run ingest:rollups
```

Combined run:

```bash
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
- Incremental metadata is bounded (`POLYMARKET_MAX_PAGES`), full Polymarket metadata backfill is resumable (`POLYMARKET_BACKFILL_MAX_PAGES_PER_RUN`).
