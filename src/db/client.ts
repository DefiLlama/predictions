import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

import { env } from "../config/env.js";
import { logger } from "../utils/logger.js";
import * as schema from "./schema.js";

const pool = new Pool({
  connectionString: env.DATABASE_URL,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10_000
});

pool.on("error", (error) => {
  logger.error({ error }, "Postgres pool emitted idle client error");
});

export const db: NodePgDatabase<typeof schema> = drizzle(pool, { schema });

export const sqlPool = pool;

export async function closeDb(): Promise<void> {
  await pool.end();
}
