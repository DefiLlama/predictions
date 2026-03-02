import { drizzle, type NodePgDatabase } from "drizzle-orm/node-postgres";
import { Pool } from "pg";

import { env } from "../config/env.js";
import * as schema from "./schema.js";

const pool = new Pool({ connectionString: env.DATABASE_URL });

export const db: NodePgDatabase<typeof schema> = drizzle(pool, { schema });

export const sqlPool = pool;

export async function closeDb(): Promise<void> {
  await pool.end();
}
