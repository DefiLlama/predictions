import { Client } from "pg";

import { env } from "../config/env.js";

function escapeIdentifier(value: string): string {
  return `"${value.replaceAll('"', '""')}"`;
}

async function main(): Promise<void> {
  const parsed = new URL(env.DATABASE_URL);
  const dbName = parsed.pathname.replace(/^\//, "");

  if (!dbName) {
    throw new Error("DATABASE_URL must include a database name");
  }

  parsed.pathname = "/postgres";

  const client = new Client({ connectionString: parsed.toString() });
  await client.connect();

  const exists = await client.query<{ exists: boolean }>(
    "select exists(select 1 from pg_database where datname = $1) as exists",
    [dbName]
  );

  if (!exists.rows[0]?.exists) {
    await client.query(`create database ${escapeIdentifier(dbName)}`);
    console.log(`Created database: ${dbName}`);
  } else {
    console.log(`Database already exists: ${dbName}`);
  }

  await client.end();
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
