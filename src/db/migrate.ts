import { migrate } from "drizzle-orm/node-postgres/migrator";

import { db, closeDb } from "./client.js";

async function main(): Promise<void> {
  await migrate(db, { migrationsFolder: "src/migrations" });
  await closeDb();
}

main().catch(async (error) => {
  console.error(error);
  await closeDb();
  process.exitCode = 1;
});
