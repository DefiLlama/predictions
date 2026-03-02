import { eq } from "drizzle-orm";

import { closeDb, db } from "./client.js";
import { platform } from "./schema.js";

const PLATFORMS = [
  { code: "polymarket", name: "Polymarket" },
  { code: "kalshi", name: "Kalshi" }
] as const;

async function main(): Promise<void> {
  for (const row of PLATFORMS) {
    const existing = await db.select({ id: platform.id }).from(platform).where(eq(platform.code, row.code)).limit(1);

    if (existing.length === 0) {
      await db.insert(platform).values(row);
    }
  }

  await closeDb();
}

main().catch(async (error) => {
  console.error(error);
  await closeDb();
  process.exitCode = 1;
});
