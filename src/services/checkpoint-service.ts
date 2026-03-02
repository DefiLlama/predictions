import { and, eq } from "drizzle-orm";

import { db } from "../db/client.js";
import { ingestCheckpoint } from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";

export async function getCheckpoint(providerCode: ProviderCode, jobName: string): Promise<Record<string, unknown> | null> {
  const rows = await db
    .select({ cursorJson: ingestCheckpoint.cursorJson })
    .from(ingestCheckpoint)
    .where(and(eq(ingestCheckpoint.providerCode, providerCode), eq(ingestCheckpoint.jobName, jobName)))
    .limit(1);

  return rows[0]?.cursorJson ?? null;
}

export async function setCheckpoint(providerCode: ProviderCode, jobName: string, cursorJson: Record<string, unknown>): Promise<void> {
  await db
    .insert(ingestCheckpoint)
    .values({ providerCode, jobName, cursorJson, updatedAt: new Date() })
    .onConflictDoUpdate({
      target: [ingestCheckpoint.providerCode, ingestCheckpoint.jobName],
      set: {
        cursorJson,
        updatedAt: new Date()
      }
    });
}
