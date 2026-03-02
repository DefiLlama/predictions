import { eq } from "drizzle-orm";

import { db } from "../db/client.js";
import { platform } from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";

export async function getPlatformOrThrow(providerCode: ProviderCode): Promise<{ id: number; code: string }> {
  const rows = await db
    .select({ id: platform.id, code: platform.code })
    .from(platform)
    .where(eq(platform.code, providerCode))
    .limit(1);

  const row = rows[0];
  if (!row) {
    throw new Error(`Platform not found: ${providerCode}. Did you run db:seed?`);
  }

  return row;
}
