import { eq, sql } from "drizzle-orm";

import { db } from "../db/client.js";
import { jobRunLog } from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";

export interface JobRunContext {
  providerCode: ProviderCode;
  jobName: string;
  requestId?: string;
}

export interface JobRunResult {
  rowsUpserted: number;
  rowsSkipped: number;
  partialReason?: string | null;
}

export async function startJobRun(context: JobRunContext): Promise<number> {
  const rows = await db
    .insert(jobRunLog)
    .values({
      requestId: context.requestId,
      providerCode: context.providerCode,
      jobName: context.jobName,
      status: "running"
    })
    .returning({ id: jobRunLog.id });

  const id = rows[0]?.id;
  if (!id) {
    throw new Error("Failed to create job_run_log row");
  }
  return id;
}

export async function finishJobRunSuccess(id: number, result: JobRunResult): Promise<void> {
  await db
    .update(jobRunLog)
    .set({
      status: result.partialReason ? "partial_success" : "success",
      finishedAt: new Date(),
      rowsUpserted: result.rowsUpserted,
      rowsSkipped: result.rowsSkipped,
      errorText: result.partialReason ?? null
    })
    .where(eq(jobRunLog.id, id));
}

export async function finishJobRunFailure(id: number, error: unknown): Promise<void> {
  const rawErrorText = error instanceof Error ? error.message : "Unknown error";
  const errorText = rawErrorText.slice(0, 4000);

  await db
    .update(jobRunLog)
    .set({
      status: "failed",
      finishedAt: new Date(),
      errorText
    })
    .where(eq(jobRunLog.id, id));
}

export async function runLoggedJob<T extends JobRunResult>(
  context: JobRunContext,
  callback: () => Promise<T>
): Promise<T> {
  const runId = await startJobRun(context);

  try {
    const result = await callback();
    await finishJobRunSuccess(runId, result);
    return result;
  } catch (error) {
    await finishJobRunFailure(runId, error);
    throw error;
  }
}

export async function recoverStaleRunningJobRuns(staleAfterMs: number): Promise<number> {
  const staleAfterSeconds = Math.max(1, Math.floor(staleAfterMs / 1000));
  const recoveredAt = new Date().toISOString();

  const result = await db.execute(sql`
    update ops.job_run_log
    set
      status = 'failed',
      finished_at = now(),
      error_text = left(
        concat(
          'Recovered stale running job during worker startup at ',
          ${recoveredAt}::text
        ),
        4000
      )
    where status = 'running'
      and finished_at is null
      and started_at < now() - (${staleAfterSeconds} * interval '1 second')
  `);

  return Number(result.rowCount ?? 0);
}
