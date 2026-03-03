import { eq, sql } from "drizzle-orm";

import { db } from "../db/client.js";
import { jobRunLog } from "../db/schema.js";
import type { ProviderCode } from "../types/domain.js";
import { logger } from "../utils/logger.js";

export interface JobRunContext {
  providerCode: ProviderCode;
  jobName: string;
  requestId?: string;
}

export interface JobRunResult {
  rowsUpserted: number;
  rowsSkipped: number;
  partialReason?: string | null;
  continueSameStep?: boolean;
}

const JOB_LOG_DB_RETRY_MAX_ATTEMPTS = 2;
const JOB_LOG_DB_RETRY_BACKOFF_MS = 500;

const RETRYABLE_DB_ERROR_CODES = new Set(["08000", "08001", "08003", "08006", "57P01", "57P02", "57P03"]);
const RETRYABLE_DB_ERROR_PATTERNS = [
  "connection terminated unexpectedly",
  "connection ended unexpectedly",
  "connection reset by peer",
  "terminating connection due to administrator command",
  "the database system is shutting down",
  "econnreset",
  "econnrefused",
  "etimedout"
];

function isRetryableDbError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const pgCode = (error as Error & { code?: unknown }).code;
  if (typeof pgCode === "string" && RETRYABLE_DB_ERROR_CODES.has(pgCode)) {
    return true;
  }

  const message = error.message.toLowerCase();
  if (RETRYABLE_DB_ERROR_PATTERNS.some((pattern) => message.includes(pattern))) {
    return true;
  }

  const cause = (error as Error & { cause?: unknown }).cause;
  if (cause instanceof Error) {
    return isRetryableDbError(cause);
  }

  return false;
}

async function runJobLogDbWrite<T>(operation: string, callback: () => Promise<T>): Promise<T> {
  let attempt = 0;

  while (true) {
    try {
      return await callback();
    } catch (error) {
      const retryable = isRetryableDbError(error) && attempt < JOB_LOG_DB_RETRY_MAX_ATTEMPTS;
      if (!retryable) {
        throw error;
      }

      attempt += 1;
      const backoffMs = JOB_LOG_DB_RETRY_BACKOFF_MS * attempt;
      logger.warn({ operation, attempt, backoffMs, error }, "Retrying job log DB write after transient error");
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
    }
  }
}

export async function startJobRun(context: JobRunContext): Promise<number> {
  const rows = await runJobLogDbWrite("startJobRun", () =>
    db
      .insert(jobRunLog)
      .values({
        requestId: context.requestId,
        providerCode: context.providerCode,
        jobName: context.jobName,
        status: "running"
      })
      .returning({ id: jobRunLog.id })
  );

  const id = rows[0]?.id;
  if (!id) {
    throw new Error("Failed to create job_run_log row");
  }
  return id;
}

export async function finishJobRunSuccess(id: number, result: JobRunResult): Promise<void> {
  const isPartial = Boolean(result.partialReason) || Boolean(result.continueSameStep);

  await runJobLogDbWrite("finishJobRunSuccess", () =>
    db
      .update(jobRunLog)
      .set({
        status: isPartial ? "partial_success" : "success",
        finishedAt: new Date(),
        rowsUpserted: result.rowsUpserted,
        rowsSkipped: result.rowsSkipped,
        errorText: result.partialReason ?? (result.continueSameStep ? "step_in_progress_resume_required" : null)
      })
      .where(eq(jobRunLog.id, id))
  );
}

export async function finishJobRunFailure(id: number, error: unknown): Promise<void> {
  const rawErrorText = error instanceof Error ? error.message : "Unknown error";
  const errorText = rawErrorText.slice(0, 4000);

  await runJobLogDbWrite("finishJobRunFailure", () =>
    db
      .update(jobRunLog)
      .set({
        status: "failed",
        finishedAt: new Date(),
        errorText
      })
      .where(eq(jobRunLog.id, id))
  );
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
    try {
      await finishJobRunFailure(runId, error);
    } catch (finishError) {
      logger.error({ runId, finishError, originalError: error }, "Failed to mark job run as failed");
    }
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
          'Recovered stale running job during process startup at ',
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
