import { PgBoss, type Job } from "pg-boss";

import { env } from "../config/env.js";
import { recoverStaleRunningJobRuns } from "../services/job-log-service.js";
import { logger } from "../utils/logger.js";
import { ACTIVE_WORKER_JOB_NAMES, ALL_JOB_NAMES, JOB_NAMES, type JobName } from "./names.js";
import { jobHandlers } from "./handlers.js";
import type { JobPayload } from "./types.js";

const queueNameByJobName: Record<JobName, string> = Object.fromEntries(
  ALL_JOB_NAMES.map((jobName) => [jobName, jobName.replaceAll(":", "__")])
) as Record<JobName, string>;

export interface WorkerRuntime {
  boss: PgBoss;
  stop: () => Promise<void>;
}

interface SchedulerDefinition {
  jobName: JobName;
  key: string;
  intervalMs: number;
  payload: JobPayload;
}

function intervalMsToCron(intervalMs: number): string {
  if (intervalMs % (60 * 1000) !== 0) {
    throw new Error(`Scheduler intervals must be whole-minute values. Received ${intervalMs}ms`);
  }

  const totalMinutes = intervalMs / (60 * 1000);
  if (totalMinutes < 1) {
    throw new Error(`Scheduler intervals must be at least 1 minute. Received ${intervalMs}ms`);
  }

  if (totalMinutes <= 59) {
    return `*/${totalMinutes} * * * *`;
  }

  if (totalMinutes % 60 === 0) {
    const totalHours = totalMinutes / 60;
    if (totalHours <= 23) {
      return `0 */${totalHours} * * *`;
    }
    if (totalHours === 24) {
      return "0 0 * * *";
    }
  }

  throw new Error(
    `Unsupported scheduler interval ${intervalMs}ms. Use minute values under 60m or hour-aligned values up to 24h.`
  );
}

function buildScheduleDefinitions(): SchedulerDefinition[] {
  return [
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES,
      key: "active:polymarket:sync:prices",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK,
      key: "active:polymarket:sync:orderbook",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES,
      key: "active:polymarket:sync:trades",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_OI,
      key: "active:polymarket:sync:oi",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_PRICES,
      key: "active:kalshi:sync:prices",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK,
      key: "active:kalshi:sync:orderbook",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_TRADES,
      key: "active:kalshi:sync:trades",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_OI,
      key: "active:kalshi:sync:oi",
      intervalMs: env.ACTIVE_POLL_INTERVAL_MS,
      payload: { scopeStatus: "active" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_PRICES,
      key: "closed:polymarket:sync:prices",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_ORDERBOOK,
      key: "closed:polymarket:sync:orderbook",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_TRADES,
      key: "closed:polymarket:sync:trades",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_OI,
      key: "closed:polymarket:sync:oi",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.POLYMARKET_SYNC_METADATA,
      key: "closed:polymarket:sync:metadata",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: {}
    },
    {
      jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
      key: "closed:polymarket:market:relink:events",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.SCOPE_REBUILD,
      key: "closed:polymarket:scope:rebuild",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
      key: "closed:polymarket:analytics:category:assign:markets",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
      key: "closed:polymarket:analytics:rollup:price:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
      key: "closed:polymarket:analytics:rollup:liquidity:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
      key: "closed:polymarket:analytics:rollup:provider-category:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "polymarket" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_PRICES,
      key: "closed:kalshi:sync:prices",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_ORDERBOOK,
      key: "closed:kalshi:sync:orderbook",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_TRADES,
      key: "closed:kalshi:sync:trades",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_OI,
      key: "closed:kalshi:sync:oi",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { scopeStatus: "closed" }
    },
    {
      jobName: JOB_NAMES.KALSHI_SYNC_METADATA,
      key: "closed:kalshi:sync:metadata",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: {}
    },
    {
      jobName: JOB_NAMES.MARKET_RELINK_EVENTS,
      key: "closed:kalshi:market:relink:events",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    },
    {
      jobName: JOB_NAMES.SCOPE_REBUILD,
      key: "closed:kalshi:scope:rebuild",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    },
    {
      jobName: JOB_NAMES.CATEGORY_ASSIGN_MARKETS,
      key: "closed:kalshi:analytics:category:assign:markets",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_PRICE_1H,
      key: "closed:kalshi:analytics:rollup:price:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_LIQUIDITY_1H,
      key: "closed:kalshi:analytics:rollup:liquidity:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    },
    {
      jobName: JOB_NAMES.ANALYTICS_ROLLUP_PROVIDER_CATEGORY_1H,
      key: "closed:kalshi:analytics:rollup:provider-category:1h",
      intervalMs: env.CLOSED_POLL_INTERVAL_MS,
      payload: { providerCode: "kalshi" }
    }
  ];
}

export async function createBoss(options?: { enableScheduleProcessing?: boolean }): Promise<PgBoss> {
  const boss = new PgBoss({
    connectionString: env.DATABASE_URL,
    schema: env.PG_BOSS_SCHEMA,
    schedule: options?.enableScheduleProcessing ?? false
  });

  boss.on("error", (error) => {
    logger.error({ error }, "pg-boss error");
  });

  await boss.start();
  return boss;
}

async function registerWorkers(boss: PgBoss): Promise<void> {
  for (const jobName of ACTIVE_WORKER_JOB_NAMES) {
    const queueName = queueNameByJobName[jobName];
    const queue = await boss.getQueue(queueName);
    if (!queue) {
      await boss.createQueue(queueName);
    }

    await boss.work(queueName, async (jobs) => {
      for (const job of jobs as Array<Job<JobPayload>>) {
        const handler = jobHandlers[jobName];
        if (!handler) {
          logger.error({ jobName }, "No handler registered");
          continue;
        }

        await handler((job.data ?? {}) as JobPayload);
      }
    });
  }
}

async function registerSchedules(boss: PgBoss): Promise<void> {
  const schedules = buildScheduleDefinitions();

  for (const schedule of schedules) {
    const queueName = queueNameByJobName[schedule.jobName];
    const cron = intervalMsToCron(schedule.intervalMs);
    const singletonSeconds = Math.max(60, Math.ceil(schedule.intervalMs / 1000));

    await boss.schedule(queueName, cron, schedule.payload, {
      key: schedule.key,
      singletonKey: schedule.key,
      singletonSeconds
    });
  }
}

export async function startWorkerRuntime(options?: { enableScheduler?: boolean }): Promise<WorkerRuntime> {
  const enableScheduler = options?.enableScheduler ?? false;
  const boss = await createBoss({ enableScheduleProcessing: enableScheduler });
  const recoveredCount = await recoverStaleRunningJobRuns(env.JOB_RUN_STALE_AFTER_MS);

  if (recoveredCount > 0) {
    logger.warn(
      { recoveredCount, staleAfterMs: env.JOB_RUN_STALE_AFTER_MS },
      "Recovered stale running jobs from previous worker lifecycle"
    );
  }

  await registerWorkers(boss);
  if (enableScheduler) {
    await registerSchedules(boss);
  }

  return {
    boss,
    stop: async () => {
      await boss.stop({ graceful: true });
    }
  };
}

export async function enqueueJob(boss: PgBoss, jobName: JobName, payload?: JobPayload): Promise<string> {
  const queueName = queueNameByJobName[jobName];
  const jobId = await boss.send(queueName, payload ?? {});

  if (!jobId) {
    throw new Error(`Failed to enqueue ${jobName}`);
  }

  return jobId;
}
