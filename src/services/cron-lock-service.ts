import type { PoolClient } from "pg";

import { sqlPool } from "../db/client.js";
import { logger } from "../utils/logger.js";

const LOCK_HEARTBEAT_INTERVAL_MS = 15_000;

export interface CronLockHandle {
  key: bigint;
  client: PoolClient;
  heartbeat: NodeJS.Timeout;
  onError: (error: Error) => void;
}

interface TryAcquireLockOptions {
  timeoutMs?: number;
  pollIntervalMs?: number;
}

async function tryAcquireOnClient(client: PoolClient, key: bigint): Promise<boolean> {
  const result = await client.query<{ acquired: boolean }>("select pg_try_advisory_lock($1::bigint) as acquired", [
    key.toString()
  ]);

  return result.rows[0]?.acquired === true;
}

function startLockHeartbeat(client: PoolClient, key: bigint): NodeJS.Timeout {
  let heartbeatInFlight = false;

  return setInterval(() => {
    if (heartbeatInFlight) {
      return;
    }

    heartbeatInFlight = true;
    void client
      .query("select 1")
      .catch((error) => {
        logger.warn({ lockKey: key.toString(), error }, "Cron lock heartbeat failed");
      })
      .finally(() => {
        heartbeatInFlight = false;
      });
  }, LOCK_HEARTBEAT_INTERVAL_MS);
}

export async function tryAcquireLock(key: bigint, options?: TryAcquireLockOptions): Promise<CronLockHandle | null> {
  const client = await sqlPool.connect();
  const onError = (error: Error): void => {
    logger.error({ lockKey: key.toString(), error }, "Cron lock client emitted error");
  };

  client.on("error", onError);

  const timeoutMs = Math.max(0, options?.timeoutMs ?? 0);
  const pollIntervalMs = Math.max(50, options?.pollIntervalMs ?? 250);
  const startedAt = Date.now();

  try {
    while (true) {
      const acquired = await tryAcquireOnClient(client, key);
      if (acquired) {
        const heartbeat = startLockHeartbeat(client, key);
        return { key, client, heartbeat, onError };
      }

      if (Date.now() - startedAt >= timeoutMs) {
        client.off("error", onError);
        client.release();
        return null;
      }

      await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    }
  } catch (error) {
    client.off("error", onError);
    client.release();
    throw error;
  }
}

export async function releaseLock(lock: CronLockHandle): Promise<void> {
  try {
    clearInterval(lock.heartbeat);
    lock.client.off("error", lock.onError);

    try {
      await lock.client.query("select pg_advisory_unlock($1::bigint)", [lock.key.toString()]);
    } catch (error) {
      logger.warn({ lockKey: lock.key.toString(), error }, "Failed to unlock advisory lock cleanly");
    }
  } finally {
    lock.client.release();
  }
}
