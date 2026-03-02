import { logger } from "./logger.js";

const RETRYABLE_STATUS = new Set([408, 429, 500, 502, 503, 504]);
const rateLimitCooldownByOrigin = new Map<string, number>();

const sleep = async (ms: number, signal?: AbortSignal): Promise<void> => {
  if (ms <= 0) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    const timeoutId = setTimeout(() => {
      cleanup();
      resolve();
    }, ms);

    const onAbort = () => {
      cleanup();
      reject(new Error("Aborted"));
    };

    const cleanup = () => {
      clearTimeout(timeoutId);
      signal?.removeEventListener("abort", onAbort);
    };

    if (signal) {
      if (signal.aborted) {
        cleanup();
        reject(new Error("Aborted"));
        return;
      }
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
};

function getOrigin(url: string): string | null {
  try {
    return new URL(url).origin;
  } catch {
    return null;
  }
}

function parseRetryAfterMs(value: string | null): number | null {
  if (!value) {
    return null;
  }

  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric >= 0) {
    return Math.floor(numeric * 1000);
  }

  const dateMs = Date.parse(value);
  if (Number.isFinite(dateMs)) {
    return Math.max(0, dateMs - Date.now());
  }

  return null;
}

function withJitter(delayMs: number, jitterRatio = 0.25): number {
  const min = Math.max(0, 1 - jitterRatio);
  const max = 1 + jitterRatio;
  const multiplier = min + Math.random() * (max - min);
  return Math.max(1, Math.floor(delayMs * multiplier));
}

function computeRetryDelayMs(params: {
  attempt: number;
  baseDelayMs: number;
  maxDelayMs: number;
  status: number | null;
  retryAfterMs: number | null;
}): number {
  const exponentialDelay = Math.min(params.maxDelayMs, params.baseDelayMs * 2 ** Math.max(0, params.attempt - 1));
  const baselineDelay = params.status === 429 ? Math.max(1000, exponentialDelay) : exponentialDelay;
  const delayWithRetryAfter =
    params.retryAfterMs === null ? baselineDelay : Math.max(baselineDelay, Math.min(params.retryAfterMs, params.maxDelayMs));
  return withJitter(delayWithRetryAfter);
}

async function waitForRateLimitCooldown(url: string, signal?: AbortSignal): Promise<void> {
  const origin = getOrigin(url);
  if (!origin) {
    return;
  }

  const cooldownUntil = rateLimitCooldownByOrigin.get(origin);
  if (!cooldownUntil) {
    return;
  }

  const now = Date.now();
  if (cooldownUntil <= now) {
    rateLimitCooldownByOrigin.delete(origin);
    return;
  }

  await sleep(cooldownUntil - now, signal);
}

function setRateLimitCooldown(url: string, delayMs: number): void {
  const origin = getOrigin(url);
  if (!origin || delayMs <= 0) {
    return;
  }

  const nextUntil = Date.now() + delayMs;
  const currentUntil = rateLimitCooldownByOrigin.get(origin) ?? 0;
  if (nextUntil > currentUntil) {
    rateLimitCooldownByOrigin.set(origin, nextUntil);
  }
}

export async function fetchJsonWithRetry<T>(
  url: string,
  init?: RequestInit,
  options?: {
    maxAttempts?: number;
    baseDelayMs?: number;
    maxDelayMs?: number;
    extraRateLimitAttempts?: number;
    signal?: AbortSignal;
    logRetries?: boolean;
  }
): Promise<T> {
  const maxAttempts = options?.maxAttempts ?? 5;
  const baseDelayMs = options?.baseDelayMs ?? 500;
  const maxDelayMs = options?.maxDelayMs ?? 15_000;
  const extraRateLimitAttempts = options?.extraRateLimitAttempts ?? 4;
  const logRetries = options?.logRetries ?? true;
  const maxTotalAttempts = maxAttempts + extraRateLimitAttempts;

  for (let attempt = 1; attempt <= maxTotalAttempts; attempt += 1) {
    await waitForRateLimitCooldown(url, options?.signal);

    let response: Response;
    try {
      response = await fetch(url, {
        ...init,
        headers: {
          Accept: "application/json",
          ...(init?.headers ?? {})
        },
        signal: options?.signal
      });
    } catch (error) {
      if (attempt >= maxAttempts) {
        throw error;
      }

      const delay = computeRetryDelayMs({
        attempt,
        baseDelayMs,
        maxDelayMs,
        status: null,
        retryAfterMs: null
      });
      if (logRetries) {
        logger.warn({ url, attempt, delay, error }, "Request failed, retrying");
      }
      await sleep(delay, options?.signal);
      continue;
    }

    if (response.ok) {
      return (await response.json()) as T;
    }

    const body = await response.text();
    const isRetryable = RETRYABLE_STATUS.has(response.status);
    const allowedAttempts = response.status === 429 ? maxAttempts + extraRateLimitAttempts : maxAttempts;
    const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));

    if (isRetryable && attempt < allowedAttempts) {
      const delay = computeRetryDelayMs({
        attempt,
        baseDelayMs,
        maxDelayMs,
        status: response.status,
        retryAfterMs
      });

      if (response.status === 429) {
        setRateLimitCooldown(url, delay);
      }

      if (logRetries) {
        logger.warn(
          {
            url,
            status: response.status,
            attempt,
            allowedAttempts,
            delay,
            retryAfterMs,
            body: body.slice(0, 500)
          },
          "Retryable HTTP error"
        );
      }

      await sleep(delay, options?.signal);
      continue;
    }

    throw new Error(`HTTP ${response.status} for ${url}: ${body.slice(0, 500)}`);
  }

  throw new Error(`Unexpected request loop exit for ${url}`);
}
