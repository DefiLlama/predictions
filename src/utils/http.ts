import { logger } from "./logger.js";

const RETRYABLE_STATUS = new Set([408, 429, 500, 502, 503, 504]);

const sleep = async (ms: number): Promise<void> => {
  await new Promise((resolve) => setTimeout(resolve, ms));
};

export async function fetchJsonWithRetry<T>(
  url: string,
  init?: RequestInit,
  options?: {
    maxAttempts?: number;
    baseDelayMs?: number;
    signal?: AbortSignal;
    logRetries?: boolean;
  }
): Promise<T> {
  const maxAttempts = options?.maxAttempts ?? 5;
  const baseDelayMs = options?.baseDelayMs ?? 500;
  const logRetries = options?.logRetries ?? true;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const response = await fetch(url, {
        ...init,
        headers: {
          Accept: "application/json",
          ...(init?.headers ?? {})
        },
        signal: options?.signal
      });

      if (!response.ok) {
        const body = await response.text();

        if (RETRYABLE_STATUS.has(response.status) && attempt < maxAttempts) {
          const delay = baseDelayMs * 2 ** (attempt - 1);
          if (logRetries) {
            logger.warn(
              {
                url,
                status: response.status,
                attempt,
                delay,
                body: body.slice(0, 500)
              },
              "Retryable HTTP error"
            );
          }
          await sleep(delay);
          continue;
        }

        throw new Error(`HTTP ${response.status} for ${url}: ${body.slice(0, 500)}`);
      }

      return (await response.json()) as T;
    } catch (error) {
      if (attempt >= maxAttempts) {
        throw error;
      }
      const delay = baseDelayMs * 2 ** (attempt - 1);
      if (logRetries) {
        logger.warn({ url, attempt, delay, error }, "Request failed, retrying");
      }
      await sleep(delay);
    }
  }

  throw new Error(`Unexpected request loop exit for ${url}`);
}
