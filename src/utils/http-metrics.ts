import { AsyncLocalStorage } from "node:async_hooks";

interface HttpMetricsStore {
  requestId: string | null;
  requests: number;
  attempts: number;
  retries: number;
  successfulRequests: number;
  failedRequests: number;
  rateLimitedResponses: number;
}

export interface HttpMetricsSnapshot {
  requestId: string | null;
  requests: number;
  attempts: number;
  retries: number;
  successfulRequests: number;
  failedRequests: number;
  rateLimitedResponses: number;
}

type UnknownErrorWithMetrics = Error & { httpMetrics?: HttpMetricsSnapshot };

const storage = new AsyncLocalStorage<HttpMetricsStore>();

function getStore(): HttpMetricsStore | null {
  return storage.getStore() ?? null;
}

function toSnapshot(store: HttpMetricsStore): HttpMetricsSnapshot {
  return {
    requestId: store.requestId,
    requests: store.requests,
    attempts: store.attempts,
    retries: store.retries,
    successfulRequests: store.successfulRequests,
    failedRequests: store.failedRequests,
    rateLimitedResponses: store.rateLimitedResponses
  };
}

function attachMetrics(error: unknown, metrics: HttpMetricsSnapshot): unknown {
  if (error instanceof Error) {
    (error as UnknownErrorWithMetrics).httpMetrics = metrics;
    return error;
  }

  const wrapped = new Error(String(error)) as UnknownErrorWithMetrics;
  wrapped.httpMetrics = metrics;
  return wrapped;
}

export function getHttpMetricsSnapshot(): HttpMetricsSnapshot | null {
  const store = getStore();
  return store ? toSnapshot(store) : null;
}

export function getHttpMetricsFromError(error: unknown): HttpMetricsSnapshot | null {
  if (!(error instanceof Error)) {
    return null;
  }

  const metrics = (error as UnknownErrorWithMetrics).httpMetrics;
  return metrics ?? null;
}

export async function runWithHttpMetrics<T>(
  requestId: string | null | undefined,
  callback: () => Promise<T>
): Promise<{ result: T; metrics: HttpMetricsSnapshot }> {
  const initial: HttpMetricsStore = {
    requestId: requestId ?? null,
    requests: 0,
    attempts: 0,
    retries: 0,
    successfulRequests: 0,
    failedRequests: 0,
    rateLimitedResponses: 0
  };

  return storage.run(initial, async () => {
    const store = getStore();
    if (!store) {
      const result = await callback();
      return {
        result,
        metrics: {
          requestId: requestId ?? null,
          requests: 0,
          attempts: 0,
          retries: 0,
          successfulRequests: 0,
          failedRequests: 0,
          rateLimitedResponses: 0
        }
      };
    }

    try {
      const result = await callback();
      return { result, metrics: toSnapshot(store) };
    } catch (error) {
      throw attachMetrics(error, toSnapshot(store));
    }
  });
}

export function recordHttpRequestStarted(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.requests += 1;
}

export function recordHttpAttempt(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.attempts += 1;
}

export function recordHttpRetry(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.retries += 1;
}

export function recordRateLimitedResponse(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.rateLimitedResponses += 1;
}

export function recordHttpRequestSucceeded(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.successfulRequests += 1;
}

export function recordHttpRequestFailed(): void {
  const store = getStore();
  if (!store) {
    return;
  }
  store.failedRequests += 1;
}
