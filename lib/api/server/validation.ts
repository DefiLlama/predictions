import type { ProviderCode } from "@/src/types/domain";

export function isProviderCode(value: string | undefined): value is ProviderCode {
  return value === "polymarket" || value === "kalshi";
}

export function parsePagination(
  limitRaw: string | null,
  offsetRaw: string | null,
  defaults: { defaultLimit: number; maxLimit: number },
): { limit: number; offset: number } | null {
  const limit = Math.min(Number(limitRaw ?? String(defaults.defaultLimit)), defaults.maxLimit);
  const offset = Number(offsetRaw ?? "0");

  if (!Number.isFinite(limit) || limit <= 0 || !Number.isFinite(offset) || offset < 0) {
    return null;
  }

  return { limit, offset };
}

export function parseOptionalPositiveInt(
  valueRaw: string | null,
  options?: { max?: number },
): number | undefined | null {
  if (valueRaw === null) {
    return undefined;
  }

  const value = Number(valueRaw);
  if (
    !Number.isInteger(value) ||
    value <= 0 ||
    (options?.max !== undefined && value > options.max)
  ) {
    return null;
  }

  return value;
}

export function parseBooleanFlag(valueRaw: string | null): boolean {
  return valueRaw === "1" || valueRaw === "true";
}

export function parseIntervalRange(params: {
  intervalRaw: string | null;
  fromRaw: string | null;
  toRaw: string | null;
}): { from: Date; to: Date } | { error: string } {
  const interval = params.intervalRaw ?? "1h";
  if (interval !== "1h") {
    return { error: "Invalid interval. Only 1h is supported." };
  }

  const to = params.toRaw ? new Date(params.toRaw) : new Date();
  const from = params.fromRaw ? new Date(params.fromRaw) : new Date(to.getTime() - 7 * 24 * 60 * 60 * 1000);

  if (Number.isNaN(from.getTime()) || Number.isNaN(to.getTime())) {
    return { error: "Invalid from/to datetime. Use ISO-8601 timestamps." };
  }

  if (from > to) {
    return { error: "Invalid range: from must be less than or equal to to." };
  }

  return { from, to };
}
