/** Format a numeric string as compact currency (e.g. $1.2M) */
export function formatUsd(value: string | number | null | undefined): string {
  if (value == null) return "—";
  const n = typeof value === "string" ? parseFloat(value) : value;
  if (!isFinite(n)) return "—";
  if (n >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(1)}B`;
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(2)}`;
}

/** Format a probability string (0–1) as a percentage */
export function formatPct(value: string | null | undefined): string {
  if (value == null) return "—";
  const n = parseFloat(value);
  if (!isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}%`;
}

/** Format a spread or delta as cents */
export function formatCents(value: string | null | undefined): string {
  if (value == null) return "—";
  const n = parseFloat(value);
  if (!isFinite(n)) return "—";
  return `${(n * 100).toFixed(1)}¢`;
}

/** Format a delta with sign and color class name */
export function formatDelta(value: string | null | undefined): {
  text: string;
  className: string;
} {
  if (value == null) return { text: "—", className: "text-[var(--text-tertiary)]" };
  const n = parseFloat(value);
  if (!isFinite(n)) return { text: "—", className: "text-[var(--text-tertiary)]" };
  const sign = n > 0 ? "+" : "";
  const pct = (n * 100).toFixed(1);
  if (n > 0) return { text: `${sign}${pct}%`, className: "text-[var(--color-success)]" };
  if (n < 0) return { text: `${pct}%`, className: "text-[var(--color-error)]" };
  return { text: `${pct}%`, className: "text-[var(--text-tertiary)]" };
}

/** ISO timestamp → relative label ("2m ago", "1h ago", etc.) */
export function relativeTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const diff = Date.now() - new Date(iso).getTime();
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  const days = Math.floor(hrs / 24);
  return `${days}d ago`;
}

/** ISO timestamp → short UTC format */
export function formatTs(iso: string | null | undefined): string {
  if (!iso) return "—";
  return new Date(iso).toLocaleString("en-US", {
    timeZone: "UTC",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }) + " UTC";
}

/** Derive display status, forcing "closed" once cutoff time has passed. */
export function effectiveStatus(
  status: string | null | undefined,
  cutoffIso: string | null | undefined,
  nowMs = Date.now(),
): string | null {
  const normalized = typeof status === "string" ? status.trim().toLowerCase() : null;
  if (!normalized && !cutoffIso) {
    return null;
  }

  if (cutoffIso) {
    const cutoffMs = Date.parse(cutoffIso);
    if (!Number.isNaN(cutoffMs) && nowMs >= cutoffMs && normalized !== "archived") {
      return "closed";
    }
  }

  return normalized;
}

/** Shared status badge classes for active vs non-active states. */
export function statusBadgeClass(status: string | null | undefined): string {
  return status === "active"
    ? "bg-[var(--color-success)]/10 text-[var(--color-success)]"
    : "bg-[var(--bg-surface)] text-[var(--text-tertiary)]";
}

/** Provider code → display name */
export function providerLabel(code: string): string {
  switch (code) {
    case "polymarket": return "Polymarket";
    case "kalshi": return "Kalshi";
    default: return code;
  }
}
