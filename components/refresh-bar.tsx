"use client";

import { useRouter } from "next/navigation";
import { relativeTime } from "@/lib/utils/format";

export function RefreshBar({ timestamp }: { timestamp: string }) {
  const router = useRouter();

  return (
    <div className="flex items-center gap-3 text-xs text-[var(--text-tertiary)]">
      <span>Updated {relativeTime(timestamp)}</span>
      <button
        onClick={() => router.refresh()}
        className="rounded bg-[var(--bg-card)] px-2 py-1 text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)] transition-colors"
      >
        Refresh
      </button>
    </div>
  );
}
