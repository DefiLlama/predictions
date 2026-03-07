"use client";

import { useRouter, useSearchParams, usePathname } from "next/navigation";

export function Pagination({
  page,
  hasNext,
}: {
  page: number;
  hasNext: boolean;
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  function go(p: number) {
    const params = new URLSearchParams(searchParams.toString());
    if (p <= 1) {
      params.delete("page");
    } else {
      params.set("page", String(p));
    }
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="flex items-center justify-center gap-3 pt-4">
      <button
        disabled={page <= 1}
        onClick={() => go(page - 1)}
        className="rounded-md border border-[var(--bg-border)] bg-[var(--bg-card)] px-4 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)] hover:text-[var(--text-primary)] disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
      >
        &larr; Prev
      </button>
      <span className="text-xs font-mono tabular-nums text-[var(--text-tertiary)]">
        {page}
      </span>
      <button
        disabled={!hasNext}
        onClick={() => go(page + 1)}
        className="rounded-md border border-[var(--bg-border)] bg-[var(--bg-card)] px-4 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)] hover:text-[var(--text-primary)] disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
      >
        Next &rarr;
      </button>
    </div>
  );
}
