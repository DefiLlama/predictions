"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";

const statuses = [
  { value: "active", label: "Active" },
  { value: "all", label: "All" },
] as const;

export function MarketStatusFilter() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const current = searchParams.get("status") === "all" ? "all" : "active";

  function select(value: "active" | "all") {
    const params = new URLSearchParams(searchParams.toString());
    params.set("status", value);
    params.delete("page");
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="flex items-center rounded-md border border-[var(--bg-border)] bg-[var(--bg-surface)] p-0.5">
      {statuses.map(({ value, label }) => (
        <button
          key={value}
          onClick={() => select(value)}
          className={`rounded px-3 py-1 text-xs font-medium transition-all ${
            current === value
              ? "bg-[var(--color-primary)] text-[var(--bg-app)] shadow-sm"
              : "text-[var(--text-tertiary)] hover:text-[var(--text-primary)]"
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
