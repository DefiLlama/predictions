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
    <div className="flex items-center gap-1">
      {statuses.map(({ value, label }) => (
        <button
          key={value}
          onClick={() => select(value)}
          className={`rounded-md px-3 py-1.5 text-xs font-medium transition-colors ${
            current === value
              ? "bg-[var(--color-primary)] text-white"
              : "bg-[var(--bg-card)] text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)]"
          }`}
        >
          {label}
        </button>
      ))}
    </div>
  );
}
