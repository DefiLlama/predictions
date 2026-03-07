"use client";

import { useRouter, useSearchParams, usePathname } from "next/navigation";

const windows = [
  { value: "24h", label: "24h" },
  { value: "7d", label: "7d" },
  { value: "30d", label: "30d" },
];

export function WindowFilter() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const current = searchParams.get("window") ?? "24h";

  function select(value: string) {
    const params = new URLSearchParams(searchParams.toString());
    params.set("window", value);
    params.delete("page");
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="flex items-center rounded-md border border-[var(--bg-border)] bg-[var(--bg-surface)] p-0.5">
      {windows.map(({ value, label }) => (
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
