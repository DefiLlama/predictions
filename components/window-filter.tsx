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
    <div className="flex items-center gap-1">
      {windows.map(({ value, label }) => (
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
