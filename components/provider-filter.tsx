"use client";

import { useRouter, useSearchParams, usePathname } from "next/navigation";

const providers = [
  { value: "", label: "All" },
  { value: "polymarket", label: "Polymarket" },
  { value: "kalshi", label: "Kalshi" },
];

export function ProviderFilter() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const current = searchParams.get("provider") ?? "";

  function select(value: string) {
    const params = new URLSearchParams(searchParams.toString());
    if (value) {
      params.set("provider", value);
    } else {
      params.delete("provider");
    }
    params.delete("page");
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="flex items-center rounded-md border border-[var(--bg-border)] bg-[var(--bg-surface)] p-0.5">
      {providers.map(({ value, label }) => (
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
