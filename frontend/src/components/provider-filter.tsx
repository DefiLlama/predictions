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
    <div className="flex items-center gap-1">
      {providers.map(({ value, label }) => (
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
