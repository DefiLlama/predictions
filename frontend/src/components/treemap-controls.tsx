"use client";

import { useRouter, useSearchParams, usePathname } from "next/navigation";

const metrics = [
  { value: "volume24h", label: "Volume" },
  { value: "liquidity", label: "Liquidity" },
];

export function TreemapControls() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();

  const current = {
    metric: searchParams.get("metric") ?? "volume24h",
  };

  function set(key: string, value: string) {
    const params = new URLSearchParams(searchParams.toString());
    params.set(key, value);
    router.push(`${pathname}?${params.toString()}`);
  }

  return (
    <div className="flex flex-wrap items-center gap-4 text-xs">
      <ToggleGroup
        label="Metric"
        options={metrics}
        value={current.metric}
        onChange={(v) => set("metric", v)}
      />
    </div>
  );
}

function ToggleGroup({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: { value: string; label: string }[];
  value: string;
  onChange: (v: string) => void;
}) {
  return (
    <div className="flex items-center gap-1.5">
      <span className="text-[var(--text-tertiary)]">{label}:</span>
      {options.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={`rounded px-2 py-1 transition-colors ${
            value === opt.value
              ? "bg-[var(--color-primary)] text-white"
              : "bg-[var(--bg-card)] text-[var(--text-secondary)] hover:bg-[var(--bg-card-hover)]"
          }`}
        >
          {opt.label}
        </button>
      ))}
    </div>
  );
}
