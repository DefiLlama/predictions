import { formatPct, formatCents, formatDelta } from "@/lib/utils/format";
import type { InstrumentSnapshot, DashboardInstrument } from "@/lib/api/types";

type Instrument = InstrumentSnapshot | DashboardInstrument;

function hasDelta(
  inst: Instrument,
): inst is DashboardInstrument {
  return "delta24h" in inst;
}

export function InstrumentTable({
  instruments,
}: {
  instruments: Instrument[];
}) {
  if (instruments.length === 0) return null;

  const showDelta = instruments.some((i) => hasDelta(i));

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-left text-[10px] uppercase tracking-wider text-[var(--text-tertiary)]">
            <th className="pb-1.5 pr-4 font-medium">Outcome</th>
            <th className="pb-1.5 pr-4 font-medium text-right">Price</th>
            {showDelta && (
              <th className="pb-1.5 pr-4 font-medium text-right">24h</th>
            )}
            <th className="pb-1.5 pr-4 font-medium text-right">Bid</th>
            <th className="pb-1.5 pr-4 font-medium text-right">Ask</th>
            <th className="pb-1.5 font-medium text-right">Spread</th>
          </tr>
        </thead>
        <tbody>
          {instruments.map((inst) => {
            const delta = hasDelta(inst) ? formatDelta(inst.delta24h) : null;
            return (
              <tr
                key={inst.instrumentRef}
                className="border-t border-[var(--bg-border)]/30"
              >
                <td className="py-1.5 pr-4 text-[var(--text-secondary)]">
                  {inst.outcomeLabel ?? inst.instrumentRef}
                </td>
                <td className="py-1.5 pr-4 text-right font-mono tabular-nums font-medium text-[var(--text-primary)]">
                  {formatPct(inst.latestPrice)}
                </td>
                {showDelta && (
                  <td className={`py-1.5 pr-4 text-right font-mono tabular-nums ${delta?.className ?? ""}`}>
                    {delta?.text ?? "\u2014"}
                  </td>
                )}
                <td className="py-1.5 pr-4 text-right font-mono tabular-nums text-[var(--text-tertiary)]">
                  {formatPct(inst.bestBid)}
                </td>
                <td className="py-1.5 pr-4 text-right font-mono tabular-nums text-[var(--text-tertiary)]">
                  {formatPct(inst.bestAsk)}
                </td>
                <td className="py-1.5 text-right font-mono tabular-nums text-[var(--text-tertiary)]">
                  {formatCents(inst.spread)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
