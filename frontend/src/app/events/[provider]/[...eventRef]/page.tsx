import { notFound } from "next/navigation";
import Link from "next/link";
import { getEventDetail, getEventPriceHistory } from "@/lib/api/client";
import { RefreshBar } from "@/components/refresh-bar";
import { InstrumentTable } from "@/components/instrument-table";
import { PriceChart } from "@/components/price-chart";
import { EmptyState } from "@/components/empty-state";
import { formatUsd, providerLabel } from "@/lib/utils/format";
import { uidToPath } from "@/lib/utils/params";

export default async function EventDetailPage({
  params,
}: {
  params: Promise<{ provider: string; eventRef: string[] }>;
}) {
  const { provider, eventRef } = await params;
  const eventUid = `${provider}:${eventRef.join("/")}`;

  let res;
  try {
    res = await getEventDetail(eventUid);
  } catch {
    notFound();
  }

  const { event, markets } = res.data;

  let eventPriceHistory = null;
  try {
    const phRes = await getEventPriceHistory(eventUid);
    eventPriceHistory = phRes.data;
  } catch {
    // Price history may not be available
  }

  const topSeries = eventPriceHistory?.series.slice(0, 5) ?? [];
  const chartInstruments =
    topSeries.map((series) => ({
      instrumentRef: series.instrumentRef,
      outcomeLabel: series.marketDisplayTitle ?? series.marketTitle ?? series.marketRef,
      outcomeIndex: null,
      points: series.points,
    }));

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <div className="mb-2 flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
          <Link href="/events" className="hover:text-[var(--color-primary)]">
            Events
          </Link>
          <span>/</span>
          <span>{providerLabel(provider)}</span>
        </div>
        <div className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-xl font-semibold text-[var(--text-primary)]">
              {event.title ?? eventUid}
            </h1>
            <div className="mt-1 flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
              <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
                {providerLabel(event.providerCode)}
              </span>
              {event.category && (
                <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
                  {event.category}
                </span>
              )}
              {event.status && (
                <span
                  className={`rounded px-1.5 py-0.5 ${
                    event.status === "active"
                      ? "bg-[var(--color-success)]/10 text-[var(--color-success)]"
                      : "bg-[var(--bg-surface)]"
                  }`}
                >
                  {event.status}
                </span>
              )}
            </div>
          </div>
          <RefreshBar timestamp={res.timestamp} />
        </div>
      </div>

      {/* Combined YES chart across event markets */}
      {eventPriceHistory && (
        <section>
          <h2 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">
            Price History — YES outcomes across top {topSeries.length} of {eventPriceHistory.series.length} markets
          </h2>
          <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
            <PriceChart instruments={chartInstruments} />
          </div>
        </section>
      )}

      {/* Markets */}
      <section>
        <h2 className="mb-3 text-base font-semibold text-[var(--text-primary)]">
          Markets ({markets.length})
        </h2>
        {markets.length === 0 ? (
          <EmptyState message="No markets for this event" />
        ) : (
          <div className="space-y-3">
            {markets.map((mkt) => (
              <div
                key={mkt.marketUid}
                className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4"
              >
                <div className="mb-3 flex items-start justify-between gap-4">
                  <Link
                    href={uidToPath(mkt.marketUid, "/markets")}
                    className="text-sm font-medium text-[var(--text-primary)] hover:text-[var(--color-primary)] transition-colors"
                  >
                    {mkt.displayTitle ?? mkt.title ?? mkt.marketRef}
                  </Link>
                  <div className="flex items-center gap-3 shrink-0 text-xs text-[var(--text-tertiary)]">
                    <span>{formatUsd(mkt.volume24h)} vol</span>
                    <span>{formatUsd(mkt.liquidity)} liq</span>
                    <span
                      className={`rounded px-1.5 py-0.5 ${
                        mkt.status === "active"
                          ? "bg-[var(--color-success)]/10 text-[var(--color-success)]"
                          : "bg-[var(--bg-surface)]"
                      }`}
                    >
                      {mkt.status}
                    </span>
                  </div>
                </div>
                <InstrumentTable instruments={mkt.instruments} />
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
