import { notFound } from "next/navigation";
import Link from "next/link";
import { getMarketDetail, getMarketPriceHistory } from "@/lib/api/client";
import { InstrumentTable } from "@/components/instrument-table";
import { PriceChart } from "@/components/price-chart";
import { effectiveStatus, formatUsd, formatTs, providerLabel, statusBadgeClass } from "@/lib/utils/format";

export default async function MarketDetailPage({
  params,
}: {
  params: Promise<{ provider: string; marketRef: string[] }>;
}) {
  const { provider, marketRef } = await params;
  const marketUid = `${provider}:${marketRef.join("/")}`;

  let detailRes;
  try {
    detailRes = await getMarketDetail(marketUid);
  } catch {
    notFound();
  }

  const { market, instruments } = detailRes.data;
  const marketDisplayStatus = effectiveStatus(market.status, market.closeTime) ?? market.status;

  // Load price history
  let priceHistory = null;
  try {
    const phRes = await getMarketPriceHistory(marketUid);
    priceHistory = phRes.data;
  } catch {
    // Price history may not be available
  }

  return (
    <div className="space-y-6">
      {/* Breadcrumb */}
      <div className="flex items-center gap-2 text-xs text-[var(--text-tertiary)]">
        <Link href="/markets" className="hover:text-[var(--color-primary)]">
          Markets
        </Link>
        <span>/</span>
        <span>{providerLabel(provider)}</span>
      </div>

      {/* Header */}
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-[var(--text-primary)]">
            {market.title ?? marketUid}
          </h1>
          <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-[var(--text-tertiary)]">
            <span className="rounded bg-[var(--bg-surface)] px-1.5 py-0.5">
              {providerLabel(market.providerCode)}
            </span>
            <span className={`rounded px-1.5 py-0.5 ${statusBadgeClass(marketDisplayStatus)}`}>
              {marketDisplayStatus}
            </span>
            {market.closeTime && (
              <span>Closes {formatTs(market.closeTime)}</span>
            )}
          </div>

          {/* Event backlink */}
          {market.eventRef && (
            <div className="mt-2">
              <Link
                href={`/events/${market.providerCode}/${market.eventRef}`}
                className="text-xs text-[var(--color-primary)] hover:underline"
              >
                Event: {market.eventTitle ?? market.eventRef}
              </Link>
            </div>
          )}
        </div>
        <div className="flex items-center gap-4 text-xs text-[var(--text-tertiary)] shrink-0">
          <span>{formatUsd(market.volume24h)} vol</span>
          <span>{formatUsd(market.liquidity)} liq</span>
        </div>
      </div>

      {/* Instruments */}
      <section>
        <h2 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">
          Instruments
        </h2>
        <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
          <InstrumentTable instruments={instruments} />
        </div>
      </section>

      {/* Price chart */}
      {priceHistory && priceHistory.instruments.length > 0 && (
        <section>
          <h2 className="mb-2 text-sm font-semibold text-[var(--text-primary)]">
            Price History (7d)
          </h2>
          <div className="rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] p-4">
            <PriceChart instruments={priceHistory.instruments} />
          </div>
        </section>
      )}
    </div>
  );
}
