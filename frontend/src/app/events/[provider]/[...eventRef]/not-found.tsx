import Link from "next/link";

export default function EventNotFound() {
  return (
    <div className="flex flex-col items-center justify-center gap-4 py-20">
      <h2 className="text-2xl font-bold text-[var(--text-primary)]">
        Event Not Found
      </h2>
      <p className="text-sm text-[var(--text-secondary)]">
        The event you&apos;re looking for doesn&apos;t exist or has been removed.
      </p>
      <Link
        href="/events"
        className="rounded-lg bg-[var(--color-primary)] px-4 py-2 text-sm font-medium text-white hover:opacity-90"
      >
        Browse Events
      </Link>
    </div>
  );
}
