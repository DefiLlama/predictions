import Link from "next/link";

export default function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center gap-4 py-20">
      <h1 className="text-4xl font-bold text-[var(--text-primary)]">404</h1>
      <p className="text-[var(--text-secondary)]">Page not found</p>
      <Link
        href="/"
        className="rounded-lg bg-[var(--color-primary)] px-4 py-2 text-sm font-medium text-white hover:opacity-90"
      >
        Back to Dashboard
      </Link>
    </div>
  );
}
