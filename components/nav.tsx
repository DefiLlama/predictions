"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const links = [
  { href: "/", label: "Dashboard" },
  { href: "/compare", label: "Compare" },
  { href: "/events", label: "Events" },
  { href: "/markets", label: "Markets" },
  { href: "/trades", label: "Top Trades" },
];

export function Nav() {
  const pathname = usePathname();

  return (
    <nav className="sticky top-0 z-40 border-b border-[var(--bg-border)] bg-[var(--bg-app)]/90 backdrop-blur-lg">
      <div className="mx-auto flex h-12 max-w-[1400px] items-center gap-8 px-4 sm:px-6 lg:px-8">
        <Link
          href="/"
          className="text-sm font-semibold tracking-tight text-[var(--text-primary)] shrink-0"
        >
          PredictionMarkets
        </Link>
        <div className="flex items-center h-full">
          {links.map(({ href, label }) => {
            const active =
              href === "/" ? pathname === "/" : pathname.startsWith(href);
            return (
              <Link
                key={href}
                href={href}
                className={`relative flex items-center h-full px-3 text-sm font-medium transition-colors ${
                  active
                    ? "text-[var(--color-primary)]"
                    : "text-[var(--text-tertiary)] hover:text-[var(--text-primary)]"
                }`}
              >
                {label}
                {active && (
                  <span className="absolute inset-x-3 -bottom-px h-0.5 bg-[var(--color-primary)] rounded-full" />
                )}
              </Link>
            );
          })}
        </div>
      </div>
    </nav>
  );
}
