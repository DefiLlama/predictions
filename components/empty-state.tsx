export function EmptyState({
  message = "No data available",
}: {
  message?: string;
}) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 rounded-lg border border-[var(--bg-border)] bg-[var(--bg-card)] py-16">
      <p className="text-sm text-[var(--text-tertiary)]">{message}</p>
    </div>
  );
}
