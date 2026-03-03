export default function Loading() {
  return (
    <div className="flex items-center justify-center py-20">
      <div className="h-8 w-8 animate-spin rounded-full border-2 border-[var(--bg-border)] border-t-[var(--color-primary)]" />
    </div>
  );
}
