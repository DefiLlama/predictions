/** Build a UID from route segments: provider + ref segments → "provider:ref" */
export function buildUid(provider: string, refSegments: string[]): string {
  return `${provider}:${refSegments.join("/")}`;
}

/** Parse a UID back to route path: "provider:ref" → "/prefix/provider/ref" */
export function uidToPath(uid: string, prefix: string): string {
  const idx = uid.indexOf(":");
  if (idx <= 0) return prefix;
  const provider = uid.slice(0, idx);
  const ref = uid.slice(idx + 1);
  return `${prefix}/${provider}/${ref}`;
}

/** Build a link preserving the provider query param */
export function withProvider(
  path: string,
  provider: string | undefined,
): string {
  if (!provider) return path;
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}provider=${provider}`;
}

/** Convert page number (1-based) to offset */
export function pageToOffset(page: number, limit: number): number {
  return Math.max(0, (page - 1) * limit);
}
