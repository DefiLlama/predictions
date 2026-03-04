import { env } from "@/src/config/env";

type CorsPolicy =
  | {
      mode: "wildcard";
      origins: null;
    }
  | {
      mode: "allowlist";
      origins: Set<string>;
    };

const DEFAULT_ALLOW_HEADERS = "Content-Type, Authorization";
const ALLOW_METHODS = "GET,HEAD,OPTIONS";

export function parseCorsPolicy(originValue: string): CorsPolicy {
  const normalized = originValue.trim();
  if (normalized === "" || normalized === "*") {
    return { mode: "wildcard", origins: null };
  }

  const origins = normalized
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (origins.length === 0) {
    return { mode: "wildcard", origins: null };
  }

  return {
    mode: "allowlist",
    origins: new Set(origins),
  };
}

const policy = parseCorsPolicy(env.CORS_ORIGIN);

function resolveAllowOrigin(request: Request, corsPolicy: CorsPolicy): string | null {
  if (corsPolicy.mode === "wildcard") {
    return "*";
  }

  const origin = request.headers.get("origin");
  if (!origin) {
    return null;
  }

  if (!corsPolicy.origins.has(origin)) {
    return null;
  }

  return origin;
}

export function applyCorsHeaders(request: Request, headers: Headers, corsPolicy: CorsPolicy = policy): void {
  const allowOrigin = resolveAllowOrigin(request, corsPolicy);
  if (allowOrigin) {
    headers.set("access-control-allow-origin", allowOrigin);
  }

  if (corsPolicy.mode === "allowlist") {
    headers.set("vary", "Origin");
  }

  headers.set("access-control-allow-methods", ALLOW_METHODS);

  const requestedHeaders = request.headers.get("access-control-request-headers");
  headers.set(
    "access-control-allow-headers",
    requestedHeaders && requestedHeaders.trim().length > 0 ? requestedHeaders : DEFAULT_ALLOW_HEADERS,
  );

  headers.set("access-control-max-age", "86400");
}

export function createPreflightResponse(request: Request): Response {
  const headers = new Headers();
  applyCorsHeaders(request, headers);
  return new Response(null, { status: 204, headers });
}
