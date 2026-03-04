import { handleMetaCategoryQuality } from "@/lib/api/server/handlers";
import { headFromGet, preflightResponse } from "@/lib/api/server/response";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(request: Request): Promise<Response> {
  return handleMetaCategoryQuality(request);
}

export async function HEAD(request: Request): Promise<Response> {
  return headFromGet(GET(request));
}

export async function OPTIONS(request: Request): Promise<Response> {
  return preflightResponse(request);
}
