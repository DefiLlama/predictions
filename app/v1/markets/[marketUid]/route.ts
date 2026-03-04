import { handleMarketDetail } from "@/lib/api/server/handlers";
import { headFromGet, preflightResponse } from "@/lib/api/server/response";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(
  request: Request,
  context: { params: Promise<{ marketUid: string }> },
): Promise<Response> {
  const { marketUid } = await context.params;
  return handleMarketDetail(request, marketUid);
}

export async function HEAD(
  request: Request,
  context: { params: Promise<{ marketUid: string }> },
): Promise<Response> {
  return headFromGet(GET(request, context));
}

export async function OPTIONS(request: Request): Promise<Response> {
  return preflightResponse(request);
}
