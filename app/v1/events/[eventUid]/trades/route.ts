import { handleEventTrades } from "@/lib/api/server/handlers";
import { headFromGet, preflightResponse } from "@/lib/api/server/response";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

export async function GET(
  request: Request,
  context: { params: Promise<{ eventUid: string }> },
): Promise<Response> {
  const { eventUid } = await context.params;
  return handleEventTrades(request, eventUid);
}

export async function HEAD(
  request: Request,
  context: { params: Promise<{ eventUid: string }> },
): Promise<Response> {
  return headFromGet(GET(request, context));
}

export async function OPTIONS(request: Request): Promise<Response> {
  return preflightResponse(request);
}
