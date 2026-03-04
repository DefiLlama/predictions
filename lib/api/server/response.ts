import { applyCorsHeaders, createPreflightResponse } from "./cors";

export function jsonWithCors(request: Request, payload: unknown, init?: ResponseInit): Response {
  const response = Response.json(payload, init);
  const headers = new Headers(response.headers);
  applyCorsHeaders(request, headers);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export async function headFromGet(getResponse: Promise<Response> | Response): Promise<Response> {
  const response = await getResponse;
  return new Response(null, {
    status: response.status,
    statusText: response.statusText,
    headers: new Headers(response.headers),
  });
}

export function preflightResponse(request: Request): Response {
  return createPreflightResponse(request);
}
