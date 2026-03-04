import { jsonWithCors } from "./response";

export function badRequest(request: Request, error: string): Response {
  return jsonWithCors(request, { error }, { status: 400 });
}

export function notFound(request: Request, error: string): Response {
  return jsonWithCors(request, { error }, { status: 404 });
}
