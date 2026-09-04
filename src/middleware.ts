import { defineMiddleware } from "astro:middleware";
import { isAuthorized } from "./lib/auth.js";

// Management routes (upload, list, delete) need the token when AUTH_TOKEN is
// set; share pages and downloads (GET/HEAD on a single file) stay public.
const isManagement = (pathname: string, method: string): boolean =>
  pathname === "/api/upload" ||
  pathname === "/api/files" ||
  (/^\/api\/(files|bundles)\/[^/]+$/.test(pathname) && method !== "GET" && method !== "HEAD");

export const onRequest = defineMiddleware(({ request, url }, next) => {
  if (!isManagement(url.pathname, request.method)) return next();
  if (isAuthorized(request.headers.get("authorization"), process.env.AUTH_TOKEN)) return next();
  return new Response(JSON.stringify({ error: "unauthorized" }), {
    status: 401,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "WWW-Authenticate": "Bearer",
    },
  });
});
