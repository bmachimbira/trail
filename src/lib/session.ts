import { randomBytes } from "node:crypto";
import type { AstroCookies } from "astro";

const COOKIE = "trail_session";
const THIRTY_DAYS = 60 * 60 * 24 * 30;
const wellFormed = /^[a-f0-9]{32}$/;

/**
 * The caller's session id, minted into an httpOnly cookie on first contact.
 * The random id is the whole credential: uploads are owned by it, and only
 * the owning session can list or delete them. Share links stay public.
 */
export const sessionId = (cookies: AstroCookies): string => {
  const existing = cookies.get(COOKIE)?.value;
  if (existing !== undefined && wellFormed.test(existing)) return existing;
  const id = randomBytes(16).toString("hex");
  cookies.set(COOKIE, id, {
    httpOnly: true,
    sameSite: "lax",
    secure: import.meta.env.PROD,
    path: "/",
    maxAge: THIRTY_DAYS,
  });
  return id;
};
