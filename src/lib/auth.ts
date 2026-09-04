import { timingSafeEqual } from "node:crypto";

/**
 * Bearer-token check for the management endpoints. An undefined/empty token
 * means no AUTH_TOKEN is configured and the instance runs open.
 */
export const isAuthorized = (authorization: string | null, token: string | undefined): boolean => {
  if (token === undefined || token === "") return true;
  if (authorization === null || !authorization.startsWith("Bearer ")) return false;
  const presented = Buffer.from(authorization.slice("Bearer ".length));
  const expected = Buffer.from(token);
  return presented.length === expected.length && timingSafeEqual(presented, expected);
};
