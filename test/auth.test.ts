import { describe, expect, it } from "vitest";
import { isAuthorized } from "../src/lib/auth.js";

describe("isAuthorized", () => {
  it("allows everything when no token is configured", () => {
    expect(isAuthorized(null, undefined)).toBe(true);
    expect(isAuthorized(null, "")).toBe(true);
    expect(isAuthorized("Bearer anything", undefined)).toBe(true);
  });

  it("accepts the configured bearer token", () => {
    expect(isAuthorized("Bearer s3cret", "s3cret")).toBe(true);
  });

  it("rejects missing, malformed, and wrong credentials", () => {
    expect(isAuthorized(null, "s3cret")).toBe(false);
    expect(isAuthorized("s3cret", "s3cret")).toBe(false);
    expect(isAuthorized("Basic s3cret", "s3cret")).toBe(false);
    expect(isAuthorized("Bearer wrong", "s3cret")).toBe(false);
    expect(isAuthorized("Bearer s3cret-extra", "s3cret")).toBe(false);
    expect(isAuthorized("Bearer", "s3cret")).toBe(false);
  });
});
