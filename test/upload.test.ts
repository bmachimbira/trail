import { mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { APIRoute } from "astro";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const uploadDir = mkdtempSync(join(tmpdir(), "trail-upload-route-"));
const boundary = "trail-test-boundary";
let upload: APIRoute;
let uploadedId = "";

beforeAll(async () => {
  process.env.STORAGE_DRIVER = "local";
  process.env.UPLOAD_DIR = uploadDir;
  process.env.MAX_UPLOAD_MB = "1";
  process.env.TTL_HOURS = "24";
  ({ POST: upload } = await import("../src/pages/api/upload.js"));
});

const asContext = (obj: unknown) => obj as never;

const multipartBody = (fileSize: number) => {
  const encoder = new TextEncoder();
  const header = encoder.encode(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="stream.bin"\r\nContent-Type: application/octet-stream\r\n\r\n`,
  );
  const footer = encoder.encode(`\r\n--${boundary}--\r\n`);
  let state: "header" | "file" | "footer" | "done" = "header";
  let sent = 0;

  return new ReadableStream<Uint8Array>({
    pull(controller) {
      if (state === "header") {
        controller.enqueue(header);
        state = "file";
        return;
      }
      if (state === "file") {
        if (sent === fileSize) {
          state = "footer";
          return;
        }
        const length = Math.min(64 * 1024, fileSize - sent);
        controller.enqueue(new Uint8Array(length).fill(0x61));
        sent += length;
        return;
      }
      if (state === "footer") {
        controller.enqueue(footer);
        state = "done";
        controller.close();
      }
    },
  });
};

const requestFor = (fileSize: number) =>
  new Request("http://localhost/api/upload", {
    method: "POST",
    headers: { "content-type": `multipart/form-data; boundary=${boundary}` },
    body: multipartBody(fileSize),
    duplex: "half",
  } as RequestInit & { duplex: "half" });

describe("streaming upload route", () => {
  it("streams a multipart file into local storage", async () => {
    const response = await upload(asContext({ request: requestFor(3) }));
    expect(response.status).toBe(201);
    const body = (await response.json()) as { filename: string; size: number; id: string };
    uploadedId = body.id;
    expect(body).toMatchObject({
      filename: "stream.bin",
      size: 3,
    });
  });

  it("returns 413 when the streamed file exceeds MAX_UPLOAD_MB", async () => {
    const response = await upload(asContext({ request: requestFor(1024 * 1024 + 1) }));
    expect(response.status).toBe(413);
    expect(await response.json()).toMatchObject({
      error: "payload_too_large",
      maxBytes: 1024 * 1024,
    });
    expect(readdirSync(uploadDir).sort()).toEqual([uploadedId, "meta.json"].sort());
  });

  it("stops and removes the temp file when the client aborts mid-upload", async () => {
    const controller = new AbortController();
    const encoder = new TextEncoder();
    // Header plus one chunk, then the body stalls until the abort.
    const body = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(
          encoder.encode(
            `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="stall.bin"\r\nContent-Type: application/octet-stream\r\n\r\n`,
          ),
        );
        c.enqueue(new Uint8Array(64 * 1024));
      },
    });
    const request = new Request("http://localhost/api/upload", {
      method: "POST",
      headers: { "content-type": `multipart/form-data; boundary=${boundary}` },
      body,
      duplex: "half",
      signal: controller.signal,
    } as RequestInit & { duplex: "half" });

    const pending = upload(asContext({ request }));
    await new Promise((r) => setTimeout(r, 100));
    controller.abort();
    const response = await Promise.race([
      pending,
      new Promise<never>((_, reject) => setTimeout(() => reject(new Error("upload never settled after abort")), 5000)),
    ]);
    expect(response.status).toBe(400);
    expect(readdirSync(uploadDir).sort()).toEqual([uploadedId, "meta.json"].sort());
  });
});

afterAll(() => {
  rmSync(uploadDir, { recursive: true, force: true });
});
