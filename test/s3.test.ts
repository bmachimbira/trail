/**
 * Integration test for the S3 storage driver: boots an in-process s3rver
 * (S3-compatible) and drives the real Astro route handlers end to end —
 * upload via multipart POST, presigned 302 download redirect, list,
 * delete — with blobs and meta.json living in the bucket.
 *
 * Env must be set before importing the app modules (config is read when
 * the runtime builds on first use), hence the dynamic imports.
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { GetObjectCommand, S3Client } from "@aws-sdk/client-s3";
import S3rver from "s3rver";
import { beforeAll, describe, expect, it } from "vitest";

const BUCKET = "trail-test";
const s3rverDir = mkdtempSync(join(tmpdir(), "trail-s3rver-"));
let endpoint = "";

beforeAll(async () => {
  const s3rver = new S3rver({
    directory: s3rverDir,
    address: "127.0.0.1",
    silent: true,
    configureBuckets: [{ name: BUCKET }],
  });
  const address = await s3rver.run();
  endpoint = `http://127.0.0.1:${address.port}`;

  process.env.STORAGE_DRIVER = "s3";
  process.env.S3_BUCKET = BUCKET;
  process.env.S3_REGION = "us-east-1";
  process.env.S3_ENDPOINT = endpoint;
  process.env.S3_PATH_STYLE = "true";
  process.env.DOWNLOAD_URL_TTL_MINUTES = "5";
  process.env.AWS_ACCESS_KEY_ID = "S3RVER";
  process.env.AWS_SECRET_ACCESS_KEY = "S3RVER";

  return async () => {
    await s3rver.close();
    rmSync(s3rverDir, { recursive: true, force: true });
  };
});

const importRoutes = async () => {
  const { GET: listFiles } = await import("../src/pages/api/files.js");
  const { POST: upload } = await import("../src/pages/api/upload.js");
  const fileRoute = await import("../src/pages/api/files/[id].js");
  return { listFiles, upload, getFile: fileRoute.GET, deleteFile: fileRoute.DELETE };
};

// Minimal AstroCookies stand-in: one jar shared across the file's requests.
const jar = new Map<string, string>();
const cookies = {
  get: (k: string) => (jar.has(k) ? { value: jar.get(k) } : undefined),
  set: (k: string, v: string) => void jar.set(k, v),
  has: (k: string) => jar.has(k),
  delete: (k: string) => void jar.delete(k),
};
const asContext = (obj: unknown) => ({ cookies, ...(obj as object) }) as never;

describe("S3 driver (s3rver)", () => {
  it("uploads through the route into the bucket", async () => {
    const { upload } = await importRoutes();
    const bytes = new TextEncoder().encode("s3 roundtrip ✅\n");
    const fd = new FormData();
    fd.append("file", new File([bytes], "s3-check.txt", { type: "text/plain" }));
    const request = new Request("http://localhost/api/upload", { method: "POST", body: fd });

    const res = await upload(asContext({ request }));
    expect(res.status).toBe(201);
    const bundle = (await res.json()) as { files: Array<{ id: string; filename: string }> };
    const body = bundle.files[0] ?? { id: "", filename: "" };
    expect(body.filename).toBe("s3-check.txt");

    // Blob and metadata both live in the bucket.
    const probe = new S3Client({ region: "us-east-1", endpoint, forcePathStyle: true });
    const blob = await probe.send(
      new GetObjectCommand({ Bucket: BUCKET, Key: `trail/blobs/${body.id}` }),
    );
    if (!blob.Body) throw new Error("missing blob body");
    expect(new Uint8Array(await blob.Body.transformToByteArray())).toEqual(bytes);
    const meta = await probe.send(new GetObjectCommand({ Bucket: BUCKET, Key: "trail/meta.json" }));
    if (!meta.Body) throw new Error("missing meta body");
    expect((await meta.Body.transformToString("utf8")).includes("s3-check.txt")).toBe(true);
  });

  it("downloads via a presigned 302 redirect with byte-identical content", async () => {
    const { upload, getFile, listFiles } = await importRoutes();

    const fd = new FormData();
    const payload = "download me over s3";
    fd.append("file", new File([payload], "dl.txt", { type: "text/plain" }));
    const uploaded = await upload(
      asContext({
        request: new Request("http://localhost/api/upload", { method: "POST", body: fd }),
      }),
    );
    const id = ((await uploaded.json()) as { files: Array<{ id: string }> }).files[0]?.id ?? "";

    const redirect = await getFile(asContext({ params: { id } }));
    expect(redirect.status).toBe(302);
    const location = redirect.headers.get("location");
    if (location === null) throw new Error("expected Location header");
    expect(location).toContain(encodeURIComponent("dl.txt"));

    const fetched = await fetch(location);
    expect(fetched.status).toBe(200);
    expect(fetched.headers.get("content-disposition")).toContain("dl.txt");
    expect(await fetched.text()).toBe(payload);

    // Counter incremented server-side even though bytes went bucket→client.
    const listed = await listFiles(asContext({}));
    const { bundles } = (await listed.json()) as {
      bundles: Array<{ files: Array<{ id: string; downloads: number }> }>;
    };
    expect(bundles.flatMap((b) => b.files).find((f) => f.id === id)?.downloads).toBe(1);
  });

  it("deletes the blob and metadata, then 404s", async () => {
    const { upload, getFile, deleteFile } = await importRoutes();

    const fd = new FormData();
    fd.append("file", new File([new Uint8Array([1, 2, 3])], "gone.bin"));
    const uploaded = await upload(
      asContext({
        request: new Request("http://localhost/api/upload", { method: "POST", body: fd }),
      }),
    );
    const id = ((await uploaded.json()) as { files: Array<{ id: string }> }).files[0]?.id ?? "";

    expect((await deleteFile(asContext({ params: { id } }))).status).toBe(204);

    const probe = new S3Client({ region: "us-east-1", endpoint, forcePathStyle: true });
    await expect(
      probe.send(new GetObjectCommand({ Bucket: BUCKET, Key: `trail/blobs/${id}` })),
    ).rejects.toMatchObject({ $metadata: { httpStatusCode: 404 } });

    expect((await getFile(asContext({ params: { id } }))).status).toBe(404);
  });
});
