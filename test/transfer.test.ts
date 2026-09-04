import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NodeContext } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import { AppConfig } from "../src/lib/config.js";
import { type KeyValueStore, LocalKVLive } from "../src/lib/kv.js";
import type { TransferRecord } from "../src/lib/meta.js";
import { type TransferMeta, TransferMetaLive } from "../src/lib/meta.js";
import { type FileStorage, FileStorageLive } from "../src/lib/storage.js";
import type { DownloadResult, FileTransferService } from "../src/lib/transfer.js";
import { FileTransfer, FileTransferLive } from "../src/lib/transfer.js";

const bytesOf = (d: DownloadResult) => {
  if (d.kind !== "bytes") throw new Error(`expected bytes download, got ${d.kind}`);
  return d;
};

interface TestCfg {
  readonly maxUploadBytes?: number;
  readonly ttlMs?: number;
}

type AllTags = FileTransfer | TransferMeta | FileStorage | KeyValueStore | AppConfig;

const makeLive = (cfg: TestCfg = {}, dir = mkdtempSync(join(tmpdir(), "trail-test-"))) => {
  const ConfigTest = Layer.succeed(AppConfig, {
    uploadDir: dir,
    maxUploadBytes: cfg.maxUploadBytes ?? 1024,
    ttlMs: cfg.ttlMs ?? 60_000,
    storageDriver: "local" as const,
    sweepIntervalMs: 60_000,
    s3: {
      bucket: "unused",
      region: "us-east-1",
      endpoint: undefined,
      pathStyle: false,
      prefix: "trail",
      downloadUrlTtlMs: 3_600_000,
    },
  });
  const Live = Layer.mergeAll(
    FileTransferLive,
    TransferMetaLive,
    FileStorageLive,
    LocalKVLive,
    ConfigTest,
  ).pipe(
    Layer.provide(FileStorageLive),
    Layer.provide(TransferMetaLive),
    Layer.provide(LocalKVLive),
    Layer.provide(ConfigTest),
    Layer.provide(NodeContext.layer),
  );
  return { Live, dir };
};

const runTest = <A, E>(
  cfg: TestCfg,
  eff: (transfer: FileTransferService) => Effect.Effect<A, E, AllTags>,
) => {
  const { Live, dir } = makeLive(cfg);
  return Effect.runPromise(
    Effect.gen(function* () {
      const transfer = yield* FileTransfer;
      return yield* eff(transfer);
    }).pipe(Effect.provide(Live), Effect.scoped),
  ).finally(() => rmSync(dir, { recursive: true, force: true }));
};

const uploadBytes = (transfer: FileTransferService, bytes: number[], filename = "notes.txt") =>
  transfer.upload({
    filename,
    contentType: "text/plain",
    content: new Uint8Array(bytes),
  });

describe("FileTransfer", () => {
  it("uploads, lists, and returns the exact bytes on get", async () => {
    const result = await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [104, 105]);
        const listed = yield* transfer.list();
        const fetched = yield* transfer.download(record.id);
        return { record, listed, fetched: bytesOf(fetched) };
      }),
    );
    expect(result.record.filename).toBe("notes.txt");
    expect(result.record.size).toBe(2);
    expect(result.fetched.record.downloads).toBe(1); // get() counts the download
    expect(result.listed).toHaveLength(1);
    expect(Array.from(result.fetched.content)).toEqual([104, 105]);
    expect(result.fetched.record.id).toBe(result.record.id);
  });

  it("persists uploads to disk under the upload dir", async () => {
    const { Live, dir } = makeLive();
    await Effect.runPromise(
      Effect.gen(function* () {
        const transfer = yield* FileTransfer;
        yield* uploadBytes(transfer, [1, 2, 3], "blob.bin");
      }).pipe(Effect.provide(Live), Effect.scoped),
    );
    expect(existsSync(join(dir, "meta.json"))).toBe(true);
    const meta = JSON.parse(readFileSync(join(dir, "meta.json"), "utf8")) as TransferRecord[];
    expect(meta).toHaveLength(1);
    expect(meta[0].filename).toBe("blob.bin");
  });

  it("reloads metadata from disk in a fresh runtime over the same dir", async () => {
    const { Live, dir } = makeLive();
    const record = await Effect.runPromise(
      Effect.gen(function* () {
        const transfer = yield* FileTransfer;
        return yield* uploadBytes(transfer, [9], "persist.txt");
      }).pipe(Effect.provide(Live), Effect.scoped),
    );
    // A brand-new layer stack (simulating a server restart) sees the record.
    const { Live: Live2 } = makeLive({}, dir);
    const found = await Effect.runPromise(
      Effect.gen(function* () {
        const transfer = yield* FileTransfer;
        return yield* transfer.find(record.id);
      }).pipe(Effect.provide(Live2), Effect.scoped),
    );
    expect(found?.filename).toBe("persist.txt");
    rmSync(dir, { recursive: true, force: true });
  });

  it("rejects uploads above the configured size limit", async () => {
    const result = await runTest({ maxUploadBytes: 4 }, (transfer) =>
      Effect.either(uploadBytes(transfer, [1, 2, 3, 4, 5])),
    );
    expect(result._tag).toBe("Left");
    if (result._tag === "Left") {
      expect(result.left).toMatchObject({ _tag: "UploadTooLargeError", size: 5, maxBytes: 4 });
    }
  });

  it("marks expired transfers as ExpiredError and hides them from list", async () => {
    const result = await runTest({ ttlMs: -60_000 }, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [1]);
        const listed = yield* transfer.list();
        const gotten = yield* Effect.either(transfer.download(record.id));
        return { record, listed, gotten };
      }),
    );
    expect(result.listed).toHaveLength(0);
    expect(result.gotten._tag).toBe("Left");
    if (result.gotten._tag === "Left") {
      expect(result.gotten.left).toMatchObject({ _tag: "ExpiredError", id: result.record.id });
    }
  });

  it("sweepExpired reaps expired records and their blobs", async () => {
    const { Live, dir } = makeLive({ ttlMs: -60_000 });
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const transfer = yield* FileTransfer;
        const record = yield* uploadBytes(transfer, [1, 2, 3], "old.bin");
        const reaped = yield* transfer.sweepExpired();
        const listed = yield* transfer.list();
        const found = yield* transfer.find(record.id);
        return { record, reaped, listed, found };
      }).pipe(Effect.provide(Live), Effect.scoped),
    );
    expect(result.reaped).toBe(1);
    expect(result.listed).toHaveLength(0);
    expect(result.found).toBeUndefined();
    expect(existsSync(join(dir, result.record.id))).toBe(false);
    rmSync(dir, { recursive: true, force: true });
  });

  it("sweepExpired keeps live transfers", async () => {
    const result = await runTest({}, (transfer) =>
      Effect.gen(function* () {
        yield* uploadBytes(transfer, [1]);
        const reaped = yield* transfer.sweepExpired();
        const listed = yield* transfer.list();
        return { reaped, listed };
      }),
    );
    expect(result.reaped).toBe(0);
    expect(result.listed).toHaveLength(1);
  });

  it("deletes the blob and metadata, then 404s", async () => {
    const result = await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [1]);
        yield* transfer.remove(record.id);
        const listed = yield* transfer.list();
        const gotten = yield* Effect.either(transfer.download(record.id));
        return { listed, gotten };
      }),
    );
    expect(result.listed).toHaveLength(0);
    expect(result.gotten._tag).toBe("Left");
    if (result.gotten._tag === "Left") {
      expect(result.gotten.left).toMatchObject({ _tag: "NotFoundError" });
    }
  });

  it("sanitizes hostile filenames", async () => {
    const result = await runTest({}, (transfer) =>
      uploadBytes(transfer, [1], '../../etc/passwd\u0000 evil"name.txt'),
    );
    expect(result.filename).toBe("....etcpasswd evilname.txt");
  });

  it("increments the download counter across gets", async () => {
    const result = await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [1]);
        yield* transfer.download(record.id);
        const second = yield* transfer.download(record.id);
        return second.record.downloads;
      }),
    );
    expect(result).toBe(2);
  });
});
