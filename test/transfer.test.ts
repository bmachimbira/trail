import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NodeContext } from "@effect/platform-node";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";
import { AppConfig } from "../src/lib/config.js";
import { MetaError } from "../src/lib/errors.js";
import { type KeyValueStore, LocalKVLive } from "../src/lib/kv.js";
import type { TransferRecord } from "../src/lib/meta.js";
import { TransferMeta, TransferMetaLive, type TransferMetaService } from "../src/lib/meta.js";
import { type FileStorage, FileStorageLive } from "../src/lib/storage.js";
import type { FileTransferService } from "../src/lib/transfer.js";
import { adoptOrphans, FileTransfer, FileTransferLive } from "../src/lib/transfer.js";

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
    adoptOrphansTo: undefined,
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

const OWNER = "session-a";

const uploadBytes = (
  transfer: FileTransferService,
  bytes: number[],
  filename = "notes.txt",
  bundleId = "bundle-a",
) =>
  transfer.upload({
    ownerId: OWNER,
    bundleId,
    filename,
    contentType: "text/plain",
    content: Stream.fromIterable([new Uint8Array(bytes)]),
  });

describe("FileTransfer", () => {
  it("uploads, lists, and returns the exact bytes on get", async () => {
    const result = await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [104, 105]);
        const listed = yield* transfer.list(OWNER);
        const fetched = yield* transfer.download(record.id);
        if (fetched.kind !== "stream")
          throw new Error(`expected stream download, got ${fetched.kind}`);
        const content = yield* Stream.runFold(fetched.content, new Uint8Array(), (all, chunk) => {
          const next = new Uint8Array(all.byteLength + chunk.byteLength);
          next.set(all);
          next.set(chunk, all.byteLength);
          return next;
        });
        return { record, listed, fetched: { ...fetched, content } };
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

  it("enforces the size limit across chunks and cleans up the partial upload", async () => {
    const { Live, dir } = makeLive({ maxUploadBytes: 4 });
    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const transfer = yield* FileTransfer;
          return yield* Effect.either(
            transfer.upload({
              ownerId: OWNER,
              bundleId: "bundle-a",
              filename: "chunked.bin",
              contentType: "application/octet-stream",
              content: Stream.fromIterable([new Uint8Array([1, 2, 3]), new Uint8Array([4, 5])]),
            }),
          );
        }).pipe(Effect.provide(Live), Effect.scoped),
      );
      expect(result._tag).toBe("Left");
      if (result._tag === "Left") {
        expect(result.left).toMatchObject({
          _tag: "UploadTooLargeError",
          size: 5,
          maxBytes: 4,
        });
      }
      expect(readdirSync(dir)).toEqual([]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("removes the blob when metadata persistence fails", async () => {
    const dir = mkdtempSync(join(tmpdir(), "trail-compensation-"));
    const ConfigTest = Layer.succeed(AppConfig, {
      uploadDir: dir,
      maxUploadBytes: 1024,
      ttlMs: 60_000,
      adoptOrphansTo: undefined,
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
    const failingMeta: TransferMetaService = {
      list: () => Effect.succeed([]),
      find: () => Effect.succeed(undefined),
      upsert: () => Effect.fail(new MetaError({ op: "write", cause: "test failure" })),
      update: (id) => Effect.fail(new MetaError({ op: "write", cause: id })),
      remove: () => Effect.succeed(undefined),
    };
    const Live = FileTransferLive.pipe(
      Layer.provide(Layer.succeed(TransferMeta, failingMeta)),
      Layer.provide(FileStorageLive),
      Layer.provide(ConfigTest),
      Layer.provide(NodeContext.layer),
    );

    try {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const transfer = yield* FileTransfer;
          return yield* Effect.either(uploadBytes(transfer, [1, 2, 3]));
        }).pipe(Effect.provide(Live), Effect.scoped),
      );
      expect(result._tag).toBe("Left");
      expect(readdirSync(dir)).toEqual([]);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("marks expired transfers as ExpiredError and hides them from list", async () => {
    const result = await runTest({ ttlMs: -60_000 }, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [1]);
        const listed = yield* transfer.list(OWNER);
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
        const listed = yield* transfer.list(OWNER);
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
        const listed = yield* transfer.list(OWNER);
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
        yield* transfer.remove(record.id, OWNER);
        const listed = yield* transfer.list(OWNER);
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

  it("hides and protects transfers from other sessions", async () => {
    await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const record = yield* uploadBytes(transfer, [1]);
        expect(yield* transfer.list("session-b")).toEqual([]);
        const denied = yield* Effect.either(transfer.remove(record.id, "session-b"));
        expect(denied._tag).toBe("Left");
        expect((yield* transfer.list(OWNER)).map((r) => r.id)).toEqual([record.id]);
        // Download is by link, so a stranger can still fetch it.
        expect((yield* transfer.download(record.id)).record.id).toBe(record.id);
      }),
    );
  });

  it("groups files uploaded together into one bundle and zips them", async () => {
    await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const a = yield* uploadBytes(transfer, [104, 105], "a.txt", "b1");
        const b = yield* uploadBytes(transfer, [1, 2, 3], "a.txt", "b1");
        yield* uploadBytes(transfer, [7], "solo.bin", "b2");

        const bundles = yield* transfer.listBundles(OWNER);
        expect(bundles.map((x) => [x.id, x.files.length])).toEqual([
          ["b2", 1],
          ["b1", 2],
        ]);
        expect(yield* transfer.listBundles("session-b")).toEqual([]);

        const zipped = yield* transfer.downloadBundle("b1");
        const zip = Buffer.concat([...(yield* Stream.runCollect(zipped.content))]);
        expect(zip.readUInt32LE(0)).toBe(0x04034b50);
        expect(zip.readUInt32LE(zip.length - 22)).toBe(0x06054b50);
        expect(zip.readUInt16LE(zip.length - 12)).toBe(2);
        // Duplicate names inside a bundle get a suffix rather than clobbering.
        expect(zip.includes(Buffer.from("a (2).txt"))).toBe(true);
        expect((yield* transfer.find(a.id))?.downloads).toBe(1);
        expect((yield* transfer.find(b.id))?.downloads).toBe(1);

        const denied = yield* Effect.either(transfer.removeBundle("b1", "session-b"));
        expect(denied._tag).toBe("Left");
        yield* transfer.removeBundle("b1", OWNER);
        expect((yield* transfer.listBundles(OWNER)).map((x) => x.id)).toEqual(["b2"]);
        expect(yield* transfer.bundle("b1")).toBeUndefined();
      }),
    );
  });

  it("adopts ownerless records into one bundle for a session", async () => {
    await runTest({}, (transfer) =>
      Effect.gen(function* () {
        const meta = yield* TransferMeta;
        const legacy = (id: string): TransferRecord => ({
          id,
          filename: `${id}.bin`,
          contentType: "application/octet-stream",
          size: 1,
          uploadedAt: new Date().toISOString(),
          expiresAt: new Date(Date.now() + 60_000).toISOString(),
          downloads: 0,
        });
        yield* meta.upsert(legacy("old1"));
        yield* meta.upsert(legacy("old2"));
        const mine = yield* uploadBytes(transfer, [1], "mine.txt", "b-mine");

        expect(yield* adoptOrphans("session-z")).toBe(2);
        const bundles = yield* transfer.listBundles("session-z");
        expect(bundles).toHaveLength(1);
        expect(bundles[0]?.files.map((f) => f.id).sort()).toEqual(["old1", "old2"]);
        // Already-owned records are untouched, and a second run is a no-op.
        expect((yield* transfer.find(mine.id))?.ownerId).toBe(OWNER);
        expect(yield* adoptOrphans("session-z")).toBe(0);
      }),
    );
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
