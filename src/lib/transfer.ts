import { Context, Duration, Effect, Layer, Schedule, type Stream } from "effect";
import { AppConfig } from "./config.js";
import {
  ExpiredError,
  type MetaError,
  NotFoundError,
  type StorageError,
  type TransferError,
} from "./errors.js";
import { TransferMeta, type TransferRecord } from "./meta.js";
import { FileStorage } from "./storage.js";
import { zipStream } from "./zip.js";

export interface UploadInput {
  readonly filename: string;
  readonly contentType: string;
  readonly content: Stream.Stream<Uint8Array, unknown>;
  readonly ownerId: string;
  readonly bundleId: string;
}

/** Files uploaded together: one share link, one expiry. */
export interface Bundle {
  readonly id: string;
  readonly uploadedAt: string;
  readonly expiresAt: string;
  readonly files: ReadonlyArray<TransferRecord>;
}

export type DownloadResult =
  | { readonly kind: "redirect"; readonly record: TransferRecord; readonly url: string }
  | {
      readonly kind: "stream";
      readonly record: TransferRecord;
      readonly content: Stream.Stream<Uint8Array, StorageError>;
    };

export interface FileTransferService {
  readonly upload: (input: UploadInput) => Effect.Effect<TransferRecord, TransferError>;
  /** Live transfers owned by `ownerId`, newest first. */
  readonly list: (ownerId: string) => Effect.Effect<ReadonlyArray<TransferRecord>, MetaError>;
  readonly find: (id: string) => Effect.Effect<TransferRecord | undefined, MetaError>;
  readonly download: (
    id: string,
  ) => Effect.Effect<DownloadResult, NotFoundError | ExpiredError | StorageError | MetaError>;
  /** Delete a transfer the caller owns; anyone else gets NotFound, not Forbidden. */
  readonly remove: (
    id: string,
    ownerId: string,
  ) => Effect.Effect<void, NotFoundError | StorageError | MetaError>;
  /** Live bundles owned by `ownerId`, newest first. */
  readonly listBundles: (ownerId: string) => Effect.Effect<ReadonlyArray<Bundle>, MetaError>;
  /** A bundle by id, expired or not; the caller decides what expiry means. */
  readonly bundle: (id: string) => Effect.Effect<Bundle | undefined, MetaError>;
  readonly removeBundle: (
    id: string,
    ownerId: string,
  ) => Effect.Effect<void, NotFoundError | StorageError | MetaError>;
  /** All files of a live bundle as one zip stream; counts a download on each. */
  readonly downloadBundle: (
    id: string,
  ) => Effect.Effect<
    { readonly bundle: Bundle; readonly content: Stream.Stream<Uint8Array, StorageError> },
    NotFoundError | ExpiredError | StorageError | MetaError
  >;
  /** Delete expired transfers (blob + metadata). Returns how many were reaped. */
  readonly sweepExpired: () => Effect.Effect<number, MetaError>;
}

export class FileTransfer extends Context.Tag("trail/FileTransfer")<
  FileTransfer,
  FileTransferService
>() {}

const randomId = () => globalThis.crypto.randomUUID().replaceAll("-", "").slice(0, 16);
export const newBundleId = randomId;

const bundleOf = (record: TransferRecord) => record.bundleId ?? record.id;

/** Group newest-first records into bundles, keeping that order. */
const groupBundles = (records: ReadonlyArray<TransferRecord>): Bundle[] => {
  const byId = new Map<string, TransferRecord[]>();
  for (const record of records) {
    const files = byId.get(bundleOf(record));
    if (files === undefined) byId.set(bundleOf(record), [record]);
    else files.push(record);
  }
  // Records arrive newest-first; inside a bundle, upload order reads better.
  return [...byId].map(([id, newestFirst]) => {
    const files = [...newestFirst].reverse();
    return {
      id,
      uploadedAt: files[0]?.uploadedAt ?? "",
      expiresAt: files.reduce((max, f) => (f.expiresAt > max ? f.expiresAt : max), ""),
      files,
    };
  });
};

/** Zip entries need distinct names; a repeat becomes "name (2).ext". */
const uniqueNames = (files: ReadonlyArray<TransferRecord>): string[] => {
  const seen = new Map<string, number>();
  return files.map((f) => {
    const n = (seen.get(f.filename) ?? 0) + 1;
    seen.set(f.filename, n);
    if (n === 1) return f.filename;
    const dot = f.filename.lastIndexOf(".");
    return dot > 0
      ? `${f.filename.slice(0, dot)} (${n})${f.filename.slice(dot)}`
      : `${f.filename} (${n})`;
  });
};

const sanitizeFilename = (name: string) => {
  // biome-ignore lint/suspicious/noControlCharactersInRegex: stripping control characters from filenames is the point
  const cleaned = name.replace(/[/\\<>:"|?*\u0000-\u001f]/g, "").trim();
  return cleaned.length > 0 ? cleaned.slice(0, 200) : "file";
};

const isExpired = (record: TransferRecord, now: number) => Date.parse(record.expiresAt) < now;

export const FileTransferLive = Layer.effect(
  FileTransfer,
  Effect.gen(function* () {
    const storage = yield* FileStorage;
    const meta = yield* TransferMeta;
    const config = yield* AppConfig;

    const upload = (input: UploadInput) =>
      Effect.gen(function* () {
        const id = randomId();
        const now = new Date();
        const contentType = input.contentType || "application/octet-stream";
        const size = yield* storage
          .put(id, input.content, contentType)
          .pipe(Effect.onError(() => storage.remove(id).pipe(Effect.ignoreLogged)));
        const record: TransferRecord = {
          id,
          filename: sanitizeFilename(input.filename),
          contentType,
          size,
          uploadedAt: now.toISOString(),
          expiresAt: new Date(now.getTime() + config.ttlMs).toISOString(),
          downloads: 0,
          ownerId: input.ownerId,
          bundleId: input.bundleId,
        };
        yield* meta.upsert(record).pipe(
          // Never leave an unreferenced blob when metadata persistence fails.
          Effect.onError(() => storage.remove(id).pipe(Effect.ignoreLogged)),
        );
        return record;
      });

    const list = (ownerId: string) =>
      meta.list().pipe(
        Effect.map((records) => {
          const now = Date.now();
          return records.filter((r) => r.ownerId === ownerId && !isExpired(r, now));
        }),
      );

    const find = (id: string) => meta.find(id);

    const download = (id: string) =>
      Effect.gen(function* () {
        const record = yield* meta.find(id);
        if (record === undefined) return yield* new NotFoundError({ id });
        if (isExpired(record, Date.now())) return yield* new ExpiredError({ id });

        const url = yield* storage.presign(record);
        if (url !== undefined) {
          // Recipients stream straight from S3; we never touch the bytes.
          const counted = yield* meta.update(id, (r) => ({ ...r, downloads: r.downloads + 1 }));
          return { kind: "redirect" as const, record: counted, url };
        }

        const content = yield* storage.get(id);
        const counted = yield* meta.update(id, (r) => ({ ...r, downloads: r.downloads + 1 }));
        return { kind: "stream" as const, record: counted, content };
      });

    const remove = (id: string, ownerId: string) =>
      Effect.gen(function* () {
        const record = yield* meta.find(id);
        if (record === undefined || record.ownerId !== ownerId) {
          return yield* new NotFoundError({ id });
        }
        yield* storage.remove(id);
        yield* meta.remove(id);
      });

    const listBundles = (ownerId: string) => list(ownerId).pipe(Effect.map(groupBundles));

    const bundle = (id: string) =>
      meta.list().pipe(
        Effect.map((records) => {
          const files = records.filter((r) => bundleOf(r) === id);
          return files.length === 0 ? undefined : groupBundles(files)[0];
        }),
      );

    const removeBundle = (id: string, ownerId: string) =>
      Effect.gen(function* () {
        const records = yield* meta.list();
        const files = records.filter((r) => bundleOf(r) === id && r.ownerId === ownerId);
        if (files.length === 0) return yield* new NotFoundError({ id });
        yield* Effect.forEach(files, (f) => Effect.zipRight(storage.remove(f.id), meta.remove(f.id)), {
          discard: true,
        });
      });

    const downloadBundle = (id: string) =>
      Effect.gen(function* () {
        const found = yield* bundle(id);
        if (found === undefined) return yield* new NotFoundError({ id });
        if (Date.parse(found.expiresAt) < Date.now()) return yield* new ExpiredError({ id });
        const names = uniqueNames(found.files);
        const entries = yield* Effect.forEach(found.files, (f, i) =>
          storage.get(f.id).pipe(Effect.map((content) => ({ name: names[i] ?? f.filename, content }))),
        );
        yield* Effect.forEach(
          found.files,
          (f) => meta.update(f.id, (r) => ({ ...r, downloads: r.downloads + 1 })),
          { discard: true },
        );
        return { bundle: found, content: zipStream(entries) };
      });

    const sweepExpired = () =>
      meta.list().pipe(
        Effect.flatMap((records) => {
          const now = Date.now();
          return Effect.forEach(
            records.filter((r) => isExpired(r, now)),
            (record) =>
              Effect.zipRight(storage.remove(record.id), meta.remove(record.id)).pipe(
                Effect.as(1),
                // A reaped failure (e.g. S3 hiccup) must not kill the sweep.
                Effect.catchAll((error) =>
                  Effect.logWarning("failed to reap transfer", {
                    id: record.id,
                    error,
                  }).pipe(Effect.as(0)),
                ),
              ),
            { concurrency: 1 },
          );
        }),
        Effect.map((counts) => counts.reduce((a, b) => a + b, 0)),
      );

    return {
      upload,
      list,
      find,
      download,
      remove,
      listBundles,
      bundle,
      removeBundle,
      downloadBundle,
      sweepExpired,
    } satisfies FileTransferService;
  }),
);

/**
 * Give every record that predates sessions (no ownerId) to `ownerId`, grouped
 * as one bundle. Returns how many were adopted; idempotent once they're owned.
 */
export const adoptOrphans = (ownerId: string) =>
  Effect.gen(function* () {
    const meta = yield* TransferMeta;
    const orphans = (yield* meta.list()).filter((r) => r.ownerId === undefined);
    if (orphans.length === 0) return 0;
    const bundleId = newBundleId();
    yield* Effect.forEach(orphans, (r) => meta.update(r.id, (cur) => ({ ...cur, ownerId, bundleId })), {
      discard: true,
    });
    yield* Effect.logInfo("adopted orphaned uploads", { count: orphans.length, ownerId, bundleId });
    return orphans.length;
  });

/** Runs adoptOrphans once at boot when ADOPT_ORPHANS_TO is set. */
export const OrphanAdoptionLive = Layer.effectDiscard(
  Effect.gen(function* () {
    const config = yield* AppConfig;
    if (config.adoptOrphansTo === undefined) return;
    yield* adoptOrphans(config.adoptOrphansTo).pipe(
      Effect.catchAll((error) => Effect.logWarning("orphan adoption failed", { error })),
    );
  }),
);

/** Reaps expired transfers at boot and then on an interval; failures are logged, never fatal. */
export const ExpirySweeperLive = Layer.scopedDiscard(
  Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    const config = yield* AppConfig;
    yield* transfer.sweepExpired().pipe(
      Effect.catchAll((error) => Effect.logWarning("expiry sweep failed", { error })),
      Effect.schedule(Schedule.spaced(Duration.millis(config.sweepIntervalMs))),
      Effect.forkScoped,
    );
  }),
);
