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

export interface UploadInput {
  readonly filename: string;
  readonly contentType: string;
  readonly content: Stream.Stream<Uint8Array, unknown>;
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
  readonly list: () => Effect.Effect<ReadonlyArray<TransferRecord>, MetaError>;
  readonly find: (id: string) => Effect.Effect<TransferRecord | undefined, MetaError>;
  readonly download: (
    id: string,
  ) => Effect.Effect<DownloadResult, NotFoundError | ExpiredError | StorageError | MetaError>;
  readonly remove: (id: string) => Effect.Effect<void, NotFoundError | StorageError | MetaError>;
  /** Delete expired transfers (blob + metadata). Returns how many were reaped. */
  readonly sweepExpired: () => Effect.Effect<number, MetaError>;
}

export class FileTransfer extends Context.Tag("trail/FileTransfer")<
  FileTransfer,
  FileTransferService
>() {}

const randomId = () => globalThis.crypto.randomUUID().replaceAll("-", "").slice(0, 16);

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
        };
        yield* meta.upsert(record).pipe(
          // Never leave an unreferenced blob when metadata persistence fails.
          Effect.onError(() => storage.remove(id).pipe(Effect.ignoreLogged)),
        );
        return record;
      });

    const list = () =>
      meta.list().pipe(
        Effect.map((records) => {
          const now = Date.now();
          return records.filter((r) => !isExpired(r, now));
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

    const remove = (id: string) =>
      Effect.gen(function* () {
        const record = yield* meta.find(id);
        if (record === undefined) return yield* new NotFoundError({ id });
        yield* storage.remove(id);
        yield* meta.remove(id);
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

    return { upload, list, find, download, remove, sweepExpired } satisfies FileTransferService;
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
