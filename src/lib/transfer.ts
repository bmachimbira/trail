import { Context, Effect, Layer } from "effect";
import { AppConfig } from "./config.js";
import {
  ExpiredError,
  NotFoundError,
  StorageError,
  MetaError,
  UploadTooLargeError,
  type TransferError,
} from "./errors.js";
import { TransferMeta, type TransferRecord } from "./meta.js";
import { FileStorage } from "./storage.js";

export interface UploadInput {
  readonly filename: string;
  readonly contentType: string;
  readonly content: Uint8Array;
}

export type DownloadResult =
  | { readonly kind: "redirect"; readonly record: TransferRecord; readonly url: string }
  | { readonly kind: "bytes"; readonly record: TransferRecord; readonly content: Uint8Array };

export interface FileTransferService {
  readonly upload: (input: UploadInput) => Effect.Effect<TransferRecord, TransferError>;
  readonly list: () => Effect.Effect<ReadonlyArray<TransferRecord>, MetaError>;
  readonly find: (id: string) => Effect.Effect<TransferRecord | undefined, MetaError>;
  readonly download: (
    id: string,
  ) => Effect.Effect<
    DownloadResult,
    NotFoundError | ExpiredError | StorageError | MetaError
  >;
  readonly remove: (id: string) => Effect.Effect<void, NotFoundError | StorageError | MetaError>;
}

export class FileTransfer extends Context.Tag("trail/FileTransfer")<
  FileTransfer,
  FileTransferService
>() {}

const randomId = () => globalThis.crypto.randomUUID().replaceAll("-", "").slice(0, 16);

const sanitizeFilename = (name: string) => {
  const cleaned = name.replace(/[/\\<>:"|?*\u0000-\u001f]/g, "").trim();
  return cleaned.length > 0 ? cleaned.slice(0, 200) : "file";
};

const isExpired = (record: TransferRecord, now: number) =>
  Date.parse(record.expiresAt) < now;

export const FileTransferLive = Layer.effect(
  FileTransfer,
  Effect.gen(function* () {
    const storage = yield* FileStorage;
    const meta = yield* TransferMeta;
    const config = yield* AppConfig;

    const upload = (input: UploadInput) =>
      Effect.gen(function* () {
        if (input.content.byteLength > config.maxUploadBytes) {
          return yield* new UploadTooLargeError({
            size: input.content.byteLength,
            maxBytes: config.maxUploadBytes,
          });
        }
        const id = randomId();
        const now = new Date();
        const record: TransferRecord = {
          id,
          filename: sanitizeFilename(input.filename),
          contentType: input.contentType || "application/octet-stream",
          size: input.content.byteLength,
          uploadedAt: now.toISOString(),
          expiresAt: new Date(now.getTime() + config.ttlMs).toISOString(),
          downloads: 0,
        };
        yield* storage.put(id, input.content, input.contentType || "application/octet-stream");
        yield* meta.upsert(record);
        return record;
      });

    const list = () => meta.list();

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
        return { kind: "bytes" as const, record: counted, content };
      });

    const remove = (id: string) =>
      Effect.gen(function* () {
        const record = yield* meta.find(id);
        if (record === undefined) return yield* new NotFoundError({ id });
        yield* storage.remove(id);
        yield* meta.remove(id);
      });

    return { upload, list, find, download, remove } satisfies FileTransferService;
  }),
);
