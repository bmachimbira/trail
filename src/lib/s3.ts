import type { Readable } from "node:stream";
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Context, Effect, Layer } from "effect";
import { AppConfig, type AppConfigService } from "./config.js";
import { MetaError, NotFoundError, StorageError } from "./errors.js";
import { contentDisposition } from "./http.js";
import { KeyValueStore, type KeyValueStoreService } from "./kv.js";
import type { TransferRecord } from "./meta.js";
import { FileStorage, type FileStorageService } from "./storage.js";

/** One shared S3Client per process, built from AppConfig. */
export class S3ClientTag extends Context.Tag("trail/S3Client")<S3ClientTag, S3Client>() {}

export const S3ClientLive = Layer.effect(
  S3ClientTag,
  Effect.map(AppConfig, (config) => {
    if (config.storageDriver !== "s3") {
      // Selection layer guards this; building anyway keeps the layer total.
      return new S3Client({ region: config.s3.region });
    }
    return new S3Client({
      region: config.s3.region,
      endpoint: config.s3.endpoint,
      forcePathStyle: config.s3.pathStyle,
    });
  }),
);

const is404 = (cause: unknown): boolean =>
  cause instanceof Error &&
  (cause.name === "NoSuchKey" ||
    cause.name === "NotFound" ||
    (cause as { $metadata?: { httpStatusCode?: number } }).$metadata?.httpStatusCode === 404);

const toStorageError = (op: "read" | "write" | "remove") => (cause: unknown) =>
  new StorageError({ op, cause });

interface KeySpace {
  readonly bucket: string;
  readonly blobKey: (id: string) => string;
  readonly kvKey: (key: string) => string;
}

const keySpace = (config: AppConfigService): KeySpace => {
  const root = config.s3.prefix.replace(/^\/+|\/+$/g, "");
  return {
    bucket: config.s3.bucket,
    blobKey: (id) => `${root}/blobs/${id}`,
    kvKey: (key) => `${root}/${key}`,
  };
};

export const FileStorageS3Live = Layer.effect(
  FileStorage,
  Effect.gen(function* () {
    const client = yield* S3ClientTag;
    const config = yield* AppConfig;
    const ks = keySpace(config);

    const put = (id: string, content: Uint8Array, contentType?: string) =>
      Effect.tryPromise({
        try: () =>
          client.send(
            new PutObjectCommand({
              Bucket: ks.bucket,
              Key: ks.blobKey(id),
              Body: content,
              ContentType: contentType ?? "application/octet-stream",
            }),
          ),
        catch: toStorageError("write"),
      }).pipe(Effect.asVoid);

    const get = (id: string) =>
      Effect.gen(function* () {
        const out = yield* Effect.tryPromise({
          try: () => client.send(new GetObjectCommand({ Bucket: ks.bucket, Key: ks.blobKey(id) })),
          catch: (cause: unknown) =>
            is404(cause) ? new NotFoundError({ id }) : new StorageError({ op: "read", cause }),
        });
        if (out.Body === undefined) return yield* new NotFoundError({ id });
        const chunks = yield* Effect.tryPromise({
          try: () => (out.Body as Readable).toArray(),
          catch: (cause) => new StorageError({ op: "read", cause }),
        });
        const total = chunks.reduce((n, c) => n + c.byteLength, 0);
        const content = new Uint8Array(total);
        let offset = 0;
        for (const chunk of chunks) {
          content.set(chunk, offset);
          offset += chunk.byteLength;
        }
        return content;
      });

    const remove = (id: string) =>
      Effect.tryPromise({
        try: () => client.send(new DeleteObjectCommand({ Bucket: ks.bucket, Key: ks.blobKey(id) })),
        catch: toStorageError("remove"),
      }).pipe(Effect.asVoid);

    const presign = (record: TransferRecord) =>
      Effect.gen(function* () {
        const remaining = Date.parse(record.expiresAt) - Date.now();
        const ttl = Math.max(30_000, Math.min(config.s3.downloadUrlTtlMs, remaining));
        const command = new GetObjectCommand({
          Bucket: ks.bucket,
          Key: ks.blobKey(record.id),
          ResponseContentDisposition: contentDisposition(record),
          ResponseContentType: record.contentType || "application/octet-stream",
        });
        return yield* Effect.tryPromise({
          try: () => getSignedUrl(client, command, { expiresIn: Math.ceil(ttl / 1000) }),
          catch: (cause) => new StorageError({ op: "read", cause }),
        });
      });

    return {
      put,
      get,
      remove,
      presign,
    } satisfies FileStorageService;
  }),
);

export const S3KVLive = Layer.effect(
  KeyValueStore,
  Effect.gen(function* () {
    const client = yield* S3ClientTag;
    const config = yield* AppConfig;
    const ks = keySpace(config);

    const read = (key: string) =>
      Effect.gen(function* () {
        const out = yield* Effect.tryPromise({
          try: () => client.send(new GetObjectCommand({ Bucket: ks.bucket, Key: ks.kvKey(key) })),
          catch: (cause) => new MetaError({ op: "read", cause }),
        });
        const body = out.Body;
        if (body === undefined) return yield* new MetaError({ op: "read", cause: "empty body" });
        return yield* Effect.tryPromise({
          try: () => body.transformToString("utf8"),
          catch: (cause) => new MetaError({ op: "read", cause }),
        });
      }).pipe(
        Effect.catchIf(
          (e) => e instanceof MetaError && is404(e.cause),
          () => Effect.succeed(undefined),
        ),
      );

    const write = (key: string, content: string) =>
      Effect.tryPromise({
        try: () =>
          client.send(
            new PutObjectCommand({ Bucket: ks.bucket, Key: ks.kvKey(key), Body: content }),
          ),
        catch: (cause) => new MetaError({ op: "write", cause }),
      }).pipe(Effect.asVoid);

    return { read, write } satisfies KeyValueStoreService;
  }),
);
