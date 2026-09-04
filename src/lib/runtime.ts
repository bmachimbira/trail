import { NodeContext } from "@effect/platform-node";
import { Effect, Layer, ManagedRuntime } from "effect";
import type { ConfigError } from "effect/ConfigError";
import { AppConfig, AppConfigLive } from "./config.js";
import type { TransferError } from "./errors.js";
import { LocalKVLive, KeyValueStore } from "./kv.js";
import { TransferMeta, TransferMetaLive } from "./meta.js";
import { FileStorage, FileStorageLive } from "./storage.js";
import { FileStorageS3Live, S3ClientLive, S3KVLive } from "./s3.js";
import { FileTransfer, FileTransferLive } from "./transfer.js";

// Storage driver picked by STORAGE_DRIVER: both stacks satisfy the same
// service tags. With S3, blobs, presigned redirects and meta.json live in
// the bucket; the local stack stays on the filesystem.
const S3Stack = Layer.mergeAll(FileStorageS3Live, S3KVLive).pipe(
  Layer.provide(S3ClientLive),
);
const LocalStack = Layer.mergeAll(FileStorageLive, LocalKVLive);

const StorageStack = Layer.unwrapEffect(
  Effect.map(AppConfig, (config) =>
    config.storageDriver === "s3" ? S3Stack : LocalStack,
  ).pipe(Effect.provide(AppConfigLive)),
);

/** Every service tag an app-level effect may require. */
export type AppServices =
  | FileTransfer
  | TransferMeta
  | FileStorage
  | KeyValueStore
  | AppConfig;

export const AppLive = Layer.mergeAll(
  FileTransferLive.pipe(Layer.provide(TransferMetaLive)),
  TransferMetaLive,
).pipe(
  Layer.provideMerge(StorageStack),
  Layer.provideMerge(AppConfigLive),
  Layer.provide(NodeContext.layer),
);

type AppRuntime = ManagedRuntime.ManagedRuntime<AppServices, ConfigError>;

// Astro dev can load a module graph per route; a global singleton keeps one
// metadata cache/semaphore for the whole process.
const store = globalThis as unknown as { __trailRuntime?: AppRuntime };
export const AppRuntime: AppRuntime = (store.__trailRuntime ??=
  ManagedRuntime.make(AppLive));

/**
 * Boundary helper for Astro routes/pages: runs an app effect on the shared
 * runtime, turning failures into a `TransferError` value on the success
 * channel so call sites can narrow with `isTransferError`.
 */
export const runApp = <A, E>(
  effect: Effect.Effect<A, E, AppServices>,
): Promise<A | TransferError> =>
  AppRuntime.runPromise(
    Effect.catchAll(
      effect as Effect.Effect<A, TransferError, AppServices>,
      (e) => Effect.succeed(e),
    ),
  );
