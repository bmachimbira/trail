import { FileSystem, Path } from "@effect/platform";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "./config.js";
import { NotFoundError, StorageError } from "./errors.js";
import type { TransferRecord } from "./meta.js";

export interface FileStorageService {
  /** Write the blob for `id`, replacing any previous content. */
  readonly put: (
    id: string,
    content: Uint8Array,
    contentType?: string,
  ) => Effect.Effect<void, StorageError>;
  /** Read the blob for `id`. */
  readonly get: (id: string) => Effect.Effect<Uint8Array, StorageError | NotFoundError>;
  /** Delete the blob for `id`; succeeds even if the blob is already gone. */
  readonly remove: (id: string) => Effect.Effect<void, StorageError>;
  /**
   * A temporary direct-download URL for the blob (presigned S3 GET), or
   * undefined when the driver serves bytes through the app (local disk).
   */
  readonly presign: (record: TransferRecord) => Effect.Effect<string | undefined, StorageError>;
}

export class FileStorage extends Context.Tag("trail/FileStorage")<
  FileStorage,
  FileStorageService
>() {}

export const FileStorageLive = Layer.effect(
  FileStorage,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* AppConfig;

    const dir = path.resolve(config.uploadDir);
    yield* fs.makeDirectory(dir, { recursive: true }).pipe(Effect.orDie);

    const blobPath = (id: string) => path.join(dir, id);
    const safeId = (id: string) => /^[A-Za-z0-9_-]+$/.test(id);

    const put = (id: string, content: Uint8Array) =>
      fs
        .writeFile(blobPath(id), content)
        .pipe(Effect.mapError((cause) => new StorageError({ op: "write", cause })));

    const get = (id: string): Effect.Effect<Uint8Array, StorageError | NotFoundError> =>
      safeId(id)
        ? fs
            .readFile(blobPath(id))
            .pipe(
              Effect.mapError((cause) =>
                cause._tag === "SystemError" && cause.reason === "NotFound"
                  ? new NotFoundError({ id })
                  : new StorageError({ op: "read", cause }),
              ),
            )
        : Effect.fail(new NotFoundError({ id }));

    const remove = (id: string) =>
      fs
        .remove(blobPath(id), { force: true })
        .pipe(Effect.mapError((cause) => new StorageError({ op: "remove", cause })));

    const presign = () => Effect.succeed(undefined);

    return { put, get, remove, presign } satisfies FileStorageService;
  }),
);
