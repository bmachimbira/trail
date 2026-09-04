import { FileSystem, Path } from "@effect/platform";
import { Context, Effect, Layer, Stream } from "effect";
import { AppConfig } from "./config.js";
import { NotFoundError, StorageError, UploadTooLargeError } from "./errors.js";
import type { TransferRecord } from "./meta.js";

export type ByteStream = Stream.Stream<Uint8Array, unknown>;

/** Keep the upload bounded without collecting any chunks in memory. */
export const limitStream = <E>(
  content: Stream.Stream<Uint8Array, E>,
  maxBytes: number,
): Stream.Stream<Uint8Array, E | UploadTooLargeError> =>
  content.pipe(
    Stream.mapAccumEffect(0, (size, chunk) => {
      const nextSize = size + chunk.byteLength;
      return nextSize > maxBytes
        ? Effect.fail(new UploadTooLargeError({ size: nextSize, maxBytes }))
        : Effect.succeed([nextSize, chunk] as const);
    }),
  );

export interface FileStorageService {
  /** Write the blob for `id`, replacing any previous content. */
  readonly put: (
    id: string,
    content: ByteStream,
    contentType?: string,
  ) => Effect.Effect<number, StorageError | UploadTooLargeError>;
  /** Read the blob for `id`. */
  readonly get: (
    id: string,
  ) => Effect.Effect<Stream.Stream<Uint8Array, StorageError>, StorageError | NotFoundError>;
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

    const toPutError = (cause: unknown): StorageError | UploadTooLargeError =>
      cause instanceof UploadTooLargeError ? cause : new StorageError({ op: "write", cause });

    const put = (id: string, content: ByteStream) => {
      let size = 0;
      const tempPath = blobPath(`.${id}.${globalThis.crypto.randomUUID()}.tmp`);
      const bounded = limitStream(content, config.maxUploadBytes).pipe(
        Stream.tap((chunk) =>
          Effect.sync(() => {
            size += chunk.byteLength;
          }),
        ),
      );

      return Stream.run(bounded, fs.sink(tempPath)).pipe(
        Effect.andThen(fs.rename(tempPath, blobPath(id))),
        Effect.map(() => size),
        Effect.mapError(toPutError),
        Effect.onError(() => fs.remove(tempPath, { force: true }).pipe(Effect.ignoreLogged)),
      );
    };

    const get = (
      id: string,
    ): Effect.Effect<Stream.Stream<Uint8Array, StorageError>, StorageError | NotFoundError> =>
      safeId(id)
        ? fs.exists(blobPath(id)).pipe(
            Effect.mapError((cause) => new StorageError({ op: "read", cause })),
            Effect.andThen((exists) =>
              exists
                ? Effect.succeed(
                    fs
                      .stream(blobPath(id), { chunkSize: 64 * 1024 })
                      .pipe(Stream.mapError((cause) => new StorageError({ op: "read", cause }))),
                  )
                : Effect.fail(new NotFoundError({ id })),
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
