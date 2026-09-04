import { FileSystem, Path } from "@effect/platform";
import { Context, Effect, Layer } from "effect";
import { AppConfig } from "./config.js";
import { MetaError } from "./errors.js";

/**
 * Tiny durable key-value store: the metadata layer persists `meta.json`
 * through this tag, to local disk or to the S3 bucket depending on driver.
 */
export interface KeyValueStoreService {
  readonly read: (key: string) => Effect.Effect<string | undefined, MetaError>;
  readonly write: (key: string, content: string) => Effect.Effect<void, MetaError>;
}

export class KeyValueStore extends Context.Tag("trail/KeyValueStore")<
  KeyValueStore,
  KeyValueStoreService
>() {}

export const LocalKVLive = Layer.effect(
  KeyValueStore,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* AppConfig;
    const dir = path.resolve(config.uploadDir);
    const fileFor = (key: string) => path.join(dir, key);

    const read = (key: string) =>
      fs
        .exists(fileFor(key))
        .pipe(
          Effect.mapError((cause) => new MetaError({ op: "read", cause })),
          Effect.andThen((exists) =>
            exists
              ? fs
                  .readFileString(fileFor(key))
                  .pipe(Effect.mapError((cause) => new MetaError({ op: "read", cause })))
              : Effect.succeed(undefined),
          ),
        );

    const write = (key: string, content: string) =>
      fs
        .writeFileString(fileFor(key), content)
        .pipe(Effect.mapError((cause) => new MetaError({ op: "write", cause })));

    return { read, write } satisfies KeyValueStoreService;
  }),
);
