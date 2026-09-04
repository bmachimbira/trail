import { Context, Effect, Layer } from "effect";
import { MetaError, NotFoundError } from "./errors.js";
import { KeyValueStore } from "./kv.js";

export interface TransferRecord {
  readonly id: string;
  readonly filename: string;
  readonly contentType: string;
  readonly size: number;
  /** ISO 8601 timestamp of the upload. */
  readonly uploadedAt: string;
  /** ISO 8601 timestamp after which the transfer is no longer downloadable. */
  readonly expiresAt: string;
  readonly downloads: number;
}

export interface TransferMetaService {
  readonly list: () => Effect.Effect<ReadonlyArray<TransferRecord>, MetaError>;
  readonly find: (id: string) => Effect.Effect<TransferRecord | undefined, MetaError>;
  readonly upsert: (record: TransferRecord) => Effect.Effect<void, MetaError>;
  /** Read-modify-write under the store's lock. */
  readonly update: (
    id: string,
    f: (record: TransferRecord) => TransferRecord,
  ) => Effect.Effect<TransferRecord, MetaError | NotFoundError>;
  readonly remove: (id: string) => Effect.Effect<void, MetaError>;
}

export class TransferMeta extends Context.Tag("trail/TransferMeta")<
  TransferMeta,
  TransferMetaService
>() {}

export const TransferMetaLive = Layer.effect(
  TransferMeta,
  Effect.gen(function* () {
    const kv = yield* KeyValueStore;
    const metaKey = "meta.json";

    // In-memory cache; the store is single-process, guarded by this semaphore.
    const cache: Map<string, TransferRecord> = new Map();
    let loaded = false;
    const semaphore = yield* Effect.makeSemaphore(1);

    const readAll: Effect.Effect<void, MetaError> = Effect.suspend(() => {
      if (loaded) return Effect.void;
      return Effect.gen(function* () {
        const raw = yield* kv.read(metaKey);
        if (raw !== undefined) {
          // A corrupt meta.json (e.g. crash mid-write) is a read error, not a defect.
          const records = yield* Effect.try({
            try: () => JSON.parse(raw) as TransferRecord[],
            catch: (cause) => new MetaError({ op: "read", cause }),
          });
          for (const record of records) cache.set(record.id, record);
        }
        loaded = true;
      });
    });

    // suspend: re-reads the cache at run time, not at layer build time.
    const persistAll: Effect.Effect<void, MetaError> = Effect.suspend(() =>
      kv.write(metaKey, JSON.stringify([...cache.values()], null, 2)),
    );

    const list = () =>
      readAll.pipe(
        Effect.andThen(() =>
          [...cache.values()].sort((a, b) => b.uploadedAt.localeCompare(a.uploadedAt)),
        ),
        semaphore.withPermits(1),
      );

    const find = (id: string) =>
      readAll.pipe(
        Effect.andThen(() => cache.get(id)),
        semaphore.withPermits(1),
      );

    const upsert = (record: TransferRecord) =>
      readAll.pipe(
        Effect.andThen(() => {
          cache.set(record.id, record);
          return persistAll;
        }),
        Effect.asVoid,
        semaphore.withPermits(1),
      );

    const update = (id: string, f: (record: TransferRecord) => TransferRecord) =>
      readAll.pipe(
        Effect.andThen(() => {
          const current = cache.get(id);
          if (current === undefined) return Effect.fail(new NotFoundError({ id }));
          const next = f(current);
          cache.set(id, next);
          return Effect.as(persistAll, next);
        }),
        semaphore.withPermits(1),
      );

    const remove = (id: string) =>
      readAll.pipe(
        Effect.andThen(() => {
          cache.delete(id);
          return persistAll;
        }),
        Effect.asVoid,
        semaphore.withPermits(1),
      );

    return { list, find, upsert, update, remove } satisfies TransferMetaService;
  }),
);
