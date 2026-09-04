import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { NodeContext } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";
import { AppConfig } from "../src/lib/config.js";
import { KeyValueStore, type KeyValueStoreService, LocalKVLive } from "../src/lib/kv.js";

const makeLive = (dir: string) => {
  const ConfigTest = Layer.succeed(AppConfig, {
    uploadDir: dir,
    maxUploadBytes: 1024,
    adoptOrphansTo: undefined,
    ttlMs: 60_000,
    sweepIntervalMs: 60_000,
    storageDriver: "local" as const,
    s3: {
      bucket: "unused",
      region: "us-east-1",
      endpoint: undefined,
      pathStyle: false,
      prefix: "trail",
      downloadUrlTtlMs: 3_600_000,
    },
  });
  return LocalKVLive.pipe(Layer.provide(ConfigTest), Layer.provide(NodeContext.layer));
};

const runWith = <A, E>(
  dir: string,
  eff: (kv: KeyValueStoreService) => Effect.Effect<A, E, KeyValueStore>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const kv = yield* KeyValueStore;
      return yield* eff(kv);
    }).pipe(Effect.provide(makeLive(dir)), Effect.scoped),
  );

describe("LocalKVLive", () => {
  it("returns undefined for a missing key and round-trips a written one", async () => {
    const dir = mkdtempSync(join(tmpdir(), "trail-kv-"));
    try {
      const result = await runWith(dir, (kv) =>
        Effect.gen(function* () {
          const missing = yield* kv.read("meta.json");
          yield* kv.write("meta.json", "[]");
          const found = yield* kv.read("meta.json");
          return { missing, found };
        }),
      );
      expect(result.missing).toBeUndefined();
      expect(result.found).toBe("[]");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });

  it("writes atomically: repeated writes leave no tmp files behind", async () => {
    const dir = mkdtempSync(join(tmpdir(), "trail-kv-"));
    try {
      await runWith(dir, (kv) =>
        Effect.gen(function* () {
          yield* kv.write("meta.json", "[]");
          yield* kv.write("meta.json", '[{"id":"x"}]');
          yield* kv.write("meta.json", "[]");
        }),
      );
      expect(readdirSync(dir)).toEqual(["meta.json"]);
      expect(readFileSync(join(dir, "meta.json"), "utf8")).toBe("[]");
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});
