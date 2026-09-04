import { Config, Context, Effect, Layer } from "effect";

export interface S3Settings {
  readonly bucket: string;
  readonly region: string;
  readonly endpoint: string | undefined;
  readonly pathStyle: boolean;
  readonly prefix: string;
  /** How long a presigned download URL stays valid. */
  readonly downloadUrlTtlMs: number;
}

export interface AppConfigService {
  /** Directory where uploaded blobs and the metadata file live (local driver). */
  readonly uploadDir: string;
  /** Maximum accepted upload size in bytes. */
  readonly maxUploadBytes: number;
  /** How long a transfer stays downloadable, in milliseconds. */
  readonly ttlMs: number;
  /** How often expired transfers are reaped, in milliseconds. */
  readonly sweepIntervalMs: number;
  /** Blob storage backend: local filesystem or S3-compatible. */
  readonly storageDriver: "local" | "s3";
  readonly s3: S3Settings;
}

export class AppConfig extends Context.Tag("trail/AppConfig")<AppConfig, AppConfigService>() {}

export const AppConfigLive = Layer.effect(
  AppConfig,
  Effect.gen(function* () {
    const driver = yield* Config.string("STORAGE_DRIVER").pipe(
      Config.map((d) => (d === "s3" ? "s3" : "local")),
      Config.withDefault("local"),
    );
    const endpoint = yield* Config.option(Config.string("S3_ENDPOINT"));

    return {
      uploadDir: yield* Config.string("UPLOAD_DIR").pipe(Config.withDefault("./uploads")),
      maxUploadBytes: yield* Config.integer("MAX_UPLOAD_MB").pipe(
        Config.withDefault(100),
        Config.map((mb) => mb * 1024 * 1024),
      ),
      ttlMs: yield* Config.integer("TTL_HOURS").pipe(
        Config.withDefault(24),
        Config.map((hours) => hours * 60 * 60 * 1000),
      ),
      sweepIntervalMs: yield* Config.integer("SWEEP_INTERVAL_MINUTES").pipe(
        Config.withDefault(60),
        Config.map((m) => m * 60 * 1000),
      ),
      storageDriver: driver,
      s3: {
        bucket: yield* Config.string("S3_BUCKET").pipe(Config.withDefault("trail")),
        region: yield* Config.string("S3_REGION").pipe(Config.withDefault("us-east-1")),
        endpoint: endpoint._tag === "Some" ? endpoint.value : undefined,
        // Virtual-hosted addressing on real AWS; path-style for MinIO/R2/s3rver.
        pathStyle: yield* Config.string("S3_PATH_STYLE").pipe(
          Config.map((v) => v === "true"),
          Config.withDefault(endpoint._tag === "Some"),
        ),
        prefix: yield* Config.string("S3_PREFIX").pipe(Config.withDefault("trail")),
        downloadUrlTtlMs: yield* Config.integer("DOWNLOAD_URL_TTL_MINUTES").pipe(
          Config.withDefault(60),
          Config.map((m) => m * 60 * 1000),
        ),
      },
    };
  }),
);
