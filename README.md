# Trail

A self-hosted file transfer service: upload a file, get a shareable link, and the
link expires automatically. Built with **Astro** (SSR pages + API routes) and
**Effect** (typed service layer with dependency injection via layers).

## Features

- Drag-and-drop or click-to-pick uploads with progress
- Shareable download pages (`/f/<id>`) with file details
- Auto-expiring transfers (default 24 h) — expired links return `410 Gone`
- Download counter, delete, recent-uploads list
- Metadata persisted to disk (`meta.json`), blobs stored on the filesystem
- CSRF origin checking (Astro `checkOrigin`) with configurable allowed hosts

## Run

```sh
npm install
npm run dev        # dev server on http://localhost:4321
```

Production:

```sh
npm run build
npm start          # serves the built app (HOST/PORT env respected)
```

## Configuration (environment variables)

| Variable                  | Default     | Meaning                                                |
| ------------------------- | ----------- | ------------------------------------------------------ |
| `UPLOAD_DIR`              | `./uploads` | Local driver: where blobs and `meta.json` are stored    |
| `MAX_UPLOAD_MB`           | `100`       | Upload size limit                                       |
| `TTL_HOURS`               | `24`        | Hours until a transfer expires                          |
| `STORAGE_DRIVER`          | `local`     | `local` (filesystem) or `s3`                            |
| `S3_BUCKET`               | `trail`     | S3 driver: bucket name                                  |
| `S3_REGION`               | `us-east-1` | S3 driver: signing region                               |
| `S3_ENDPOINT`             | —           | S3 driver: custom endpoint (MinIO, Cloudflare R2, …)    |
| `S3_PATH_STYLE`           | auto        | Path-style addressing (`true` for MinIO/R2/s3rver)      |
| `S3_PREFIX`               | `trail`     | S3 driver: key prefix (`<prefix>/blobs/<id>`, `meta.json`) |
| `DOWNLOAD_URL_TTL_MINUTES`| `60`        | S3 driver: presigned download URL lifetime (capped by transfer TTL) |
| `ALLOWED_HOSTS`           | —           | Extra trusted hostnames (comma-separated) for Astro's origin check |

AWS credentials come from the default SDK chain (`AWS_ACCESS_KEY_ID` etc.,
instance profiles, …) — never put secrets in the repo.

### S3 mode

With `STORAGE_DRIVER=s3`, blobs and `meta.json` live in the bucket and
downloads redirect (302) to short-lived presigned URLs, so file bytes stream
from S3 straight to recipients — the app server never touches them. The
download counter is still incremented server-side at redirect time.

Works with anything S3-compatible: AWS S3, MinIO (`S3_ENDPOINT=http://…`,
`S3_PATH_STYLE=true`), Cloudflare R2. Expired transfers are gated at read
time; to reclaim the storage, add a bucket lifecycle rule matching your TTL:

```sh
aws s3api put-bucket-lifecycle-configuration --bucket <bucket> --lifecycle-configuration '{
  "Rules": [{ "Filter": { "Prefix": "trail/blobs/" }, "Status": "Enabled",
              "ID": "trail-expiry", "Expiration": { "Days": 2 } }]
}'
```

Uploads still pass through the app server (multipart form, bounded by
`MAX_UPLOAD_MB`); direct browser→S3 presigned PUTs are the natural next step.

## HTTP API

| Method   | Path              | Description                                        |
| -------- | ----------------- | -------------------------------------------------- |
| `POST`   | `/api/upload`     | Multipart form field `file` → 201 + transfer record |
| `GET`    | `/api/files`      | List transfers, newest first                        |
| `GET`    | `/api/files/<id>` | Download the file (`404` missing, `410` expired)    |
| `DELETE` | `/api/files/<id>` | Delete blob + metadata (`204`, `404` if missing)    |

Mutating requests must pass Astro's origin check: browsers send this
automatically; for `curl` add `-H "Origin: http://<host>:<port>"`.

Example:

```sh
curl -H "Origin: http://127.0.0.1:4400" -F "file=@report.pdf" http://127.0.0.1:4400/api/upload
```

## Design

Warm paper minimal — cream ground, warm ink, one terracotta accent. Type is
Fraunces (display serif) + Karla (UI sans), self-hosted via `@fontsource-variable`
(no external font CDN). Design tokens live in `src/layouts/Layout.astro`
(OKLCH custom properties); the guiding context is in `.impeccable.md`.

## Architecture

```
src/
  lib/
    config.ts    AppConfig service        — Effect Config (env-driven), Layer
    storage.ts   FileStorage service      — local-disk blob put/get/remove
    s3.ts        S3 driver                — bucket-backed FileStorage + presigned GETs + KV
    kv.ts        KeyValueStore tag        — meta.json persistence (fs or S3)
    meta.ts      TransferMeta service     — JSON-file metadata store, semaphore-guarded
    transfer.ts  FileTransfer service     — upload/list/find/download/remove orchestration
    runtime.ts   AppLive layer composition + shared ManagedRuntime + runApp boundary
    errors.ts    Tagged errors (NotFound, Expired, Storage, Meta, UploadTooLarge)
    http.ts      Error→HTTP mapping, Content-Disposition helpers
  pages/
    index.astro        Upload UI (dropzone, progress, list)
    f/[id].astro       SSR share page
    api/*.ts           API routes (thin: runApp + isTransferError narrowing)
test/
  transfer.test.ts     Vitest suite over real temp dirs (local driver)
  s3.test.ts           Integration suite: in-process s3rver + real route handlers
```

The whole backend is plain Effect: services are `Context.Tag`s wired by layers
(`src/lib/runtime.ts`), so tests swap config/layers freely and Astro routes stay
thin adapters. Errors flow through the channel and are narrowed to HTTP status
codes at the boundary (`isTransferError`).

## Tests

```sh
npm test
```

## Notes / limits

- Single-process store (metadata cached in memory, persisted per mutation).
- Downloads are buffered, not streamed — `MAX_UPLOAD_MB` doubles as the
  practical memory bound. Streaming via `Stream` is a natural next step.
- The in-memory cache relies on layer memoization: one `ManagedRuntime` per
  process is shared by all routes (global singleton in `runtime.ts`).
