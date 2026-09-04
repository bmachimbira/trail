import type { TransferError } from "./errors.js";
import type { TransferRecord } from "./meta.js";

const errorTags = new Set([
  "NotFoundError",
  "ExpiredError",
  "UploadTooLargeError",
  "StorageError",
  "MetaError",
]);

export const isTransferError = (u: unknown): u is TransferError =>
  typeof u === "object" &&
  u !== null &&
  "_tag" in u &&
  typeof (u as { _tag: unknown })._tag === "string" &&
  errorTags.has((u as { _tag: string })._tag);

export const json = (body: unknown, status = 200): Response =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json; charset=utf-8" },
  });

export const errorToResponse = (error: TransferError): Response => {
  switch (error._tag) {
    case "NotFoundError":
      return json({ error: "not_found", id: error.id }, 404);
    case "ExpiredError":
      return json({ error: "expired", id: error.id }, 410);
    case "UploadTooLargeError":
      return json({ error: "payload_too_large", size: error.size, maxBytes: error.maxBytes }, 413);
    case "StorageError":
    case "MetaError":
      return json({ error: "internal_error", op: error.op }, 500);
  }
};

export const contentDisposition = (record: TransferRecord): string => {
  const fallback = record.filename.replace(/[^\x20-\x7e]/g, "_").replace(/["\\]/g, "_");
  return `attachment; filename="${fallback}"; filename*=UTF-8''${encodeURIComponent(record.filename)}`;
};
