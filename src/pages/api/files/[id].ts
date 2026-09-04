import type { APIRoute } from "astro";
import { Effect } from "effect";
import { contentDisposition, errorToResponse, isTransferError } from "../../../lib/http.js";
import { runApp } from "../../../lib/runtime.js";
import { FileTransfer } from "../../../lib/transfer.js";

export const GET: APIRoute = async ({ params }) => {
  const id = params.id ?? "";
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.download(id);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);

  if (outcome.kind === "redirect") {
    // Presigned S3 URL: the recipient downloads straight from the bucket.
    return new Response(null, {
      status: 302,
      headers: { Location: outcome.url, "Cache-Control": "private, no-store" },
    });
  }

  const { record, content } = outcome;
  // fs.readFile yields a non-shared ArrayBuffer; this cast is view-only.
  const body = content as unknown as Uint8Array<ArrayBuffer>;
  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": record.contentType || "application/octet-stream",
      "Content-Length": String(content.byteLength),
      "Content-Disposition": contentDisposition(record),
      "Cache-Control": "private, no-store",
    },
  });
};

export const DELETE: APIRoute = async ({ params }) => {
  const id = params.id ?? "";
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.remove(id);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);
  return new Response(null, { status: 204 });
};
