import type { APIRoute } from "astro";
import { Effect, Stream } from "effect";
import { contentDisposition, errorToResponse, isTransferError } from "../../../lib/http.js";
import { runApp } from "../../../lib/runtime.js";
import { sessionId } from "../../../lib/session.js";
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
  return new Response(Stream.toReadableStream(content), {
    status: 200,
    headers: {
      "Content-Type": record.contentType || "application/octet-stream",
      "Content-Length": String(record.size),
      "Content-Disposition": contentDisposition(record),
      "Cache-Control": "private, no-store",
    },
  });
};

export const DELETE: APIRoute = async ({ params, cookies }) => {
  const id = params.id ?? "";
  const owner = sessionId(cookies);
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.remove(id, owner);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);
  return new Response(null, { status: 204 });
};
