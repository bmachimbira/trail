import { HttpServerRequest, Multipart } from "@effect/platform";
import type { APIRoute } from "astro";
import { Effect, Stream } from "effect";
import { StorageError } from "../../lib/errors.js";
import { errorToResponse, isTransferError, json } from "../../lib/http.js";
import type { TransferRecord } from "../../lib/meta.js";
import { runApp } from "../../lib/runtime.js";
import { FileTransfer } from "../../lib/transfer.js";

export const POST: APIRoute = async ({ request }) => {
  if (
    !(request.headers.get("content-type") ?? "").toLowerCase().startsWith("multipart/form-data")
  ) {
    return json({ error: "invalid_form" }, 400);
  }

  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    const incoming = HttpServerRequest.fromWeb(request);
    const parts = incoming.multipartStream.pipe(
      Multipart.withLimitsStream({ maxFieldSize: 64 * 1024 }),
      Stream.mapError((cause) => new StorageError({ op: "read", cause })),
    );

    return yield* parts.pipe(
      Stream.runFoldEffect(undefined as TransferRecord | undefined, (uploaded, part) => {
        if (Multipart.isField(part)) return Effect.succeed(uploaded);
        if (uploaded !== undefined || part.key !== "file") {
          return part.content.pipe(
            Stream.runDrain,
            Effect.mapError((cause) => new StorageError({ op: "read", cause })),
            Effect.as(uploaded),
          );
        }
        return transfer.upload({
          filename: part.name,
          contentType: part.contentType,
          content: part.content,
        });
      }),
    );
  });

  // A truncated body leaves @effect/platform's multipart channel re-pumping an
  // ended mailbox forever (100% CPU, orphaned .tmp). Astro aborts the signal
  // when the socket closes, which interrupts the fiber and runs put()'s cleanup.
  const outcome = await runApp(effect, { signal: request.signal }).catch((cause: unknown) => {
    if (request.signal.aborted) return new StorageError({ op: "read", cause });
    throw cause;
  });
  if (isTransferError(outcome) && outcome._tag === "StorageError" && outcome.op === "read") {
    return json({ error: "invalid_form" }, 400);
  }
  if (isTransferError(outcome)) return errorToResponse(outcome);
  if (outcome === undefined) return json({ error: "missing_file" }, 400);

  return json({ ...outcome, downloadUrl: `/api/files/${outcome.id}` }, 201);
};
