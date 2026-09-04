import { HttpServerRequest, Multipart } from "@effect/platform";
import type { APIRoute } from "astro";
import { Effect, Stream } from "effect";
import { StorageError } from "../../lib/errors.js";
import { errorToResponse, isTransferError, json, publicBundle } from "../../lib/http.js";
import type { TransferRecord } from "../../lib/meta.js";
import { runApp } from "../../lib/runtime.js";
import { sessionId } from "../../lib/session.js";
import { FileTransfer, newBundleId } from "../../lib/transfer.js";

export const POST: APIRoute = async ({ request, cookies }) => {
  if (
    !(request.headers.get("content-type") ?? "").toLowerCase().startsWith("multipart/form-data")
  ) {
    return json({ error: "invalid_form" }, 400);
  }

  const owner = sessionId(cookies);
  const bundleId = newBundleId();
  const uploaded: TransferRecord[] = [];
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    const incoming = HttpServerRequest.fromWeb(request);
    const parts = incoming.multipartStream.pipe(
      Multipart.withLimitsStream({ maxFieldSize: 64 * 1024 }),
      Stream.mapError((cause) => new StorageError({ op: "read", cause })),
    );
    const drain = (part: Multipart.File) =>
      part.content.pipe(
        Stream.runDrain,
        Effect.mapError((cause) => new StorageError({ op: "read", cause })),
      );

    return yield* parts.pipe(
      Stream.runForEach((part) => {
        if (Multipart.isField(part)) return Effect.void;
        if (part.key !== "file") return drain(part);
        return transfer
          .upload({
            filename: part.name,
            contentType: part.contentType,
            content: part.content,
            ownerId: owner,
            bundleId,
          })
          .pipe(
            Effect.map((record) => {
              uploaded.push(record);
            }),
          );
      }),
      Effect.as(uploaded),
      // One bad file sinks the whole bundle: never leave a half-uploaded link.
      Effect.tapError(() =>
        Effect.forEach(uploaded, (r) => transfer.remove(r.id, owner).pipe(Effect.ignoreLogged)),
      ),
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
  const first = outcome[0];
  if (first === undefined) return json({ error: "missing_file" }, 400);

  return json(
    publicBundle({ id: bundleId, uploadedAt: first.uploadedAt, expiresAt: first.expiresAt, files: outcome }),
    201,
  );
};
