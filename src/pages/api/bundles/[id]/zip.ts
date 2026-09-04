import type { APIRoute } from "astro";
import { Effect, Stream } from "effect";
import { errorToResponse, isTransferError } from "../../../../lib/http.js";
import { runApp } from "../../../../lib/runtime.js";
import { FileTransfer } from "../../../../lib/transfer.js";

/** Every file in the bundle as one stored zip; public by link, like the share page. */
export const GET: APIRoute = async ({ params }) => {
  const id = params.id ?? "";
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.downloadBundle(id);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);

  return new Response(Stream.toReadableStream(outcome.content), {
    status: 200,
    headers: {
      "Content-Type": "application/zip",
      "Content-Disposition": `attachment; filename="trail-${id}.zip"`,
      "Cache-Control": "private, no-store",
    },
  });
};
