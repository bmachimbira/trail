import type { APIRoute } from "astro";
import { Effect } from "effect";
import { errorToResponse, isTransferError } from "../../../lib/http.js";
import { runApp } from "../../../lib/runtime.js";
import { sessionId } from "../../../lib/session.js";
import { FileTransfer } from "../../../lib/transfer.js";

export const DELETE: APIRoute = async ({ params, cookies }) => {
  const id = params.id ?? "";
  const owner = sessionId(cookies);
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.removeBundle(id, owner);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);
  return new Response(null, { status: 204 });
};
