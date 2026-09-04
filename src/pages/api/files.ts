import type { APIRoute } from "astro";
import { Effect } from "effect";
import { errorToResponse, isTransferError, json, publicBundle } from "../../lib/http.js";
import { runApp } from "../../lib/runtime.js";
import { sessionId } from "../../lib/session.js";
import { FileTransfer } from "../../lib/transfer.js";

export const GET: APIRoute = async ({ cookies }) => {
  const owner = sessionId(cookies);
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.listBundles(owner);
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);

  return json({ bundles: outcome.map(publicBundle) });
};
