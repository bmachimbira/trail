import type { APIRoute } from "astro";
import { Effect } from "effect";
import { errorToResponse, isTransferError, json } from "../../lib/http.js";
import { runApp } from "../../lib/runtime.js";
import { FileTransfer } from "../../lib/transfer.js";

export const GET: APIRoute = async () => {
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.list();
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);

  return json({ files: outcome });
};
