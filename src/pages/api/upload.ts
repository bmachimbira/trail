import type { APIRoute } from "astro";
import { Effect } from "effect";
import { errorToResponse, isTransferError, json } from "../../lib/http.js";
import { runApp } from "../../lib/runtime.js";
import { FileTransfer } from "../../lib/transfer.js";

export const POST: APIRoute = async ({ request }) => {
  let form: FormData;
  try {
    form = await request.formData();
  } catch {
    return json({ error: "invalid_form" }, 400);
  }

  const file = form.get("file");
  if (!(file instanceof File)) {
    return json({ error: "missing_file" }, 400);
  }

  const content = new Uint8Array(await file.arrayBuffer());
  const effect = Effect.gen(function* () {
    const transfer = yield* FileTransfer;
    return yield* transfer.upload({
      filename: file.name,
      contentType: file.type,
      content,
    });
  });

  const outcome = await runApp(effect);
  if (isTransferError(outcome)) return errorToResponse(outcome);

  return json({ ...outcome, downloadUrl: `/api/files/${outcome.id}` }, 201);
};
