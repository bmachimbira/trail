// Production entry. Astro's node adapter copies response bodies to the socket
// without honouring backpressure (NodeApp.writeResponse loops reader.read() →
// destination.write() and ignores the false return), so a multi-GB download to
// a slow or vanished client is buffered in RAM until the box OOMs. This gate
// makes each read wait for the socket to drain, and cancels the source stream
// when the client goes away. Check astro/dist/core/app/node.js on upgrades.
import { NodeApp } from "astro/app/node";

const writeUngated = NodeApp.writeResponse;

const drained = (destination) =>
  new Promise((resolve) => {
    const done = () => {
      destination.off("drain", done);
      destination.off("close", done);
      resolve();
    };
    destination.once("drain", done);
    destination.once("close", done);
  });

NodeApp.writeResponse = (source, destination) => {
  if (!source.body) return writeUngated(source, destination);
  const reader = source.body.getReader();
  const gated = new ReadableStream({
    async pull(controller) {
      if (destination.writableNeedDrain) await drained(destination);
      if (destination.destroyed || destination.writableEnded) {
        controller.close();
        await reader.cancel();
        return;
      }
      const { value, done } = await reader.read();
      if (done) controller.close();
      else controller.enqueue(value);
    },
    cancel: (reason) => reader.cancel(reason),
  });
  const gatedResponse = new Response(gated, {
    status: source.status,
    statusText: source.statusText,
    headers: source.headers,
  });
  return writeUngated(gatedResponse, destination);
};

await import("./dist/server/entry.mjs");
