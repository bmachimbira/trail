import { Data } from "effect";

/** Blob could not be read from or written to disk. */
export class StorageError extends Data.TaggedError("StorageError")<{
  readonly op: "read" | "write" | "remove";
  readonly cause: unknown;
}> {}

/** Metadata store read/write failure. */
export class MetaError extends Data.TaggedError("MetaError")<{
  readonly op: "read" | "write";
  readonly cause: unknown;
}> {}

export class NotFoundError extends Data.TaggedError("NotFoundError")<{
  readonly id: string;
}> {}

export class ExpiredError extends Data.TaggedError("ExpiredError")<{
  readonly id: string;
}> {}

export class UploadTooLargeError extends Data.TaggedError("UploadTooLargeError")<{
  readonly size: number;
  readonly maxBytes: number;
}> {}

export type TransferError =
  | StorageError
  | MetaError
  | NotFoundError
  | ExpiredError
  | UploadTooLargeError;
