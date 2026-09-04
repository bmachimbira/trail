declare module "s3rver" {
  import type { AddressInfo } from "node:net";
  export interface S3rverBucket {
    name: string;
  }
  export interface S3rverOptions {
    directory?: string;
    address?: string;
    port?: number;
    silent?: boolean;
    configureBuckets?: S3rverBucket[];
  }
  export default class S3rver {
    constructor(options: S3rverOptions);
    /** Starts the server and resolves to its bound address. */
    run(): Promise<AddressInfo>;
    close(): Promise<void>;
  }
}
