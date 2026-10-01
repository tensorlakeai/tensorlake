declare const __SDK_VERSION__: string;
export const SDK_VERSION: string = __SDK_VERSION__;

export const API_URL =
  process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai";
export const API_KEY = process.env.TENSORLAKE_API_KEY ?? undefined;
export const NAMESPACE = process.env.INDEXIFY_NAMESPACE ?? "default";
export const SANDBOX_PROXY_URL =
  process.env.TENSORLAKE_SANDBOX_PROXY_URL ?? "https://sandbox.tensorlake.ai";

export const DEFAULT_HTTP_TIMEOUT_MS = 300_000;
export const MAX_RETRIES = 3;
export const RETRY_BACKOFF_MS = 500;

// Upper bound on pages `list()` follows. Guards against an infinite loop if
// the server ever repeats a cursor. 10,000 pages is far more than any real
// namespace needs (the CLI's `tl sbx ls` uses the same bound).
export const MAX_LIST_PAGES = 10_000;
