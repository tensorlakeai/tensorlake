export const SDK_VERSION = "0.4.49";

export const API_URL =
  process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai";
export const API_KEY = process.env.TENSORLAKE_API_KEY ?? undefined;
export const NAMESPACE = process.env.INDEXIFY_NAMESPACE ?? "default";
export const SANDBOX_PROXY_URL =
  process.env.TENSORLAKE_SANDBOX_PROXY_URL ?? "https://sandbox.tensorlake.ai";

export const DEFAULT_HTTP_TIMEOUT_MS = 30_000;
export const MAX_RETRIES = 3;
export const RETRY_BACKOFF_MS = 500;
export const RETRYABLE_STATUS_CODES = new Set([429, 502, 503, 504]);
