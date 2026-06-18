/** Base exception for all sandbox-related errors. */
export class SandboxException extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SandboxException";
  }
}

/** General sandbox operation error. */
export class SandboxError extends SandboxException {
  constructor(message: string) {
    super(message);
    this.name = "SandboxError";
  }
}

/**
 * Raised when the client cannot complete a request against the API server.
 *
 * The original transport error (typically undici's `TypeError: fetch failed`,
 * whose real reason — `ECONNREFUSED`, `UND_ERR_CONNECT_TIMEOUT`, `EMFILE`, … —
 * lives in `.cause`) is preserved on this error's {@link cause} for programmatic
 * inspection, and its full chain is folded into the message so it survives
 * wrappers that only forward `error.message`. No interpretation of the failure
 * (client vs server) is applied — the raw inner error is surfaced as-is so the
 * reader can judge.
 */
export class SandboxConnectionError extends SandboxError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(`Connection error: ${message}`);
    this.name = "SandboxConnectionError";
    if (options?.cause !== undefined) this.cause = options.cause;
  }
}

/**
 * Flatten an error and its `cause` chain into a single readable line, appending
 * each level's `code` when it is not already present in the message. undici
 * reports `fetch failed` at the top and the real reason one or more levels down
 * in `.cause`, so this surfaces the part that actually identifies the failure.
 *
 * @example "fetch failed: connect ECONNREFUSED 10.0.0.1:443"
 * @example "fetch failed: Connect Timeout Error (UND_ERR_CONNECT_TIMEOUT)"
 */
export function describeError(err: unknown): string {
  const parts: string[] = [];
  const seen = new Set<unknown>();
  let current: unknown = err;

  for (let depth = 0; depth < 6; depth++) {
    if (current == null || typeof current !== "object" || seen.has(current)) {
      break;
    }
    seen.add(current);
    const e = current as { message?: unknown; code?: unknown; cause?: unknown };
    const message = typeof e.message === "string" ? e.message.trim() : "";
    const code = typeof e.code === "string" ? e.code : undefined;

    let segment = message;
    if (code && (!message || !message.includes(code))) {
      segment = segment ? `${segment} (${code})` : code;
    }
    if (segment && !parts.includes(segment)) parts.push(segment);

    current = e.cause;
  }

  if (parts.length > 0) return parts.join(": ");
  return err instanceof Error ? err.message : String(err);
}

/** Raised when a sandbox is not found. */
export class SandboxNotFoundError extends SandboxError {
  readonly sandboxId: string;

  constructor(sandboxId: string) {
    super(`Sandbox not found: ${sandboxId}`);
    this.name = "SandboxNotFoundError";
    this.sandboxId = sandboxId;
  }
}

/** Raised when a sandbox pool is not found. */
export class PoolNotFoundError extends SandboxError {
  readonly poolId: string;

  constructor(poolId: string) {
    super(`Sandbox pool not found: ${poolId}`);
    this.name = "PoolNotFoundError";
    this.poolId = poolId;
  }
}

/** Raised when attempting to delete a pool that is in use. */
export class PoolInUseError extends SandboxError {
  readonly poolId: string;

  constructor(poolId: string, message?: string) {
    const base = `Cannot delete pool ${poolId}: pool is in use`;
    super(message ? `${base} - ${message}` : base);
    this.name = "PoolInUseError";
    this.poolId = poolId;
  }
}

/** Raised when the remote API returns an error. */
export class RemoteAPIError extends SandboxError {
  readonly statusCode: number;
  readonly responseMessage: string;

  constructor(statusCode: number, message: string) {
    super(`API error (status ${statusCode}): ${message}`);
    this.name = "RemoteAPIError";
    this.statusCode = statusCode;
    this.responseMessage = message;
  }
}

/** Raised when request output is fetched before the request has completed. */
export class RequestNotFinishedError extends Error {
  constructor() {
    super("Request has not finished yet");
    this.name = "RequestNotFinishedError";
  }
}

/** Raised when a request completed unsuccessfully. */
export class RequestFailedError extends Error {
  readonly failure: string;

  constructor(failure: string) {
    super(`Request failed: ${failure}`);
    this.name = "RequestFailedError";
    this.failure = failure;
  }
}

/** Raised when a request surfaced an application-level error. */
export class RequestExecutionError extends Error {
  readonly functionName?: string;

  constructor(message: string, functionName?: string) {
    super(
      functionName ? `Request error in ${functionName}: ${message}` : message,
    );
    this.name = "RequestExecutionError";
    this.functionName = functionName;
  }
}
