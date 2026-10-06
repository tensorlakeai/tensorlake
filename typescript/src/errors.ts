import type { ContainerResourcesInfo, ResizeErrorReason, SandboxInfo } from "./models.js";
/** Base exception for all sandbox-related errors. */
export class SandboxException extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SandboxException";
  }
}

/**
 * General sandbox operation error.
 *
 * `reason` carries the server's reason when the error describes a sandbox
 * that failed or terminated: `no_capacity` when a `maxPendingSecs` bound
 * expired, `cancelled` when a pending sandbox was deleted, or a startup
 * reason such as `ConfigurationError`. It is undefined for errors that are
 * not about a sandbox's fate.
 */
export class SandboxError extends SandboxException {
  readonly reason?: string;
  readonly sandboxId?: string;

  constructor(
    message: string,
    options?: { reason?: string; sandboxId?: string },
  ) {
    super(message);
    this.name = "SandboxError";
    if (options?.reason !== undefined) this.reason = options.reason;
    if (options?.sandboxId !== undefined) this.sandboxId = options.sandboxId;
  }
}

/** A failed, timed-out, interrupted, or superseded resize, with confirmed allocation. */
export class SandboxResizeError extends SandboxError {
  declare readonly reason: ResizeErrorReason;
  readonly generation: number;
  readonly info?: SandboxInfo;

  constructor(payload: {
    sandboxId: string;
    generation: number;
    reason: ResizeErrorReason;
    message: string;
    info?: SandboxInfo | null;
  }) {
    const resources = payload.info?.resources;
    const allocation = resources
      ? `${resources.cpus} CPUs, ${resources.memoryMb} MiB memory, ${resources.diskMb} MiB disk`
      : "unavailable";
    super(
      `Sandbox ${payload.sandboxId} resize generation ${payload.generation}: ${payload.message}; last confirmed allocation: ${allocation}`,
      { reason: payload.reason, sandboxId: payload.sandboxId },
    );
    this.name = "SandboxResizeError";
    this.generation = payload.generation;
    this.info = payload.info ?? undefined;
  }

  /** Last confirmed allocation, if an observation was available. */
  get confirmedResources(): ContainerResourcesInfo | undefined {
    return this.info?.resources;
  }
}

/**
 * The wait budget ran out while the sandbox was still queued.
 *
 * Thrown by `Sandbox.create`, `createAndConnect`, `PendingSandbox.ready` and
 * a lazy `connect` when their timeout elapses before the sandbox is running.
 * **The sandbox is not deleted**: it keeps its place in the queue and starts
 * whenever capacity arrives. Call `ready()` again to keep waiting,
 * `Sandbox.connect` from any other process, or `delete` it to give up.
 * `pendingReason` is the last reason the scheduler recorded (`scheduling`,
 * `no_resources_available`, `pool_at_capacity`, ...).
 */
export class SandboxPending extends SandboxError {
  declare readonly sandboxId: string;
  readonly pendingReason?: string;
  readonly timeout?: number;

  constructor(
    sandboxId: string,
    options?: { pendingReason?: string; timeout?: number },
  ) {
    let message = `Sandbox ${sandboxId} is still pending`;
    if (options?.timeout !== undefined) message += ` after ${options.timeout}s`;
    if (options?.pendingReason) message += ` (${options.pendingReason})`;
    message +=
      "; it keeps its place in the queue. Wait again with ready() or " +
      `Sandbox.connect({ sandboxId: '${sandboxId}' }), or delete it to cancel.`;
    super(message, { reason: options?.pendingReason, sandboxId });
    this.name = "SandboxPending";
    this.pendingReason = options?.pendingReason;
    this.timeout = options?.timeout;
  }
}

/**
 * Raised when the client cannot complete a request against the API server.
 *
 * The original transport error is preserved on this error's {@link cause} for programmatic
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
 * each level's `code` when it is not already present in the message. Transport
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
  declare readonly sandboxId: string;

  constructor(sandboxId: string) {
    super(`Sandbox not found: ${sandboxId}`, { sandboxId });
    this.name = "SandboxNotFoundError";
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

export function formatErrorDetails(errorDetails: unknown): string | undefined {
  if (errorDetails == null) return undefined;
  if (typeof errorDetails === "string") {
    const detail = errorDetails.trim();
    return detail || undefined;
  }
  if (Array.isArray(errorDetails)) {
    const parts = errorDetails
      .map((item) => formatErrorDetails(item))
      .filter((item): item is string => Boolean(item));
    return parts.length > 0 ? parts.join("; ") : JSON.stringify(errorDetails);
  }
  if (typeof errorDetails === "object") {
    for (const key of ["message", "detail", "error", "reason"]) {
      const value = (errorDetails as Record<string, unknown>)[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
    return JSON.stringify(errorDetails);
  }
  return String(errorDetails);
}

/** Raised when the remote API returns an error. */
export class RemoteAPIError extends SandboxError {
  readonly statusCode: number;
  /** Original response body, retained for compatibility and diagnostics. */
  readonly responseMessage: string;
  declare readonly sandboxId?: string;
  /** Server reason, including unknown future reasons. */
  declare readonly reason?: string;
  readonly errorDetails?: unknown;

  constructor(statusCode: number, message: string) {
    let failure: Record<string, unknown> | undefined;
    try {
      const payload: unknown = JSON.parse(message);
      if (payload && typeof payload === "object" && !Array.isArray(payload)) {
        const record = payload as Record<string, unknown>;
        if (
          typeof record.sandbox_id === "string" &&
          record.sandbox_id.trim() &&
          (record.status === "failed" || record.status === "terminated")
        )
          failure = record;
      }
    } catch {
      // Non-JSON errors keep their existing message.
    }
    const sandboxId = failure?.sandbox_id as string | undefined;
    const rawReason = failure?.reason ?? failure?.termination_reason;
    const reason = typeof rawReason === "string" ? rawReason : undefined;
    const errorDetails = failure?.error_details;
    let displayMessage = message;
    if (failure) {
      displayMessage = `Sandbox ${sandboxId} ${failure.status}`;
      if (reason) displayMessage += ` (${reason})`;
      const detail = formatErrorDetails(errorDetails);
      if (detail) displayMessage += `: ${detail}`;
      if (!reason && !detail) displayMessage = message;
    }
    super(`API error (status ${statusCode}): ${displayMessage}`, {
      reason,
      sandboxId,
    });
    this.errorDetails = errorDetails;
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
