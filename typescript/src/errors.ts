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

/** Raised when the client cannot connect to the API server. */
export class SandboxConnectionError extends SandboxError {
  constructor(message: string) {
    super(`Connection error: ${message}`);
    this.name = "SandboxConnectionError";
  }
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
