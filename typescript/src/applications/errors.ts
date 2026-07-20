const REQUEST_ERROR_BRAND = Symbol.for("tensorlake.applications.request-error.v1");
const FUNCTION_ERROR_BRAND = Symbol.for("tensorlake.applications.function-error.v1");

export class TensorlakeApplicationError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "TensorlakeApplicationError";
  }
}

export class SDKUsageError extends TensorlakeApplicationError {
  constructor(message: string) {
    super(message);
    this.name = "SDKUsageError";
  }
}

export class SerializationError extends TensorlakeApplicationError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "SerializationError";
  }
}

export class DeserializationError extends TensorlakeApplicationError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "DeserializationError";
  }
}

export class FunctionError extends TensorlakeApplicationError {
  readonly [FUNCTION_ERROR_BRAND] = true;

  static [Symbol.hasInstance](value: unknown): boolean {
    return isFunctionError(value);
  }

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "FunctionError";
  }
}

/** Recognizes FunctionError instances thrown by independently bundled SDK copies. */
function isFunctionError(value: unknown): value is FunctionError {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Record<PropertyKey, unknown>;
  return candidate[FUNCTION_ERROR_BRAND] === true && typeof candidate.message === "string";
}

export class RequestError extends TensorlakeApplicationError {
  readonly [REQUEST_ERROR_BRAND] = true;

  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "RequestError";
  }
}

/** Recognizes RequestError instances thrown by independently bundled SDK copies. */
export function isRequestError(value: unknown): value is RequestError {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Record<PropertyKey, unknown>;
  return candidate[REQUEST_ERROR_BRAND] === true && typeof candidate.message === "string";
}

export class ReplayMismatchError extends TensorlakeApplicationError {
  constructor(message = "Function execution diverged from its durable event history") {
    super(message);
    this.name = "ReplayMismatchError";
  }
}
