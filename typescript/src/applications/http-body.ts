const HTTP_BODY_BRAND = Symbol.for("tensorlake.applications.http-body.v1");

export class HttpBody {
  readonly [HTTP_BODY_BRAND] = true;
  readonly content: Uint8Array;
  readonly contentType?: string;

  static [Symbol.hasInstance](value: unknown): boolean {
    if (this !== HttpBody) {
      return Function.prototype[Symbol.hasInstance].call(this, value);
    }
    return isHttpBody(value);
  }

  constructor(
    content: Uint8Array | ArrayBuffer,
    contentType?: string,
  ) {
    this.content = content instanceof Uint8Array
      ? content
      : new Uint8Array(content);
    this.contentType = contentType;
  }

  text(encoding = "utf-8"): string {
    return new TextDecoder(encoding, { fatal: true }).decode(this.content);
  }

  json<T = unknown>(): T {
    return JSON.parse(this.text()) as T;
  }
}

/**
 * Recognizes Tensorlake HTTP bodies across independently bundled SDK copies.
 *
 * Deployed application code and the function executor are separate bundles
 * loaded into one Node process, so their `HttpBody` constructors are not
 * referentially equal.
 */
export function isHttpBody(value: unknown): value is HttpBody {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Record<PropertyKey, unknown>;
  return candidate[HTTP_BODY_BRAND] === true
    && candidate.content instanceof Uint8Array
    && (
      candidate.contentType === undefined
      || typeof candidate.contentType === "string"
    );
}
