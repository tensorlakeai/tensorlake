import { SDKUsageError } from "./errors.js";

const FILE_BRAND = Symbol.for("tensorlake.applications.file.v1");

export class File {
  readonly [FILE_BRAND] = true;
  readonly content: Uint8Array;
  readonly contentType: string;

  static [Symbol.hasInstance](value: unknown): boolean {
    if (this !== File) {
      return Function.prototype[Symbol.hasInstance].call(this, value);
    }
    return isFile(value);
  }

  constructor(content: Uint8Array | ArrayBuffer, contentType: string) {
    if (!contentType.trim()) {
      throw new SDKUsageError("File contentType cannot be empty");
    }
    this.content = content instanceof Uint8Array ? content : new Uint8Array(content);
    this.contentType = contentType;
  }
}

/**
 * Recognizes Tensorlake Files across independently bundled SDK copies.
 *
 * Deployed application code and the function executor are separate bundles
 * loaded into one Node process, so their `File` constructors are not
 * referentially equal. The shared brand and `Symbol.hasInstance` keep
 * `instanceof File` useful across that boundary.
 */
export function isFile(value: unknown): value is File {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Record<PropertyKey, unknown>;
  return candidate[FILE_BRAND] === true
    && candidate.content instanceof Uint8Array
    && typeof candidate.contentType === "string"
    && candidate.contentType.trim().length > 0;
}
