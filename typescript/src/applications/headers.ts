export type HeadersInit =
  | Headers
  | Record<string, string>
  | Iterable<readonly [string, string]>;

const HEADERS_BRAND = Symbol.for("tensorlake.applications.headers.v1");

/** Immutable, case-insensitive application request headers. */
export class Headers implements Iterable<readonly [string, string]> {
  readonly [HEADERS_BRAND] = true;
  readonly #items: readonly (readonly [string, string])[];
  readonly #lookup: ReadonlyMap<string, readonly string[]>;

  static [Symbol.hasInstance](value: unknown): boolean {
    if (this !== Headers) {
      return Function.prototype[Symbol.hasInstance].call(this, value);
    }
    return isHeaders(value);
  }

  constructor(headers: HeadersInit = []) {
    const entries = headers instanceof Headers
      ? [...headers]
      : Symbol.iterator in Object(headers)
        ? [...headers as Iterable<readonly [string, string]>]
        : Object.entries(headers);
    const lookup = new Map<string, string[]>();
    this.#items = Object.freeze(entries.map(([name, value]) => {
      if (typeof name !== "string" || typeof value !== "string") {
        throw new TypeError("Headers require string names and values");
      }
      const item = Object.freeze([name, value] as const);
      const key = name.toLowerCase();
      lookup.set(key, [...(lookup.get(key) ?? []), value]);
      return item;
    }));
    this.#lookup = lookup;
    Object.freeze(this);
  }

  get(name: string): string | undefined {
    return this.#lookup.get(name.toLowerCase())?.at(-1);
  }

  getAll(name: string): readonly string[] {
    return Object.freeze([...(this.#lookup.get(name.toLowerCase()) ?? [])]);
  }

  has(name: string): boolean {
    return this.#lookup.has(name.toLowerCase());
  }

  entries(): IterableIterator<readonly [string, string]> {
    return this.#items[Symbol.iterator]();
  }

  [Symbol.iterator](): IterableIterator<readonly [string, string]> {
    return this.entries();
  }
}

/** Recognizes immutable request headers across independently bundled SDK copies. */
export function isHeaders(value: unknown): value is Headers {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Record<PropertyKey, unknown>;
  return candidate[HEADERS_BRAND] === true
    && typeof candidate.get === "function"
    && typeof candidate.getAll === "function"
    && typeof candidate.has === "function"
    && typeof candidate[Symbol.iterator] === "function";
}
