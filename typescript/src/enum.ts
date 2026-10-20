/** Decode a response enum while accepting values from newer servers. */
export function enumValue<E extends Record<string, string> & { UNKNOWN: string }>(
  values: E,
  value: unknown,
): E[keyof E] {
  return typeof value === "string" && Object.values(values).includes(value)
    ? value as E[keyof E]
    : values.UNKNOWN as E[keyof E];
}

/** Diagnostic data is deliberately invisible to equality and JSON serialization. */
export function withRaw<T extends object>(value: T, raw: Record<string, unknown>): T & { readonly raw: Record<string, unknown> } {
  return Object.defineProperty(value, "raw", { value: raw, enumerable: false, configurable: true }) as T & { readonly raw: Record<string, unknown> };
}
