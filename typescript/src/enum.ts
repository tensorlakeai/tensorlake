/** Decode a response enum while accepting values from newer servers. */
export function enumValue<E extends Record<string, string> & { UNKNOWN: string }>(
  values: E,
  value: unknown,
): E[keyof E] {
  return typeof value === "string" && Object.values(values).includes(value)
    ? value as E[keyof E]
    : values.UNKNOWN as E[keyof E];
}

/** Validate values supplied by SDK callers without accepting typos. */
export function requireEnumValue<E extends Record<string, string>>(
  values: E,
  value: unknown,
): E[keyof E] {
  if (typeof value === "string" && Object.values(values).includes(value)) {
    return value as E[keyof E];
  }
  throw new TypeError(`Invalid enum value: ${String(value)}`);
}
