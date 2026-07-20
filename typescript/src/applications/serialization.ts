import { createHash } from "node:crypto";
import { DeserializationError, SerializationError } from "./errors.js";
import { isFile } from "./file.js";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder("utf-8", { fatal: true });

export interface SerializedValue {
  data: Uint8Array;
  contentType: string;
  encoding: "json" | "raw";
}

function assertJSONValue(value: unknown, path: string, seen: Set<object>): void {
  if (value === null || typeof value === "string" || typeof value === "boolean") return;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new SerializationError(`${path} contains a non-finite number`);
    return;
  }
  if (typeof value !== "object") {
    throw new SerializationError(`${path} contains unsupported ${typeof value}`);
  }
  if (isFile(value)) {
    throw new SerializationError(`${path} contains a nested File; File is only supported as a direct value`);
  }
  if (seen.has(value)) throw new SerializationError(`${path} contains a cycle`);
  if (Object.getPrototypeOf(value) !== Object.prototype && !Array.isArray(value)) {
    throw new SerializationError(`${path} contains unsupported ${value.constructor?.name ?? "object"}`);
  }
  seen.add(value);
  if (Array.isArray(value)) {
    value.forEach((item, index) => assertJSONValue(item, `${path}[${index}]`, seen));
  } else {
    for (const [key, item] of Object.entries(value)) {
      assertJSONValue(item, `${path}.${key}`, seen);
    }
  }
  seen.delete(value);
}

export function serializeValue(value: unknown): SerializedValue {
  if (isFile(value)) {
    return { data: value.content, contentType: value.contentType, encoding: "raw" };
  }
  try {
    assertJSONValue(value, "value", new Set());
    return {
      data: textEncoder.encode(JSON.stringify(value)),
      contentType: "application/json; charset=UTF-8",
      encoding: "json",
    };
  } catch (error) {
    if (error instanceof SerializationError) throw error;
    throw new SerializationError("Failed to serialize JSON value", { cause: error });
  }
}

export function deserializeJSON(data: Uint8Array): unknown {
  try {
    return JSON.parse(textDecoder.decode(data));
  } catch (error) {
    throw new DeserializationError("Failed to deserialize JSON value", { cause: error });
  }
}

export function jsonRoundTrip<T>(value: T): T {
  const serialized = serializeValue(value);
  if (serialized.encoding !== "json") return value;
  return deserializeJSON(serialized.data) as T;
}

export function canonicalJSON(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJSON).join(",")}]`;
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
  return `{${entries.map(([key, item]) => `${JSON.stringify(key)}:${canonicalJSON(item)}`).join(",")}}`;
}

export function sha256(data: Uint8Array): string {
  return createHash("sha256").update(data).digest("hex");
}
