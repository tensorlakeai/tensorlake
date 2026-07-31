import { createHash } from "node:crypto";
import { open } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { File } from "../applications/file.js";
import { deserializeJSON, serializeValue } from "../applications/serialization.js";

export interface BlobChunk {
  uri?: string;
  size?: number;
  etag?: string;
}

export interface BlobValue {
  id?: string;
  chunks?: BlobChunk[];
}

export interface SerializedObjectManifestValue {
  encoding?: string | number;
  encodingVersion?: number;
  size?: number;
  metadataSize?: number;
  sha256Hash?: string;
  contentType?: string;
  sourceFunctionCallId?: string;
}

export interface SerializedObjectInsideBlobValue {
  manifest?: SerializedObjectManifestValue;
  offset?: number;
}

export interface PreparedSerializedObject {
  object: SerializedObjectInsideBlobValue;
  bytes: Uint8Array;
}

export type BlobLog = (message: string, fields?: Record<string, unknown>) => void;

function enumIs(value: string | number | undefined, name: string, numberValue: number): boolean {
  return value === name || value === numberValue;
}

function concatenate(parts: Uint8Array[]): Uint8Array {
  const result = new Uint8Array(parts.reduce((total, part) => total + part.byteLength, 0));
  let offset = 0;
  for (const part of parts) {
    result.set(part, offset);
    offset += part.byteLength;
  }
  return result;
}

function networkURI(uri: string): string {
  if (uri.startsWith("s3://")) return `https://${uri.slice("s3://".length)}`;
  if (uri.startsWith("gs://")) return `https://${uri.slice("gs://".length)}`;
  return uri;
}

class BlobHTTPError extends Error {
  constructor(readonly status: number) {
    super(`BLOB operation failed with HTTP ${status}`);
  }
}

function uriKind(uri: string): string {
  if (uri.startsWith("file://")) return "file";
  if (uri.startsWith("s3://")) return "s3";
  if (uri.startsWith("gs://")) return "gcs";
  try {
    return new URL(uri).protocol.replace(":", "") || "unknown";
  } catch {
    return "unknown";
  }
}

function throwIfAborted(signal?: AbortSignal): void {
  signal?.throwIfAborted();
}

async function abortableDelay(delayMs: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  if (signal == null) {
    await new Promise((resolve) => setTimeout(resolve, delayMs));
    return;
  }
  await new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, delayMs);
    const onAbort = () => {
      clearTimeout(timer);
      reject(signal.reason);
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

async function fetchWithRetries(
  uri: string,
  init?: RequestInit,
  log?: BlobLog,
  fields: Record<string, unknown> = {},
  signal?: AbortSignal,
): Promise<Response> {
  let lastError: unknown;
  for (let attempt = 0; attempt < 4; attempt += 1) {
    throwIfAborted(signal);
    const startedAt = Date.now();
    log?.("HTTP attempt starting", {
      ...fields,
      attempt: attempt + 1,
      method: init?.method ?? "GET",
      uri_kind: uriKind(uri),
      has_range: new Headers(init?.headers).has("range"),
    });
    try {
      const response = await fetch(networkURI(uri), {
        ...init,
        signal,
      });
      log?.("HTTP attempt completed", {
        ...fields,
        attempt: attempt + 1,
        method: init?.method ?? "GET",
        status_code: response.status,
        duration_ms: Date.now() - startedAt,
      });
      throwIfAborted(signal);
      if (response.ok) return response;
      if ([400, 403, 404].includes(response.status) || attempt === 3) {
        throw new BlobHTTPError(response.status);
      }
      lastError = new BlobHTTPError(response.status);
    } catch (error) {
      log?.("HTTP attempt failed", {
        ...fields,
        attempt: attempt + 1,
        method: init?.method ?? "GET",
        duration_ms: Date.now() - startedAt,
        error: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
      });
      throwIfAborted(signal);
      if (error instanceof BlobHTTPError && [400, 403, 404].includes(error.status)) throw error;
      lastError = error;
      if (attempt === 3) break;
    }
    const retryDelayMs = 100 * (2 ** attempt);
    log?.("HTTP attempt scheduled for retry", {
      ...fields,
      next_attempt: attempt + 2,
      retry_delay_ms: retryDelayMs,
    });
    await abortableDelay(retryDelayMs, signal);
  }
  throw new Error("BLOB operation failed after retries", { cause: lastError });
}

async function readChunkRange(
  chunk: BlobChunk,
  index: number,
  logicalChunkOffset: number,
  offsetWithinChunk: number,
  size: number,
  sharedURI: boolean,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  throwIfAborted(signal);
  if (!chunk.uri) throw new Error("BLOB chunk has no URI");
  const physicalOffset = sharedURI ? logicalChunkOffset + offsetWithinChunk : offsetWithinChunk;
  const fields = {
    chunk_index: index,
    chunk_offset: logicalChunkOffset,
    read_offset: physicalOffset,
    read_bytes: size,
    uri_kind: uriKind(chunk.uri),
    shared_uri: sharedURI,
  };
  const startedAt = Date.now();
  log?.("download chunk starting", fields);
  if (chunk.uri.startsWith("file://")) {
    const file = await open(fileURLToPath(chunk.uri), "r");
    try {
      throwIfAborted(signal);
      const data = new Uint8Array(size);
      const { bytesRead } = await file.read(data, 0, size, physicalOffset);
      throwIfAborted(signal);
      if (bytesRead !== size) {
        throw new Error(`BLOB chunk ended after ${bytesRead} bytes; expected ${size}`);
      }
      log?.("download chunk completed", {
        ...fields,
        downloaded_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return data;
    } finally {
      await file.close();
    }
  }
  const headers = { range: `bytes=${physicalOffset}-${physicalOffset + size - 1}` };
  const response = await fetchWithRetries(chunk.uri, { headers }, log, fields, signal);
  let data = new Uint8Array(await response.arrayBuffer());
  throwIfAborted(signal);
  // A server may ignore Range and return the complete object with HTTP 200.
  if (response.status === 200 && (physicalOffset !== 0 || data.byteLength !== size)) {
    data = data.subarray(physicalOffset, physicalOffset + size);
  }
  if (data.byteLength !== size) {
    throw new Error(`BLOB chunk returned ${data.byteLength} bytes; expected ${size}`);
  }
  log?.("download chunk completed", {
    ...fields,
    status_code: response.status,
    downloaded_bytes: data.byteLength,
    duration_ms: Date.now() - startedAt,
  });
  return data;
}

export async function downloadBlobRange(
  blob: BlobValue,
  offset: number,
  size: number,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  throwIfAborted(signal);
  if (!Number.isSafeInteger(offset) || offset < 0 || !Number.isSafeInteger(size) || size < 0) {
    throw new Error("BLOB range offset and size must be non-negative safe integers");
  }
  const chunks = blob.chunks ?? [];
  const totalSize = chunks.reduce((total, chunk) => total + (chunk.size ?? 0), 0);
  if (offset + size > totalSize) {
    throw new Error(`BLOB range ${offset}..${offset + size} exceeds declared size ${totalSize}`);
  }
  if (size === 0) return new Uint8Array();
  const uriCounts = new Map<string, number>();
  for (const chunk of chunks) {
    if (chunk.uri != null) uriCounts.set(chunk.uri, (uriCounts.get(chunk.uri) ?? 0) + 1);
  }
  const rangeEnd = offset + size;
  let chunkOffset = 0;
  const reads: Array<Promise<Uint8Array>> = [];
  for (const [index, chunk] of chunks.entries()) {
    const chunkSize = chunk.size ?? 0;
    const chunkEnd = chunkOffset + chunkSize;
    const overlapStart = Math.max(offset, chunkOffset);
    const overlapEnd = Math.min(rangeEnd, chunkEnd);
    if (overlapStart < overlapEnd) {
      reads.push(readChunkRange(
        chunk,
        index,
        chunkOffset,
        overlapStart - chunkOffset,
        overlapEnd - overlapStart,
        (uriCounts.get(chunk.uri ?? "") ?? 0) > 1,
        log,
        signal,
      ));
    }
    chunkOffset = chunkEnd;
  }
  const data = concatenate(await Promise.all(reads));
  throwIfAborted(signal);
  if (data.byteLength !== size) throw new Error("BLOB does not contain the complete requested range");
  log?.("download blob completed", {
    blob_id: blob.id,
    chunk_count: chunks.length,
    downloaded_bytes: data.byteLength,
    range_offset: offset,
    range_size: size,
  });
  return data;
}

export async function downloadBlob(
  blob: BlobValue,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<Uint8Array> {
  const size = (blob.chunks ?? []).reduce((total, chunk) => total + (chunk.size ?? 0), 0);
  return downloadBlobRange(blob, 0, size, log, signal);
}

export async function downloadSerializedObject(
  object: SerializedObjectInsideBlobValue,
  blob: BlobValue,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<{ data: Uint8Array; metadata: Uint8Array; contentType?: string; encoding?: string | number }> {
  throwIfAborted(signal);
  const manifest = object.manifest;
  if (manifest?.size == null || manifest.metadataSize == null) {
    throw new Error("Serialized object manifest is missing size or metadata_size");
  }
  if (!Number.isSafeInteger(manifest.size) || manifest.size < 0) {
    throw new Error("Serialized object size must be a non-negative safe integer");
  }
  if (!Number.isSafeInteger(manifest.metadataSize) || manifest.metadataSize < 0) {
    throw new Error("Serialized object metadata_size must be a non-negative safe integer");
  }
  if (manifest.metadataSize > manifest.size) {
    throw new Error("Serialized object metadata_size cannot exceed size");
  }
  if (typeof manifest.sha256Hash !== "string" || manifest.sha256Hash.length === 0) {
    throw new Error("Serialized object manifest is missing sha256_hash");
  }
  const start = object.offset ?? 0;
  const bytes = await downloadBlobRange(blob, start, manifest.size, log, signal);
  throwIfAborted(signal);
  const hash = createHash("sha256").update(bytes).digest("hex");
  if (hash !== manifest.sha256Hash) {
    throw new Error(`Serialized object hash mismatch: expected ${manifest.sha256Hash}, got ${hash}`);
  }
  return {
    metadata: bytes.subarray(0, manifest.metadataSize),
    data: bytes.subarray(manifest.metadataSize),
    contentType: manifest.contentType,
    encoding: manifest.encoding,
  };
}

export function deserializeValueFromProtocol(
  value: { data: Uint8Array; contentType?: string; encoding?: string | number },
): unknown {
  if (enumIs(value.encoding, "SERIALIZED_OBJECT_ENCODING_RAW", 5)) {
    return new File(value.data, value.contentType ?? "application/octet-stream");
  }
  if (enumIs(value.encoding, "SERIALIZED_OBJECT_ENCODING_UTF8_TEXT", 2)) {
    return new TextDecoder("utf-8", { fatal: true }).decode(value.data);
  }
  if (
    enumIs(value.encoding, "SERIALIZED_OBJECT_ENCODING_UTF8_JSON", 1) ||
    value.contentType?.toLowerCase().includes("json")
  ) {
    return deserializeJSON(value.data);
  }
  throw new Error(`Unsupported serialized object encoding '${String(value.encoding)}'`);
}

export function prepareSerializedObject(
  value: unknown,
  offset = 0,
  sourceFunctionCallId?: string,
): PreparedSerializedObject {
  const serialized = serializeValue(value);
  const metadata = new TextEncoder().encode(JSON.stringify({
    format: "tensorlake.typescript.value.v1",
  }));
  const bytes = concatenate([metadata, serialized.data]);
  return {
    bytes,
    object: {
      offset,
      manifest: {
        encoding: serialized.encoding === "json"
          ? "SERIALIZED_OBJECT_ENCODING_UTF8_JSON"
          : "SERIALIZED_OBJECT_ENCODING_RAW",
        encodingVersion: 0,
        size: bytes.byteLength,
        metadataSize: metadata.byteLength,
        sha256Hash: createHash("sha256").update(bytes).digest("hex"),
        ...(serialized.encoding === "raw" ? { contentType: serialized.contentType } : {}),
        sourceFunctionCallId,
      },
    },
  };
}

export function prepareTextObject(value: string): PreparedSerializedObject {
  const bytes = new TextEncoder().encode(value);
  return {
    bytes,
    object: {
      offset: 0,
      manifest: {
        encoding: "SERIALIZED_OBJECT_ENCODING_UTF8_TEXT",
        encodingVersion: 0,
        size: bytes.byteLength,
        metadataSize: 0,
        sha256Hash: createHash("sha256").update(bytes).digest("hex"),
      },
    },
  };
}

async function uploadChunk(
  chunk: BlobChunk,
  data: Uint8Array,
  index: number,
  offset: number,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<BlobChunk> {
  throwIfAborted(signal);
  if (!chunk.uri) throw new Error("Writable BLOB chunk has no URI");
  const fields = {
    chunk_index: index,
    chunk_offset: offset,
    upload_bytes: data.byteLength,
    chunk_capacity: chunk.size ?? 0,
    uri_kind: uriKind(chunk.uri),
  };
  const startedAt = Date.now();
  log?.("upload chunk starting", fields);
  if (chunk.uri.startsWith("file://")) {
    const filePath = fileURLToPath(chunk.uri);
    let file;
    try {
      file = await open(filePath, "r+");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      file = await open(filePath, "w+");
    }
    try {
      throwIfAborted(signal);
      await file.write(data, 0, data.byteLength, offset);
      throwIfAborted(signal);
    } finally {
      await file.close();
    }
    log?.("upload chunk completed", { ...fields, duration_ms: Date.now() - startedAt });
    return { ...chunk, size: data.byteLength };
  }
  const response = await fetchWithRetries(chunk.uri, {
    method: "PUT",
    ...(data.byteLength === 0 ? {} : {
      body: Uint8Array.from(data).buffer,
    }),
  }, log, fields, signal);
  throwIfAborted(signal);
  const uploaded = {
    ...chunk,
    size: data.byteLength,
    etag: response.headers.get("etag") ?? undefined,
  };
  log?.("upload chunk completed", {
    ...fields,
    status_code: response.status,
    has_etag: uploaded.etag != null,
    duration_ms: Date.now() - startedAt,
  });
  return uploaded;
}

export async function uploadBlob(
  blob: BlobValue,
  data: Uint8Array,
  log?: BlobLog,
  signal?: AbortSignal,
): Promise<BlobValue> {
  throwIfAborted(signal);
  const chunks = blob.chunks ?? [];
  const capacity = chunks.reduce((total, chunk) => total + (chunk.size ?? 0), 0);
  if (data.byteLength > capacity) throw new Error(`BLOB capacity ${capacity} is smaller than ${data.byteLength}`);
  let offset = 0;
  const uploaded: BlobChunk[] = [];
  for (const [index, chunk] of chunks.entries()) {
    const size = Math.min(chunk.size ?? 0, data.byteLength - offset);
    if (size <= 0) break;
    uploaded.push(await uploadChunk(
      chunk,
      data.subarray(offset, offset + size),
      index,
      offset,
      log,
      signal,
    ));
    offset += size;
  }
  if (data.byteLength === 0 && chunks.length > 0) {
    uploaded.push(await uploadChunk(chunks[0], data, 0, 0, log, signal));
  }
  throwIfAborted(signal);
  if (offset !== data.byteLength) throw new Error(`Only uploaded ${offset} of ${data.byteLength} bytes`);
  log?.("upload blob completed", {
    blob_id: blob.id,
    chunk_count: uploaded.length,
    uploaded_bytes: data.byteLength,
  });
  return { ...blob, chunks: uploaded };
}

export function joinPrepared(values: PreparedSerializedObject[]): Uint8Array {
  return concatenate(values.map((value) => value.bytes));
}
