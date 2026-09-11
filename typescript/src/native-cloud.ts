import { callNative, loadNativeSandboxBinding, nativeEventStream, type NativeEmit } from "./native-sandbox.js";
import { releaseNativeHandle } from "./native-worker-client.js";
import type { Traced } from "./traced.js";

export interface NativeCloudPart {
  name: string;
  data: Buffer;
  filename?: string;
  contentType?: string;
}

export interface NativeCloudRequest {
  method: string;
  path: string;
  headersJson: string;
  body?: Buffer;
  parts?: NativeCloudPart[];
  statusOnlyCodes: number[];
}

export interface NativeCloudResponse {
  status: number;
  headersJson: string;
  data: Uint8Array;
  traceId: string;
}

export interface NativeCloudClient {
  request(request: NativeCloudRequest): Promise<NativeCloudResponse>;
  stream(request: NativeCloudRequest, emit: NativeEmit): Promise<string>;
}

interface CloudTransportOptions {
  baseUrl: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  userAgent: string;
  maxRetries: number;
  retryBackoffMs: number;
  timeoutMs: number;
}

interface RequestOptions {
  body?: string | Blob | ArrayBuffer | Uint8Array | FormData;
  json?: unknown;
  headers?: Record<string, string>;
  /** Return these statuses without consuming their unused diagnostic bodies. */
  statusOnlyCodes?: Set<number>;
}

/** Only marshals values across the worker boundary; Rust owns all HTTP I/O. */
export class NativeCloudTransport {
  private native?: NativeCloudClient;
  private closed = false;

  constructor(private readonly options: CloudTransportOptions) {
    if (!Number.isSafeInteger(options.maxRetries) || options.maxRetries < 0 || options.maxRetries > 0xffff_ffff) {
      throw new RangeError("maxRetries must be a non-negative 32-bit integer");
    }
    if (!Number.isFinite(options.retryBackoffMs) || options.retryBackoffMs < 0) {
      throw new RangeError("retryBackoffMs must be a non-negative finite number");
    }
  }

  private client(): NativeCloudClient {
    if (this.closed) throw new Error("Tensorlake native client is closed");
    if (this.native) return this.native;
    const ctor = loadNativeSandboxBinding().NativeCloudClient;
    if (!ctor) throw new Error("Native binding does not export NativeCloudClient; rebuild with 'npm run build:native'");
    return this.native = new ctor(JSON.stringify(this.options));
  }

  close(): void {
    this.closed = true;
    releaseNativeHandle(this.native ?? null);
    this.native = undefined;
  }

  async requestResponse(method: string, path: string, options?: RequestOptions): Promise<Traced<Response>> {
    const request = await serializeRequest(method, path, options);
    const raw = await callNative(() => this.client().request(request));
    const response = new Response(
      [204, 205, 304].includes(raw.status) ? null : Uint8Array.from(raw.data).buffer,
      { status: raw.status, headers: JSON.parse(raw.headersJson) as Record<string, string> },
    );
    return Object.assign(response, { traceId: raw.traceId });
  }

  async requestJson<T>(method: string, path: string): Promise<Traced<T>> {
    const response = await this.requestResponse(method, path);
    const text = await response.text();
    const value = text ? JSON.parse(text) : null;
    return Object.assign(value ?? {}, { traceId: response.traceId }) as Traced<T>;
  }

  async *stream(method: string, path: string, signal?: AbortSignal): AsyncGenerator<Record<string, unknown>> {
    if (signal?.aborted) return;
    const request = await serializeRequest(method, path, { headers: { Accept: "text/event-stream" } });
    yield* nativeEventStream((emit) => this.client().stream(request, emit), undefined, signal);
  }
}

async function serializeRequest(method: string, path: string, options?: RequestOptions): Promise<NativeCloudRequest> {
  const headers = { ...options?.headers };
  let body = options?.body;
  if (options?.json !== undefined) {
    body = JSON.stringify(options.json);
    if (!Object.keys(headers).some((key) => key.toLowerCase() === "content-type")) {
      headers["Content-Type"] = "application/json";
    }
  }
  const request: NativeCloudRequest = {
    method,
    path,
    headersJson: JSON.stringify(headers),
    statusOnlyCodes: [...(options?.statusOnlyCodes ?? [])],
  };
  if (body instanceof FormData) {
    request.parts = [];
    for (const [name, value] of body.entries()) {
      request.parts.push(typeof value === "string"
        ? { name, data: Buffer.from(value) }
        : { name, data: Buffer.from(await value.arrayBuffer()), filename: value.name, contentType: value.type || "application/octet-stream" });
    }
  } else if (body !== undefined) {
    if (body instanceof Blob) request.body = Buffer.from(await body.arrayBuffer());
    else if (typeof body === "string") request.body = Buffer.from(body);
    else if (body instanceof ArrayBuffer) request.body = Buffer.from(new Uint8Array(body));
    else request.body = Buffer.from(body);
  }
  return request;
}
