import { Agent, fetch as undiciFetch, setGlobalDispatcher } from "undici";
import * as defaults from "./defaults.js";
import {
  PoolInUseError,
  PoolNotFoundError,
  RemoteAPIError,
  SandboxConnectionError,
  SandboxNotFoundError,
} from "./errors.js";

export interface HttpClientOptions {
  baseUrl: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  hostHeader?: string;
  routingHint?: string;
  maxRetries?: number;
  retryBackoffMs?: number;
  timeoutMs?: number;
}

setGlobalDispatcher(
  new Agent({
    keepAliveTimeout: 60_000,
    allowH2: true,
  }),
);

type RequestBody = BodyInit | Uint8Array | ArrayBuffer;

export interface HttpRequestOptions {
  body?: RequestBody | null;
  headers?: Record<string, string>;
  json?: unknown;
  signal?: AbortSignal;
  allowHttpErrors?: boolean;
}

/**
 * Internal HTTP client with retry logic, auth headers, and error mapping.
 *
 * Uses native `fetch`. Retries on transient status codes (429, 502, 503, 504)
 * with exponential backoff.
 */
export class HttpClient {
  private readonly baseUrl: string;
  private readonly headers: Record<string, string>;
  private readonly maxRetries: number;
  private readonly retryBackoffMs: number;
  private readonly timeoutMs: number;
  private abortController: AbortController | null = null;

  constructor(options: HttpClientOptions) {
    const url = options.baseUrl;
    this.baseUrl = url.endsWith("/") ? url.slice(0, -1) : url;
    this.maxRetries = options.maxRetries ?? defaults.MAX_RETRIES;
    this.retryBackoffMs = options.retryBackoffMs ?? defaults.RETRY_BACKOFF_MS;
    this.timeoutMs = options.timeoutMs ?? defaults.DEFAULT_HTTP_TIMEOUT_MS;

    this.headers = {};
    if (options.apiKey) {
      this.headers["Authorization"] = `Bearer ${options.apiKey}`;
    }
    if (options.organizationId) {
      this.headers["X-Forwarded-Organization-Id"] = options.organizationId;
    }
    if (options.projectId) {
      this.headers["X-Forwarded-Project-Id"] = options.projectId;
    }
    if (options.hostHeader) {
      this.headers["Host"] = options.hostHeader;
    }
    if (options.routingHint) {
      this.headers["X-Tensorlake-Route-Hint"] = options.routingHint;
    }
  }

  close(): void {
    this.abortController?.abort();
    this.abortController = null;
  }

  /** Make a JSON request, returning the parsed response body. */
  async requestJson<T>(
    method: string,
    path: string,
    options?: {
      body?: unknown;
      headers?: Record<string, string>;
      signal?: AbortSignal;
    },
  ): Promise<T> {
    const response = await this.requestResponse(method, path, {
      json: options?.body,
      headers: options?.headers,
      signal: options?.signal,
    });
    const text = await response.text();
    if (!text) return undefined as T;
    return JSON.parse(text) as T;
  }

  /** Make a request returning raw bytes. */
  async requestBytes(
    method: string,
    path: string,
    options?: {
      body?: RequestBody | null;
      contentType?: string;
      headers?: Record<string, string>;
      signal?: AbortSignal;
    },
  ): Promise<Uint8Array> {
    const headers = { ...(options?.headers ?? {}) };
    if (options?.contentType) {
      headers["Content-Type"] = options.contentType;
    }

    const response = await this.requestResponse(method, path, {
      body: options?.body,
      headers,
      signal: options?.signal,
    });
    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  /** Make a request and return the response body as an SSE stream. */
  async requestStream(
    method: string,
    path: string,
    options?: { signal?: AbortSignal; json?: unknown },
  ): Promise<ReadableStream<Uint8Array>> {
    const response = await this.requestResponse(method, path, {
      json: options?.json,
      headers: { Accept: "text/event-stream" },
      signal: options?.signal,
    });
    if (!response.body) {
      throw new RemoteAPIError(
        response.status,
        "No response body for SSE stream",
      );
    }
    return response.body;
  }

  /** Make a request and return the raw Response. */
  async requestResponse(
    method: string,
    path: string,
    options?: HttpRequestOptions,
  ): Promise<Response> {
    const headers = {
      ...this.headers,
      ...(options?.headers ?? {}),
    };
    const hasJsonBody = options?.json !== undefined;
    if (hasJsonBody && !hasHeader(headers, "Content-Type")) {
      headers["Content-Type"] = "application/json";
    }

    const body = hasJsonBody
      ? JSON.stringify(options?.json)
      : normalizeRequestBody(options?.body);

    return this.doFetch(
      method,
      path,
      body,
      headers,
      options?.signal,
      options?.allowHttpErrors ?? false,
    );
  }

  private async doFetch(
    method: string,
    path: string,
    body: BodyInit | undefined,
    headers: Record<string, string>,
    signal?: AbortSignal,
    allowHttpErrors = false,
  ): Promise<Response> {
    const url = `${this.baseUrl}${path}`;
    let lastError: Error | undefined;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      if (attempt > 0) {
        const delay = this.retryBackoffMs * Math.pow(2, attempt - 1);
        await sleep(delay);
      }

      this.abortController = new AbortController();
      const timeoutId = setTimeout(
        () => this.abortController?.abort(),
        this.timeoutMs,
      );

      // Combine external signal with internal timeout
      const combinedSignal = signal
        ? anySignal([signal, this.abortController.signal])
        : this.abortController.signal;

      try {
        const response = (await undiciFetch(url, {
          method,
          headers,
          body: body ?? null,
          signal: combinedSignal,
        })) as Response;

        clearTimeout(timeoutId);

        if (response.ok) return response;

        // Check if retryable
        if (
          defaults.RETRYABLE_STATUS_CODES.has(response.status) &&
          attempt < this.maxRetries
        ) {
          lastError = new RemoteAPIError(
            response.status,
            await response.text().catch(() => ""),
          );
          continue;
        }

        if (allowHttpErrors) {
          return response;
        }

        // Non-retryable error — throw mapped error
        const errorBody = await response.text().catch(() => "");
        throwMappedError(response.status, errorBody, path);
      } catch (err) {
        clearTimeout(timeoutId);

        if (
          err instanceof RemoteAPIError ||
          err instanceof SandboxNotFoundError ||
          err instanceof PoolNotFoundError ||
          err instanceof PoolInUseError
        ) {
          throw err;
        }

        if (signal?.aborted) {
          throw new SandboxConnectionError("Request aborted");
        }

        // Network / timeout error
        lastError = err instanceof Error ? err : new Error(String(err));

        if (attempt >= this.maxRetries) {
          throw new SandboxConnectionError(lastError.message);
        }
      }
    }

    throw new SandboxConnectionError(lastError?.message ?? "Request failed");
  }
}

function hasHeader(headers: Record<string, string>, name: string): boolean {
  const lowered = name.toLowerCase();
  return Object.keys(headers).some((key) => key.toLowerCase() === lowered);
}

function normalizeRequestBody(body?: RequestBody | null): BodyInit | undefined {
  if (body == null) {
    return undefined;
  }
  if (body instanceof Uint8Array) {
    return Uint8Array.from(body).buffer;
  }
  return body;
}

/** Map HTTP status codes to specific error types. */
function throwMappedError(status: number, body: string, path: string): never {
  let message = body;
  try {
    const parsed = JSON.parse(body);
    if (parsed.message) message = parsed.message;
    else if (parsed.error) message = parsed.error;
  } catch {
    // use raw body
  }

  if (status === 404) {
    // Determine entity type from path
    if (path.includes("sandbox-pools") || path.includes("pools")) {
      const match = path.match(/sandbox-pools\/([^/]+)/);
      if (match) throw new PoolNotFoundError(match[1]);
    }
    if (path.includes("sandboxes")) {
      const match = path.match(/sandboxes\/([^/]+)/);
      if (match) throw new SandboxNotFoundError(match[1]);
    }
    throw new RemoteAPIError(404, message);
  }

  if (status === 409) {
    if (path.includes("sandbox-pools") || path.includes("pools")) {
      const match = path.match(/sandbox-pools\/([^/]+)/);
      if (match) throw new PoolInUseError(match[1], message);
    }
  }

  throw new RemoteAPIError(status, message);
}

/** Combine multiple AbortSignals into one that aborts when any fires. */
function anySignal(signals: AbortSignal[]): AbortSignal {
  const controller = new AbortController();
  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort(signal.reason);
      return controller.signal;
    }
    signal.addEventListener("abort", () => controller.abort(signal.reason), {
      once: true,
    });
  }
  return controller.signal;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
