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
  maxRetries?: number;
  retryBackoffMs?: number;
  timeoutMs?: number;
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
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.maxRetries = options.maxRetries ?? defaults.MAX_RETRIES;
    this.retryBackoffMs = options.retryBackoffMs ?? defaults.RETRY_BACKOFF_MS;
    this.timeoutMs = options.timeoutMs ?? defaults.DEFAULT_HTTP_TIMEOUT_MS;

    this.headers = {
      "Content-Type": "application/json",
    };
    if (options.apiKey) {
      this.headers["Authorization"] = `Bearer ${options.apiKey}`;
    }
    if (options.organizationId) {
      this.headers["X-Organization-ID"] = options.organizationId;
    }
    if (options.projectId) {
      this.headers["X-Project-ID"] = options.projectId;
    }
    if (options.hostHeader) {
      this.headers["Host"] = options.hostHeader;
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
      signal?: AbortSignal;
    },
  ): Promise<T> {
    const response = await this.request(method, path, options);
    const text = await response.text();
    if (!text) return undefined as T;
    return JSON.parse(text) as T;
  }

  /** Make a request returning raw bytes. */
  async requestBytes(
    method: string,
    path: string,
    options?: {
      body?: Uint8Array;
      contentType?: string;
      signal?: AbortSignal;
    },
  ): Promise<Uint8Array> {
    const headers = { ...this.headers };
    if (options?.contentType) {
      headers["Content-Type"] = options.contentType;
    }

    const response = await this.doFetch(
      method,
      path,
      options?.body,
      headers,
      options?.signal,
    );
    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  /** Make a request and return the raw Response (for SSE streaming). */
  async requestStream(
    method: string,
    path: string,
    options?: { signal?: AbortSignal },
  ): Promise<ReadableStream<Uint8Array>> {
    const headers = { ...this.headers, Accept: "text/event-stream" };
    const response = await this.doFetch(
      method,
      path,
      undefined,
      headers,
      options?.signal,
    );
    if (!response.body) {
      throw new RemoteAPIError(response.status, "No response body for SSE stream");
    }
    return response.body;
  }

  /** Low-level fetch with retry, timeout, and error mapping. */
  private async request(
    method: string,
    path: string,
    options?: {
      body?: unknown;
      signal?: AbortSignal;
    },
  ): Promise<Response> {
    const body =
      options?.body !== undefined ? JSON.stringify(options.body) : undefined;
    return this.doFetch(method, path, body, this.headers, options?.signal);
  }

  private async doFetch(
    method: string,
    path: string,
    body: string | Uint8Array | undefined,
    headers: Record<string, string>,
    signal?: AbortSignal,
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
        const response = await fetch(url, {
          method,
          headers,
          body,
          signal: combinedSignal,
        });

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

        // Non-retryable error — throw mapped error
        const errorBody = await response.text().catch(() => "");
        throwMappedError(response.status, errorBody, path);
      } catch (err) {
        clearTimeout(timeoutId);

        if (err instanceof RemoteAPIError || err instanceof SandboxNotFoundError ||
            err instanceof PoolNotFoundError || err instanceof PoolInUseError) {
          throw err;
        }

        if (signal?.aborted) {
          throw new SandboxConnectionError("Request aborted");
        }

        // Network / timeout error
        lastError =
          err instanceof Error ? err : new Error(String(err));

        if (attempt >= this.maxRetries) {
          throw new SandboxConnectionError(lastError.message);
        }
      }
    }

    throw new SandboxConnectionError(lastError?.message ?? "Request failed");
  }
}

/** Map HTTP status codes to specific error types. */
function throwMappedError(
  status: number,
  body: string,
  path: string,
): never {
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
