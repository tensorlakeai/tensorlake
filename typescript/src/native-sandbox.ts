import { createRequire } from "node:module";
import {
  PoolInUseError,
  PoolNotFoundError,
  RemoteAPIError,
  SandboxConnectionError,
  SandboxError,
  SandboxNotFoundError,
} from "./errors.js";

/**
 * Bridge to the Rust core's sandbox lifecycle + proxy clients (`napi-rs`).
 *
 * The proxy data path (process/file/stream ops) is served by the Rust core so
 * it shares `reqwest`'s connection pool — replacing the previous undici path,
 * which multiplexed every parallel request onto a single, untuned HTTP/2
 * connection. The native binding is required: there is no JS fallback.
 *
 * Convention (mirrors the Python binding): structured payloads cross as JSON
 * strings, bytes as `Buffer`, and every call surfaces the W3C `traceId`.
 * Errors arrive as a JSON `{category, status, message}` string in the native
 * error message; {@link translateNativeError} rethrows them as the SDK's typed
 * errors.
 */

// ---- Native value shapes --------------------------------------------------

export interface TracedJson {
  traceId: string;
  json: string;
}

export interface TracedBytes {
  traceId: string;
  data: Buffer;
}

export interface TracedEvents {
  traceId: string;
  events: string[];
}

/** Per-event callback used by the streaming proxy methods. */
export type NativeEmit = (eventJson: string) => void;

export interface NativeSandboxProxyClient {
  baseUrl(): string;

  startProcess(payloadJson: string): Promise<TracedJson>;
  listProcesses(): Promise<TracedJson>;
  // `process` is the pid-or-name path segment (the TS layer stringifies the number|string arg).
  getProcess(process: string): Promise<TracedJson>;
  killProcess(process: string): Promise<string>;
  restartProcess(process: string): Promise<TracedJson>;
  sendSignal(process: string, signal: number): Promise<TracedJson>;
  writeStdin(process: string, data: Buffer): Promise<string>;
  closeStdin(process: string): Promise<string>;
  getStdout(process: string): Promise<TracedJson>;
  getStderr(process: string): Promise<TracedJson>;
  getOutput(process: string): Promise<TracedJson>;

  followStdout(process: string, emit: NativeEmit): Promise<string>;
  followStderr(process: string, emit: NativeEmit): Promise<string>;
  followOutput(process: string, emit: NativeEmit): Promise<string>;
  runProcess(payloadJson: string): Promise<TracedEvents>;
  runProcessStreaming(payloadJson: string, emit: NativeEmit): Promise<string>;

  readFile(path: string): Promise<TracedBytes>;
  writeFile(path: string, content: Buffer): Promise<string>;
  uploadFile(path: string, localPath: string): Promise<string>;
  deleteFile(path: string): Promise<string>;
  listDirectory(path: string): Promise<TracedJson>;

  createPtySession(payloadJson: string): Promise<TracedJson>;
  deletePtySession(sessionId: string): Promise<string>;
  health(): Promise<TracedJson>;
  info(): Promise<TracedJson>;
}

export interface NativeSandboxClient {
  createSandbox(requestJson: string): Promise<TracedJson>;
  claimSandbox(poolId: string): Promise<TracedJson>;
  copySandbox(sandboxId: string, times: number): Promise<TracedJson>;
  getSandbox(sandboxId: string): Promise<TracedJson>;
  listSandboxes(): Promise<TracedJson>;
  listArchivedSandboxes(
    limit?: number | null,
    cursor?: string | null,
    direction?: string | null,
  ): Promise<TracedJson>;
  getArchivedSandbox(sandboxId: string): Promise<TracedJson>;
  getSandboxLogs(requestJson: string): Promise<TracedJson>;
  listSandboxLogProcesses(sandboxId: string): Promise<TracedJson>;
  updateSandbox(sandboxId: string, requestJson: string): Promise<TracedJson>;
  deleteSandbox(sandboxId: string): Promise<string>;
  suspendSandbox(sandboxId: string): Promise<string>;
  resumeSandbox(sandboxId: string): Promise<string>;
  attachFileSystem(
    sandboxId: string,
    fileSystemId: string,
    mountPath: string,
  ): Promise<TracedJson>;
  detachFileSystem(
    sandboxId: string,
    mountPath: string,
  ): Promise<TracedJson>;
  createSnapshot(
    sandboxId: string,
    snapshotType?: string | null,
  ): Promise<TracedJson>;
  getSnapshot(snapshotId: string): Promise<TracedJson>;
  listSnapshots(): Promise<TracedJson>;
  deleteSnapshot(snapshotId: string): Promise<string>;
  createPool(requestJson: string): Promise<TracedJson>;
  getPool(poolId: string): Promise<TracedJson>;
  listPools(): Promise<TracedJson>;
  updatePool(poolId: string, requestJson: string): Promise<TracedJson>;
  deletePool(poolId: string): Promise<string>;

  connectProxy(
    proxyUrl: string,
    sandboxId: string,
    routingHint?: string | null,
    requestTimeoutSec?: number | null,
  ): NativeSandboxProxyClient;
}

interface NativeSandboxClientCtor {
  new (
    apiUrl: string,
    apiKey?: string | null,
    organizationId?: string | null,
    projectId?: string | null,
    namespace?: string | null,
    userAgent?: string | null,
    requestTimeoutSec?: number | null,
  ): NativeSandboxClient;
}

interface NativeSandboxProxyClientCtor {
  new (
    proxyUrl: string,
    sandboxId: string,
    apiKey?: string | null,
    organizationId?: string | null,
    projectId?: string | null,
    routingHint?: string | null,
    userAgent?: string | null,
    requestTimeoutSec?: number | null,
  ): NativeSandboxProxyClient;
}

export interface NativeSandboxBinding {
  NativeSandboxClient: NativeSandboxClientCtor;
  NativeSandboxProxyClient: NativeSandboxProxyClientCtor;
  /** Validate a managed-process name; throws on failure. Single source-of-truth rule in Rust. */
  validateManagedName: (name: string) => void;
}

// ---- Binding loader -------------------------------------------------------

// `require` exists in the CJS bundle but not in ESM; declared here so the
// runtime check below typechecks under "module": "esnext".
declare const require: NodeRequire | undefined;
declare const module: { exports?: unknown } | undefined;

let cachedBinding: NativeSandboxBinding | undefined;
let cachedBindingError: Error | undefined;

function resolveRequire(): NodeRequire {
  // tsup rewrites `import.meta.url` in the CJS output; prefer the injected
  // real `require` there and fall back to `createRequire` for ESM. Mirrors
  // the loader in sandbox-image.ts.
  if (
    typeof module !== "undefined" &&
    module.exports != null &&
    typeof require !== "undefined"
  ) {
    return require;
  }
  return createRequire(import.meta.url);
}

export function loadNativeSandboxBinding(): NativeSandboxBinding {
  if (cachedBinding) return cachedBinding;
  if (cachedBindingError) throw cachedBindingError;
  try {
    const { loadNative } = resolveRequire()("../lib/runtime.cjs") as {
      loadNative: () => NativeSandboxBinding;
    };
    const binding = loadNative();
    if (
      binding == null ||
      typeof binding.NativeSandboxClient !== "function" ||
      typeof binding.NativeSandboxProxyClient !== "function"
    ) {
      throw new SandboxError(
        "native binding does not export the sandbox clients; rebuild with 'npm run build:native'",
      );
    }
    cachedBinding = binding;
    return cachedBinding;
  } catch (error) {
    cachedBindingError =
      error instanceof Error ? error : new Error(String(error));
    throw cachedBindingError;
  }
}

/** Test seam: replace (or clear) the native binding. */
export function __setNativeSandboxBindingForTest(
  binding: NativeSandboxBinding | undefined,
): void {
  cachedBinding = binding;
  cachedBindingError = undefined;
}

// ---- Error translation ----------------------------------------------------

interface NativeErrorPayload {
  category: string;
  status: number | null;
  message: string;
}

/** Context used to reconstruct entity-specific typed errors from a 404/409. */
export interface NativeErrorContext {
  sandboxId?: string;
  poolId?: string;
  /**
   * When set, a 404 maps to the typed not-found error for that entity
   * (`SandboxNotFoundError` / `PoolNotFoundError`). Lifecycle ops set it;
   * proxy/data-plane ops omit it so a 404 (e.g. missing file or process)
   * stays a generic `RemoteAPIError`, matching the pre-shim behavior.
   */
  notFoundKind?: "sandbox" | "pool";
}

function parseNativeError(error: unknown): NativeErrorPayload | null {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : "";
  if (!message) return null;
  try {
    const parsed = JSON.parse(message) as Partial<NativeErrorPayload>;
    if (parsed && typeof parsed.category === "string") {
      return {
        category: parsed.category,
        status: typeof parsed.status === "number" ? parsed.status : null,
        message: typeof parsed.message === "string" ? parsed.message : message,
      };
    }
  } catch {
    // Not a structured native error — fall through.
  }
  return null;
}

/**
 * Map a native error to the SDK's typed error hierarchy, matching the status
 * codes the undici path produced (404 → SandboxNotFoundError/PoolNotFoundError,
 * 409 → PoolInUseError, connection failures → SandboxConnectionError).
 */
export function translateNativeError(
  error: unknown,
  context?: NativeErrorContext,
): Error {
  const payload = parseNativeError(error);
  if (!payload) {
    if (error instanceof Error) return error;
    return new SandboxError(String(error));
  }

  const { category, status, message } = payload;

  if (category === "connection") {
    return new SandboxConnectionError(message, { cause: error });
  }

  if (status === 404) {
    if (context?.notFoundKind === "pool" && context.poolId) {
      return new PoolNotFoundError(context.poolId);
    }
    if (context?.notFoundKind === "sandbox" && context.sandboxId) {
      return new SandboxNotFoundError(context.sandboxId);
    }
    return new RemoteAPIError(404, message);
  }

  if (status === 409 && context?.poolId) {
    return new PoolInUseError(context.poolId, message);
  }

  if (typeof status === "number") {
    return new RemoteAPIError(status, message);
  }

  return new SandboxError(message);
}

/** Run a native call, translating any error to the SDK's typed hierarchy. */
export async function callNative<T>(
  fn: () => Promise<T>,
  context?: NativeErrorContext,
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    throw translateNativeError(error, context);
  }
}

// ---- Streaming bridge -----------------------------------------------------

/**
 * Adapt the native per-event callback API into an async generator, preserving
 * the live-streaming semantics of `followStdout`/`followStderr`/`followOutput`.
 * The native `start` call resolves once the upstream stream closes; events are
 * buffered in a queue and drained in order.
 */
export async function* nativeEventStream(
  start: (emit: NativeEmit) => Promise<string>,
  context?: NativeErrorContext,
  signal?: AbortSignal,
): AsyncGenerator<Record<string, unknown>> {
  const queue: string[] = [];
  let wake: (() => void) | null = null;
  let finished = false;
  let failure: unknown = null;

  const notify = () => {
    if (wake) {
      const w = wake;
      wake = null;
      w();
    }
  };

  const emit: NativeEmit = (eventJson) => {
    queue.push(eventJson);
    notify();
  };

  const onAbort = () => notify();
  signal?.addEventListener("abort", onAbort, { once: true });

  const startPromise = start(emit)
    .catch((error) => {
      failure = error;
    })
    .finally(() => {
      finished = true;
      notify();
    });

  try {
    while (true) {
      if (queue.length > 0) {
        yield JSON.parse(queue.shift()!) as Record<string, unknown>;
        continue;
      }
      if (signal?.aborted) break;
      if (finished) break;
      await new Promise<void>((resolve) => {
        wake = resolve;
      });
    }
    if (failure != null) {
      throw translateNativeError(failure, context);
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    // Ensure the native call settles so it never surfaces as an unhandled
    // rejection when the consumer breaks early.
    void startPromise.catch(() => {});
  }
}

/**
 * Reduce the buffered `runProcess` events into a `CommandResult`, matching the
 * SSE-parsing logic the undici `run()` path used.
 */
export function assembleCommandResult(events: string[]): {
  exitCode: number;
  stdout: string;
  stderr: string;
} {
  const stdoutLines: string[] = [];
  const stderrLines: string[] = [];
  let exitCode = -1;

  for (const eventJson of events) {
    const raw = JSON.parse(eventJson) as Record<string, unknown>;
    if (typeof raw.line === "string") {
      if (raw.stream === "stderr") {
        stderrLines.push(raw.line);
      } else {
        stdoutLines.push(raw.line);
      }
    } else if ("exit_code" in raw || "signal" in raw) {
      if (typeof raw.exit_code === "number") {
        exitCode = raw.exit_code;
      } else if (typeof raw.signal === "number") {
        exitCode = -raw.signal;
      }
    }
  }

  return { exitCode, stdout: stdoutLines.join("\n"), stderr: stderrLines.join("\n") };
}
