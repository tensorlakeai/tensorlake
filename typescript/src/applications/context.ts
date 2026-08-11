import { AsyncLocalStorage } from "node:async_hooks";
import { SDKUsageError } from "./errors.js";
import { Headers, type HeadersInit } from "./headers.js";
import { deserializeJSON, serializeValue } from "./serialization.js";

export interface ProgressOptions {
  message?: string;
  attributes?: Record<string, string>;
}

export interface RequestState {
  set(key: string, value: unknown): Promise<void>;
  get<T>(key: string, defaultValue?: T): Promise<T | undefined>;
}

export interface RequestMetrics {
  timer(name: string, value: number): Promise<void>;
  counter(name: string, value?: number): Promise<void>;
}

export interface FunctionProgress {
  update(current: number, total: number, options?: ProgressOptions): Promise<void>;
}

export function validateRequestStateKey(key: unknown): asserts key is string {
  if (typeof key !== "string") {
    throw new SDKUsageError(`State key must be a string, got: ${String(key)}`);
  }
}

export function serializeRequestStateValue(value: unknown): Uint8Array {
  const serialized = serializeValue(value);
  if (serialized.encoding !== "json") {
    throw new SDKUsageError("Request state values must be JSON values");
  }
  return serialized.data;
}

export function validateCounterMetric(name: unknown, value: unknown): void {
  if (typeof name !== "string") {
    throw new SDKUsageError(`Counter name must be a string, got: ${String(name)}`);
  }
  if (typeof value !== "number" || !Number.isInteger(value)) {
    throw new SDKUsageError(`Counter value must be an int, got: ${String(value)}`);
  }
  if (!Number.isSafeInteger(value)) {
    throw new SDKUsageError(`Counter value must be a safe int, got: ${String(value)}`);
  }
}

export function validateTimerMetric(name: unknown, value: unknown): void {
  if (typeof name !== "string") {
    throw new SDKUsageError(`Timer name must be a string, got: ${String(name)}`);
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new SDKUsageError(`Timer value must be a finite number, got: ${String(value)}`);
  }
}

function isPlainRecord(value: unknown): value is Record<PropertyKey, unknown> {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return false;
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

export function validateProgressUpdate(
  current: number,
  total: number,
  options?: ProgressOptions,
): void {
  if (!Number.isFinite(current) || !Number.isFinite(total) || total < 0 || current < 0) {
    throw new SDKUsageError("Progress current and total must be non-negative finite numbers");
  }
  if (options != null && !isPlainRecord(options)) {
    throw new SDKUsageError("Progress options must be an object");
  }
  if (options?.message != null && typeof options.message !== "string") {
    throw new SDKUsageError("Progress message must be a string");
  }
  if (options?.attributes != null) {
    if (!isPlainRecord(options.attributes)) {
      throw new SDKUsageError("Progress attributes must be an object of string key/value pairs");
    }
    for (const key of Reflect.ownKeys(options.attributes)) {
      if (typeof key !== "string") {
        throw new SDKUsageError("Progress attributes must contain only string keys and values");
      }
      const value = options.attributes[key];
      if (typeof value !== "string") {
        throw new SDKUsageError("Progress attributes must contain only string keys and values");
      }
    }
  }
}

export interface RequestContextValue {
  readonly requestId: string;
  readonly headers: Headers;
  readonly signal: AbortSignal;
  readonly state: RequestState;
  readonly metrics: RequestMetrics;
  readonly progress: FunctionProgress;
}

// A deployed application bundle contains its own copy of the applications SDK,
// while the function executor has another copy in its runtime capsule. Both
// copies execute in the same Node process and must observe the same context.
// Symbol.for keeps the AsyncLocalStorage process-global without exposing it as
// public API or coupling the application bundle to the executor package.
const REQUEST_CONTEXT_STORAGE_KEY = Symbol.for(
  "tensorlake.applications.request-context-storage.v1",
);

function requestContextStorage(): AsyncLocalStorage<RequestContextValue> {
  const target = globalThis as typeof globalThis & {
    [REQUEST_CONTEXT_STORAGE_KEY]?: AsyncLocalStorage<RequestContextValue>;
  };
  target[REQUEST_CONTEXT_STORAGE_KEY] ??= new AsyncLocalStorage<RequestContextValue>();
  return target[REQUEST_CONTEXT_STORAGE_KEY];
}

export class RequestContext {
  static get(): RequestContextValue {
    const context = requestContextStorage().getStore();
    if (context == null) {
      throw new SDKUsageError("RequestContext.get() can only be called from a running Tensorlake function");
    }
    return context;
  }
}

export function runWithRequestContext<T>(
  context: RequestContextValue,
  callback: () => Promise<T>,
): Promise<T> {
  return requestContextStorage().run(context, callback);
}

export async function waitWithAbortSignal<T>(
  value: Promise<T>,
  signal: AbortSignal,
): Promise<T> {
  if (signal.aborted) throw signal.reason;
  let onAbort: (() => void) | undefined;
  try {
    return await Promise.race([
      value,
      new Promise<never>((_resolve, reject) => {
        onAbort = () => reject(signal.reason);
        signal.addEventListener("abort", onAbort, { once: true });
      }),
    ]);
  } finally {
    if (onAbort != null) signal.removeEventListener("abort", onAbort);
  }
}

export class MemoryRequestContext implements RequestContextValue {
  readonly headers: Headers;
  readonly signal: AbortSignal;
  private readonly values = new Map<string, Uint8Array>();
  private readonly counters = new Map<string, number>();
  private readonly timers = new Map<string, number[]>();
  private lastProgress?: { current: number; total: number; options?: ProgressOptions };

  constructor(
    readonly requestId: string,
    options: { signal?: AbortSignal; headers?: HeadersInit } = {},
  ) {
    this.headers = new Headers(options.headers);
    this.signal = options.signal ?? new AbortController().signal;
  }

  readonly state: RequestState = {
    set: async (key, value) => {
      validateRequestStateKey(key);
      this.values.set(key, serializeRequestStateValue(value).slice());
    },
    get: async <T>(key: string, defaultValue?: T) => {
      validateRequestStateKey(key);
      const value = this.values.get(key);
      return value == null ? defaultValue : deserializeJSON(value) as T;
    },
  };

  readonly metrics: RequestMetrics = {
    counter: async (name, value = 1) => {
      validateCounterMetric(name, value);
      this.counters.set(name, (this.counters.get(name) ?? 0) + value);
    },
    timer: async (name, value) => {
      validateTimerMetric(name, value);
      const values = this.timers.get(name) ?? [];
      values.push(value);
      this.timers.set(name, values);
    },
  };

  readonly progress: FunctionProgress = {
    update: async (current, total, options) => {
      validateProgressUpdate(current, total, options);
      this.lastProgress = { current, total, options };
    },
  };
}
