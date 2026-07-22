import { AsyncLocalStorage } from "node:async_hooks";
import { SDKUsageError } from "./errors.js";
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

export interface RequestContextValue {
  readonly requestId: string;
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

export class MemoryRequestContext implements RequestContextValue {
  readonly signal: AbortSignal;
  private readonly values = new Map<string, Uint8Array>();
  private readonly counters = new Map<string, number>();
  private readonly timers = new Map<string, number[]>();
  private lastProgress?: { current: number; total: number; options?: ProgressOptions };

  constructor(
    readonly requestId: string,
    options: { signal?: AbortSignal } = {},
  ) {
    this.signal = options.signal ?? new AbortController().signal;
  }

  readonly state: RequestState = {
    set: async (key, value) => {
      const serialized = serializeValue(value);
      if (serialized.encoding !== "json") {
        throw new SDKUsageError("Request state values must be JSON values");
      }
      this.values.set(key, serialized.data.slice());
    },
    get: async <T>(key: string, defaultValue?: T) => {
      const value = this.values.get(key);
      return value == null ? defaultValue : deserializeJSON(value) as T;
    },
  };

  readonly metrics: RequestMetrics = {
    counter: async (name, value = 1) => {
      this.counters.set(name, (this.counters.get(name) ?? 0) + value);
    },
    timer: async (name, value) => {
      const values = this.timers.get(name) ?? [];
      values.push(value);
      this.timers.set(name, values);
    },
  };

  readonly progress: FunctionProgress = {
    update: async (current, total, options) => {
      if (!Number.isFinite(current) || !Number.isFinite(total) || total < 0 || current < 0) {
        throw new SDKUsageError("Progress current and total must be non-negative finite numbers");
      }
      if (options?.attributes != null) {
        for (const [key, value] of Object.entries(options.attributes)) {
          if (typeof key !== "string" || typeof value !== "string") {
            throw new SDKUsageError("Progress attributes must contain only string keys and values");
          }
        }
      }
      this.lastProgress = { current, total, options };
    },
  };
}
