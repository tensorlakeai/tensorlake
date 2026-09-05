import { createRequire } from "node:module";
import { SHARE_ENV, Worker } from "node:worker_threads";
import type { NativeEmit, NativeSandboxBinding } from "./native-sandbox.js";
import {
  cancelNativeCall,
  closeNativeHandle,
  type NativeCall,
  type NativeHandle,
  type WorkerRequest,
  type WorkerResponse,
} from "./native-worker-protocol.js";
import { resolveProxyTarget } from "./url.js";

interface PendingCall {
  resolve: (value: any) => void;
  reject: (error: Error) => void;
  emit?: NativeEmit;
  // Keep descriptors (and their parents) alive until the operation settles.
  handle?: NativeHandle;
}

/** One persistent worker per SDK installation and calling isolate. */
export class NativeWorkerClient {
  private worker?: Worker;
  private nextId = 1;
  private registered = new Set<number>();
  private pending = new Map<number, PendingCall>();
  private finalizers = new FinalizationRegistry<number>((id) =>
    this.release(id),
  );

  constructor(
    private readonly workerPath: () => string | URL = () =>
      createRequire(import.meta.url).resolve(
        "tensorlake/internal/native-worker",
      ),
  ) {}

  handle(
    kind: NativeHandle["kind"],
    args: unknown[],
    parent?: NativeHandle,
  ): object {
    const handle: NativeHandle = { id: this.nextId++, kind, args, parent };
    this.finalizers.register(handle, handle.id);
    let closed = false;
    return new Proxy(
      {},
      {
        get: (_target, method) => {
          if (method === "then") return undefined;
          if (method === closeNativeHandle)
            return () => {
              closed = true;
              this.release(handle.id);
            };
          if (method === "connectProxy")
            return (...proxyArgs: unknown[]) => {
              if (closed) throw new Error("Tensorlake native client is closed");
              return this.handle("NativeSandboxProxyClient", proxyArgs, handle);
            };
          if (method === "baseUrl")
            return () =>
              resolveProxyTarget(String(args[0]), String(args[1])).baseUrl;
          if (typeof method !== "string") return undefined;
          return (...callArgs: unknown[]) => {
            if (closed)
              return Promise.reject(
                new Error("Tensorlake native client is closed"),
              );
            const emit =
              typeof callArgs.at(-1) === "function"
                ? (callArgs.pop() as NativeEmit)
                : undefined;
            return this.call(handle, method, callArgs, emit);
          };
        },
      },
    );
  }

  call(
    handle: NativeHandle | undefined,
    method: string,
    args: unknown[],
    emit?: NativeEmit,
  ): NativeCall<any> {
    const id = this.nextId++;
    let worker: Worker | undefined;
    const promise: NativeCall<any> = new Promise((resolve, reject) => {
      try {
        worker = this.getWorker();
        this.pending.set(id, { resolve, reject, emit, handle });
        worker.ref();
        if (handle) this.register(handle, worker);
        this.send(worker, {
          type: "call",
          id,
          handleId: handle?.id,
          method,
          args,
          stream: !!emit,
        });
      } catch (error) {
        this.pending.delete(id);
        this.unrefIfIdle();
        reject(error);
      }
    });
    promise[cancelNativeCall] = () => {
      const pending = this.pending.get(id);
      if (pending && worker) {
        pending.emit = undefined;
        this.send(worker, { type: "cancel", id });
      }
    };
    return promise;
  }

  private getWorker(): Worker {
    if (this.worker) return this.worker;
    const worker = new Worker(this.workerPath(), {
      env: SHARE_ENV,
      name: "tensorlake-native",
    });
    this.worker = worker;
    worker.on("message", (message: WorkerResponse) => {
      const pending = this.pending.get(message.id);
      if (!pending || this.worker !== worker) return;
      if (message.type === "event") {
        // Acknowledge only when the consumer asks for the next event. At most
        // one event per stream is outstanding across Rust, worker and caller.
        Promise.resolve()
          .then(() => pending.emit?.(message.value))
          .then(
            () => {
              if (this.worker === worker)
                this.send(worker, { type: "ack", id: message.id });
            },
            (error: Error) => {
              this.send(worker, { type: "cancel", id: message.id });
              pending.reject(error);
            },
          );
        return;
      }
      this.pending.delete(message.id);
      if (message.type === "error") {
        const error = new Error(message.error.message);
        error.name = message.error.name;
        error.stack = message.error.stack ?? error.stack;
        pending.reject(error);
      } else {
        pending.resolve(message.value);
      }
      this.unrefIfIdle();
    });
    const fail = (error: Error) => {
      if (this.worker !== worker) return;
      this.worker = undefined;
      this.registered.clear();
      for (const pending of this.pending.values()) pending.reject(error);
      this.pending.clear();
      // Never replay an in-flight request: it may already have taken effect.
      // A subsequent explicit call starts a fresh worker from descriptors.
      void worker.terminate();
    };
    worker.on("error", fail);
    worker.on("messageerror", fail);
    worker.on("exit", (code) =>
      fail(
        new Error(
          `Tensorlake native worker exited (${code}); in-flight operations were not retried`,
        ),
      ),
    );
    worker.unref();
    return worker;
  }

  private register(handle: NativeHandle, worker: Worker): void {
    if (this.registered.has(handle.id)) return;
    if (handle.parent) this.register(handle.parent, worker);
    this.send(worker, {
      type: "create",
      handle: { id: handle.id, kind: handle.kind, args: handle.args },
      parentId: handle.parent?.id,
    });
    this.registered.add(handle.id);
  }

  private release(handleId: number): void {
    if (this.worker && this.registered.delete(handleId))
      this.send(this.worker, { type: "release", handleId });
  }

  private send(worker: Worker, message: WorkerRequest): void {
    worker.postMessage(message);
  }
  private unrefIfIdle(): void {
    if (this.pending.size === 0) this.worker?.unref();
  }
}

export function createWorkerBinding(
  runtime: NativeWorkerClient,
): NativeSandboxBinding {
  return {
    NativeSandboxClient: class {
      constructor(...args: unknown[]) {
        return runtime.handle("NativeSandboxClient", args);
      }
    },
    NativeSandboxProxyClient: class {
      constructor(...args: unknown[]) {
        return runtime.handle("NativeSandboxProxyClient", args);
      }
    },
    NativeRepositoryClient: class {
      constructor(...args: unknown[]) {
        return runtime.handle("NativeRepositoryClient", args);
      }
    },
    validateManagedName: (name: string) =>
      runtime.call(undefined, "validateManagedName", [name]),
  } as unknown as NativeSandboxBinding;
}

// ESM and CJS can coexist in one application. Share the worker across bundles
// of this SDK version, while keeping incompatible installed versions separate.
declare const __SDK_VERSION__: string;
const runtimeKey = Symbol.for(
  `tensorlake.native.worker.${typeof __SDK_VERSION__ === "undefined" ? "source" : __SDK_VERSION__}.v1`,
);
const runtimes = globalThis as typeof globalThis & {
  [runtimeKey]?: NativeWorkerClient;
};
export function workerBinding(): NativeSandboxBinding {
  return createWorkerBinding(
    (runtimes[runtimeKey] ??= new NativeWorkerClient()),
  );
}

/** Runtime capsules supply the worker alongside their bundled application SDK. */
export function configureNativeWorker(workerPath: () => string | URL): void {
  runtimes[runtimeKey] ??= new NativeWorkerClient(workerPath);
}

export function releaseNativeHandle(handle: object | null): void {
  (handle as { [closeNativeHandle]?: () => void } | null)?.[
    closeNativeHandle
  ]?.();
}
