import { vi } from "vitest";
import {
  __setNativeSandboxBindingForTest,
  type NativeSandboxBinding,
  type NativeSandboxClient,
  type NativeSandboxProxyClient,
} from "../src/native-sandbox.js";

/**
 * Shared test harness for the Rust-backed sandbox SDK.
 *
 * The TS SDK is a shim over the native binding: `SandboxClient` calls
 * `NativeSandboxClient` (management) and `Sandbox` calls a
 * `NativeSandboxProxyClient` (process/file/stream ops). This installs a fake
 * binding so tests can drive both without a real Rust client or network.
 *
 * Conventions (matching the real binding):
 * - JSON methods resolve `{ traceId, json }`; `json` is a JSON string the SDK
 *   parses and runs through `fromSnakeKeys`. Default: `{ traceId: "t", json: "{}" }`.
 * - "void" methods (delete/suspend/resume/stdin/...) resolve a trace-id string.
 * - bytes: `readFile` resolves `{ traceId, data: Buffer }`.
 * - `runProcess` resolves `{ traceId, events: string[] }` (each a JSON string).
 * - streaming `follow*`/`runProcessStreaming` take `(…, emit)` and resolve a
 *   trace-id string after invoking `emit(jsonString)` per event.
 * - errors: throw `new Error(JSON.stringify({ category, status, message }))` to
 *   exercise typed-error translation (e.g. status 404 -> SandboxNotFoundError).
 */

export type FakeFns = Record<string, ReturnType<typeof vi.fn>>;

export interface NativeStub {
  /** Fake `NativeSandboxClient` (management). */
  client: FakeFns;
  /** Fake `NativeSandboxProxyClient` (process/file/stream ops). */
  proxy: FakeFns;
  /** Args the `NativeSandboxClient` constructor was last called with. */
  clientCtorArgs: unknown[];
  /** Args the `NativeSandboxProxyClient` constructor was last called with. */
  proxyCtorArgs: unknown[];
}

const tracedJson = (json = "{}") => async () => ({ traceId: "t", json });
const tracedId = () => async () => "t";

function makeProxy(): FakeFns {
  return {
    baseUrl: vi.fn(() => "http://localhost:9443"),
    startProcess: vi.fn(tracedJson()),
    listProcesses: vi.fn(tracedJson('{"processes":[]}')),
    getProcess: vi.fn(tracedJson()),
    killProcess: vi.fn(tracedId()),
    restartProcess: vi.fn(tracedJson()),
    sendSignal: vi.fn(tracedJson()),
    writeStdin: vi.fn(tracedId()),
    closeStdin: vi.fn(tracedId()),
    getStdout: vi.fn(tracedJson()),
    getStderr: vi.fn(tracedJson()),
    getOutput: vi.fn(tracedJson()),
    followStdout: vi.fn(tracedId()),
    followStderr: vi.fn(tracedId()),
    followOutput: vi.fn(tracedId()),
    runProcess: vi.fn(async () => ({ traceId: "t", events: [] })),
    runProcessStreaming: vi.fn(tracedId()),
    readFile: vi.fn(async () => ({ traceId: "t", data: Buffer.alloc(0) })),
    writeFile: vi.fn(tracedId()),
    uploadFile: vi.fn(tracedId()),
    deleteFile: vi.fn(tracedId()),
    listDirectory: vi.fn(tracedJson()),
    createPtySession: vi.fn(tracedJson()),
    deletePtySession: vi.fn(tracedId()),
    health: vi.fn(tracedJson('{"healthy":true}')),
    info: vi.fn(tracedJson()),
  };
}

function makeClient(proxy: FakeFns): FakeFns {
  return {
    createSandbox: vi.fn(tracedJson()),
    claimSandbox: vi.fn(tracedJson()),
    copySandbox: vi.fn(tracedJson()),
    getSandbox: vi.fn(tracedJson()),
    listSandboxes: vi.fn(tracedJson('{"sandboxes":[]}')),
    listArchivedSandboxes: vi.fn(tracedJson('{"sandboxes":[]}')),
    getArchivedSandbox: vi.fn(tracedJson()),
    updateSandbox: vi.fn(tracedJson()),
    deleteSandbox: vi.fn(tracedId()),
    suspendSandbox: vi.fn(tracedId()),
    resumeSandbox: vi.fn(tracedId()),
    attachSharedFileSystem: vi.fn(tracedJson()),
    detachSharedFileSystem: vi.fn(tracedJson()),
    createSnapshot: vi.fn(tracedJson()),
    getSnapshot: vi.fn(tracedJson()),
    listSnapshots: vi.fn(tracedJson('{"snapshots":[]}')),
    deleteSnapshot: vi.fn(tracedId()),
    createPool: vi.fn(tracedJson()),
    getPool: vi.fn(tracedJson()),
    listPools: vi.fn(tracedJson('{"pools":[]}')),
    updatePool: vi.fn(tracedJson()),
    deletePool: vi.fn(tracedId()),
    connectProxy: vi.fn(() => proxy as unknown as NativeSandboxProxyClient),
  };
}

/**
 * Install a fake native binding. Pass `overrides.client` / `overrides.proxy`
 * to replace specific method implementations (e.g. assert on args or return
 * canned JSON). Returns handles for assertions.
 */
export function installNativeStub(overrides?: {
  client?: Record<string, unknown>;
  proxy?: Record<string, unknown>;
}): NativeStub {
  const proxy = makeProxy();
  const client = makeClient(proxy);
  Object.assign(proxy, overrides?.proxy ?? {});
  Object.assign(client, overrides?.client ?? {});

  const stub: NativeStub = {
    client,
    proxy,
    clientCtorArgs: [],
    proxyCtorArgs: [],
  };

  const binding: NativeSandboxBinding = {
    NativeSandboxClient: class {
      constructor(...args: unknown[]) {
        stub.clientCtorArgs = args;
        return client as unknown as NativeSandboxClient;
      }
    } as unknown as NativeSandboxBinding["NativeSandboxClient"],
    NativeSandboxProxyClient: class {
      constructor(...args: unknown[]) {
        stub.proxyCtorArgs = args;
        return proxy as unknown as NativeSandboxProxyClient;
      }
    } as unknown as NativeSandboxBinding["NativeSandboxProxyClient"],
  };
  __setNativeSandboxBindingForTest(binding);
  return stub;
}

/** Clear the installed binding. Call in `afterEach`. */
export function clearNativeStub(): void {
  __setNativeSandboxBindingForTest(undefined);
}
