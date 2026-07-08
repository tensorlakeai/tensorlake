import { vi } from "vitest";
import {
  __setNativeSandboxBindingForTest,
  type NativeRepositoryClient,
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
  /** Fake `NativeRepositoryClient`. */
  repository: FakeFns;
  /** Args the `NativeSandboxClient` constructor was last called with. */
  clientCtorArgs: unknown[];
  /** Args the `NativeSandboxProxyClient` constructor was last called with. */
  proxyCtorArgs: unknown[];
  /** Args the `NativeRepositoryClient` constructor was last called with. */
  repositoryCtorArgs: unknown[];
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
    attachFileSystem: vi.fn(tracedJson()),
    detachFileSystem: vi.fn(tracedJson()),
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
    selectSandboxProxyUrl: vi.fn(
      (
        sandboxId: string,
        sandboxUrl?: string | null,
        ingressEndpoint?: string | null,
        explicitProxyUrl?: string | null,
      ) =>
        sandboxUrl ??
        explicitProxyUrl ??
        (() => {
          throw new Error(
            "server response did not include sandbox_url; refusing to derive a proxy URL",
          );
        })(),
    ),
  };
}

function makeRepository(): FakeFns {
  return {
    gitRepoUrl: vi.fn((repo: string) => `https://git.tensorlake.ai/project_1/${repo}`),
    createRepo: vi.fn(tracedJson()),
    listRepos: vi.fn(tracedJson('{"project":"project_1","repos":[]}')),
    deleteRepo: vi.fn(tracedId()),
    forkRepo: vi.fn(tracedJson()),
    archiveRepo: vi.fn(tracedId()),
    restoreRepo: vi.fn(tracedId()),
    repoInfo: vi.fn(tracedJson()),
    listBranches: vi.fn(tracedJson('{"repo":"repo","branches":[]}')),
    listRefs: vi.fn(tracedJson('{"repo":"repo","refs":[]}')),
    deleteBranch: vi.fn(tracedId()),
    listOperations: vi.fn(tracedJson('{"repo":"repo","operations":[]}')),
    gitCredential: vi.fn(async () => JSON.stringify({
      token: "tok",
      tokenType: "bearer",
      expiresAt: "",
      gitUsername: "t",
      repoPattern: "*",
      scopes: [],
    })),
    commitStatus: vi.fn(tracedJson()),
    pushWorktree: vi.fn(tracedJson()),
    mergeRepo: vi.fn(tracedJson()),
    commitConflicts: vi.fn(tracedJson("null")),
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
  repository?: Record<string, unknown>;
}): NativeStub {
  const proxy = makeProxy();
  const client = makeClient(proxy);
  const repository = makeRepository();
  Object.assign(proxy, overrides?.proxy ?? {});
  Object.assign(client, overrides?.client ?? {});
  Object.assign(repository, overrides?.repository ?? {});

  const stub: NativeStub = {
    client,
    proxy,
    repository,
    clientCtorArgs: [],
    proxyCtorArgs: [],
    repositoryCtorArgs: [],
  };

  const binding: NativeSandboxBinding = {
    // Mirrors the Rust `validate_managed_name` rule so tests drive accept/reject: reject
    // only empty, names containing '/', and all-digit names (reserved for PID).
    validateManagedName: (name: string) => {
      if (name === "" || name.includes("/") || /^[0-9]+$/.test(name)) {
        throw new Error(
          `managed process name must not be empty, contain '/', or be all digits: ${name}`,
        );
      }
    },
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
    NativeRepositoryClient: class {
      constructor(...args: unknown[]) {
        stub.repositoryCtorArgs = args;
        return repository as unknown as NativeRepositoryClient;
      }
    } as unknown as NonNullable<NativeSandboxBinding["NativeRepositoryClient"]>,
  };
  __setNativeSandboxBindingForTest(binding);
  return stub;
}

/** Clear the installed binding. Call in `afterEach`. */
export function clearNativeStub(): void {
  __setNativeSandboxBindingForTest(undefined);
}
