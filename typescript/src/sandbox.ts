import type { SandboxClient } from "./client.js";
import {
  type ConnectDesktopOptions,
  Desktop,
} from "./desktop.js";
import {
  RemoteAPIError,
  SandboxError,
  SandboxNotFoundError,
} from "./errors.js";
import { type Traced } from "./http.js";
import {
  assembleCommandResult,
  callNative,
  loadNativeSandboxBinding,
  nativeEventStream,
  type NativeSandboxProxyClient,
} from "./native-sandbox.js";
import {
  type CheckpointOptions,
  type CommandResult,
  type ConnectOptions,
  type CopySandboxOptions,
  type CopySandboxResponse,
  type CreateAndConnectOptions,
  type CreatePtySessionOptions,
  type DaemonInfo,
  type GetOrCreateOptions,
  type GetOrCreateOutcome,
  type GetSandboxLogsOptions,
  type HealthResponse,
  type ListDirectoryResponse,
  type OutputEvent,
  type OutputResponse,
  OutputMode,
  type ProcessInfo,
  type ProcessUser,
  type PtySessionInfo,
  type RunOptions,
  type SandboxClientOptions,
  type SandboxInfo,
  type SandboxLogsResponse,
  type SandboxOptions,
  type SandboxProcessLogFiltersResponse,
  type SendSignalResponse,
  type FileSystemMount,
  type AttachFileSystemOptions,
  type SnapshotInfo,
  type StartProcessOptions,
  SandboxStatus,
  StdinMode,
  type SuspendResumeOptions,
  type UpdateSandboxOptions,
  fromSnakeKeys,
  toSnakeKeys,
} from "./models.js";
import {
  type CreateTunnelOptions,
  TcpTunnel,
} from "./tunnel.js";
import { nowMs, logSdkTimingEvent, sdkTimingPayloadsEnabled, logSdkTiming } from "./sdk-timings.js";
import { explicitProxyUrlOverride, resolveProxyTarget } from "./url.js";
import WebSocket, { type RawData } from "ws";

class SandboxProxyConnection {
  baseUrl = "";
  wsHeaders: Record<string, string> = {};

  private nativeProxy: NativeSandboxProxyClient | null = null;
  private resolveProxyInfo?: (
    identifier: string,
  ) => Promise<Traced<SandboxInfo>>;
  private resolvePromise: Promise<void> | null = null;
  private routingHint?: string;
  private readonly optionRoutingHint?: string;
  private readonly explicitProxyUrl?: string;
  private resolveGeneration = 0;

  constructor(
    private readonly sandbox: Sandbox,
    private readonly options: SandboxOptions,
  ) {
    this.routingHint = options.routingHint;
    this.optionRoutingHint = options.routingHint;
    this.explicitProxyUrl = options.explicitProxyUrl ?? explicitProxyUrlOverride() ?? undefined;
    this.resolveProxyInfo = options.resolveProxyInfo;
    if (options.proxyUrl != null) {
      this.nativeProxy = this.configureProxy(
        options.proxyUrl,
        options.sandboxId,
        options.routingHint,
      );
    } else if (this.resolveProxyInfo == null) {
      throw new SandboxError(
        "proxyUrl is required for direct Sandbox construction; use Sandbox.connect(...) or SandboxClient.connect(...) to use the server-returned sandbox_url.",
      );
    }
  }

  async ensureResolved(): Promise<void> {
    if (this.nativeProxy != null) {
      return;
    }
    if (this.resolveProxyInfo == null) {
      return;
    }
    if (this.resolvePromise != null) {
      return this.resolvePromise;
    }

    const identifier = this.sandbox._getLifecycleIdentifier();
    const resolveStart = nowMs();
    logSdkTimingEvent("sandbox.proxy", "resolve_start", {
      sandbox_id: identifier,
    });

    const generation = this.resolveGeneration;
    this.resolvePromise = this.resolveProxyInfo(identifier)
      .then((info) => {
        if (generation !== this.resolveGeneration) {
          return;
        }
        this.sandbox.traceId = info.traceId;
        this.sandbox._setLifecycleIdentifier(info.sandboxId);
        this.sandbox._setName(info.name ?? null);
        this.routingHint = info.routingHint ?? this.optionRoutingHint;
        const proxyUrl = this.options.nativeClient?.selectSandboxProxyUrl(
          info.sandboxId,
          info.sandboxUrl ?? null,
          info.ingressEndpoint ?? null,
          this.explicitProxyUrl ?? null,
        ) ?? info.sandboxUrl
          ?? this.explicitProxyUrl;
        if (proxyUrl == null) {
          throw new SandboxError(
            "server response did not include sandbox_url; refusing to derive a proxy URL",
          );
        }
        this.nativeProxy = this.configureProxy(
          proxyUrl,
          info.sandboxId,
          this.routingHint,
        );
        logSdkTiming("sandbox.proxy", "resolve_complete", resolveStart, {
          sandbox_id: info.sandboxId,
          server_trace_id: info.traceId,
          routing_hint: this.routingHint,
          ingress_endpoint: info.ingressEndpoint,
          sandbox_url: info.sandboxUrl,
        });
      })
      .finally(() => {
        this.resolvePromise = null;
      });

    return this.resolvePromise;
  }

  hasExplicitProxyUrl(): boolean {
    return this.explicitProxyUrl != null;
  }

  refreshFromInfo(info: Traced<SandboxInfo>): void {
    const routingHint = info.routingHint ?? this.optionRoutingHint;
    const proxyUrl = this.options.nativeClient?.selectSandboxProxyUrl(
      info.sandboxId,
      info.sandboxUrl ?? null,
      info.ingressEndpoint ?? null,
      this.explicitProxyUrl ?? null,
    ) ?? info.sandboxUrl
      ?? this.explicitProxyUrl;
    if (proxyUrl == null) {
      throw new SandboxError(
        "server response did not include sandbox_url; refusing to derive a proxy URL",
      );
    }
    const state = this.buildProxyState(
      proxyUrl,
      info.sandboxId,
      routingHint,
    );
    this.resolveGeneration += 1;
    this.resolvePromise = null;
    this.routingHint = routingHint;
    this.nativeProxy = state.nativeProxy;
    this.baseUrl = state.baseUrl;
    this.wsHeaders = state.wsHeaders;
    this.sandbox.traceId = info.traceId;
    this.sandbox._setLifecycleIdentifier(info.sandboxId);
    this.sandbox._setName(info.name ?? null);
  }

  /** Await proxy resolution and return the Rust-backed proxy client. */
  async client(): Promise<NativeSandboxProxyClient> {
    await this.ensureResolved();
    if (this.nativeProxy == null) {
      throw new SandboxError(
        "server response did not include sandbox_url; refusing to derive a proxy URL",
      );
    }
    return this.nativeProxy;
  }

  close(): void {
    // The underlying reqwest client is released when the native handle is
    // garbage-collected; there is nothing to close eagerly.
  }

  private configureProxy(
    proxyUrl: string,
    sandboxId: string,
    routingHint?: string,
  ): NativeSandboxProxyClient {
    const state = this.buildProxyState(proxyUrl, sandboxId, routingHint);
    this.baseUrl = state.baseUrl;
    this.wsHeaders = state.wsHeaders;
    return state.nativeProxy;
  }

  private buildProxyState(
    proxyUrl: string,
    sandboxId: string,
    routingHint?: string,
  ): {
    nativeProxy: NativeSandboxProxyClient;
    baseUrl: string;
    wsHeaders: Record<string, string>;
  } {
    // `baseUrl`/`wsHeaders` are still computed here for the WebSocket consumers
    // (PTY, tunnel, desktop), which do not flow through the native HTTP client.
    const { baseUrl, hostHeader, sandboxIdHeader } = resolveProxyTarget(
      proxyUrl,
      sandboxId,
    );
    const wsHeaders: Record<string, string> = {};
    if (this.options.apiKey) {
      wsHeaders.Authorization = `Bearer ${this.options.apiKey}`;
    }
    if (this.options.organizationId) {
      wsHeaders["X-Forwarded-Organization-Id"] = this.options.organizationId;
    }
    if (this.options.projectId) {
      wsHeaders["X-Forwarded-Project-Id"] = this.options.projectId;
    }
    if (hostHeader) {
      wsHeaders.Host = hostHeader;
    }
    if (sandboxIdHeader) {
      wsHeaders["X-Tensorlake-Sandbox-Id"] = sandboxIdHeader;
    }

    // Prefer minting from the shared lifecycle client so the proxy reuses its
    // connection pool; fall back to a standalone client when none was wired.
    let nativeProxy: NativeSandboxProxyClient;
    if (this.options.nativeClient) {
      nativeProxy = this.options.nativeClient.connectProxy(
        proxyUrl,
        sandboxId,
        routingHint ?? null,
        this.proxyRequestTimeoutSec(),
      );
    } else {
      const binding = loadNativeSandboxBinding();
      nativeProxy = new binding.NativeSandboxProxyClient(
        proxyUrl,
        sandboxId,
        this.options.apiKey ?? null,
        this.options.organizationId ?? null,
        this.options.projectId ?? null,
        routingHint ?? null,
        null,
        this.proxyRequestTimeoutSec(),
      );
    }
    return { nativeProxy, baseUrl, wsHeaders };
  }

  private proxyRequestTimeoutSec(): number | null {
    if (this.options.requestTimeout != null) {
      return this.options.requestTimeout;
    }
    if (this.options.timeoutMs != null) {
      return this.options.timeoutMs / 1000;
    }
    return null;
  }
}

function processUserPayload(
  user: ProcessUser | undefined,
): ProcessUser | undefined {
  // No user requested: omit the field so the sandbox resolves the image's
  // configured user (the image USER directive, falling back to root).
  if (user == null) {
    return undefined;
  }
  if (typeof user === "string" && user.trim() === "") {
    throw new SandboxError("process user must not be empty");
  }
  return user;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const PTY_OP_DATA = 0x00;
const PTY_OP_RESIZE = 0x01;
const PTY_OP_READY = 0x02;
const PTY_OP_EXIT = 0x03;

export type PtyDataHandler = (data: Uint8Array) => void;
export type PtyExitHandler = (exitCode: number) => void;

export interface PtyConnectionOptions {
  onData?: PtyDataHandler;
  onExit?: PtyExitHandler;
}

export interface CreatePtyOptions
  extends CreatePtySessionOptions,
    PtyConnectionOptions {}

export class Pty {
  readonly sessionId: string;
  readonly token: string;

  private readonly wsUrl: string;
  private readonly wsHeaders: Record<string, string>;
  private readonly killSession: () => Promise<void>;
  private socket: WebSocket | null = null;
  private connectPromise: Promise<this> | null = null;
  private intentionalDisconnect = false;
  private exitCode: number | null = null;
  private waitSettled = false;
  private readonly dataHandlers = new Set<PtyDataHandler>();
  private readonly exitHandlers = new Set<PtyExitHandler>();
  private readonly waitPromise: Promise<number>;
  private resolveWait!: (exitCode: number) => void;
  private rejectWait!: (error: unknown) => void;

  constructor(options: {
    sessionId: string;
    token: string;
    wsUrl: string;
    wsHeaders: Record<string, string>;
    killSession: () => Promise<void>;
  }) {
    this.sessionId = options.sessionId;
    this.token = options.token;
    this.wsUrl = options.wsUrl;
    this.wsHeaders = options.wsHeaders;
    this.killSession = options.killSession;
    this.waitPromise = new Promise<number>((resolve, reject) => {
      this.resolveWait = resolve;
      this.rejectWait = reject;
    });
  }

  onData(handler: PtyDataHandler): () => void {
    this.dataHandlers.add(handler);
    return () => this.dataHandlers.delete(handler);
  }

  onExit(handler: PtyExitHandler): () => void {
    this.exitHandlers.add(handler);
    if (this.exitCode != null) {
      queueMicrotask(() => handler(this.exitCode!));
    }
    return () => this.exitHandlers.delete(handler);
  }

  async connect(): Promise<this> {
    if (this.socket?.readyState === WebSocket.OPEN) {
      return this;
    }
    if (this.connectPromise) {
      return this.connectPromise;
    }

    this.intentionalDisconnect = false;

    this.connectPromise = new Promise<this>((resolve, reject) => {
      let opened = false;
      const socket = new WebSocket(this.wsUrl, {
        headers: this.wsHeaders,
      });
      this.socket = socket;

      socket.on("open", async () => {
        try {
          await sendPtyFrame(socket, Buffer.from([PTY_OP_READY]));
          opened = true;
          resolve(this);
        } catch (error) {
          reject(error);
        }
      });

      socket.on("message", (message: RawData) => {
        const bytes = normalizePtyMessage(message);
        const opcode = bytes[0];

        if (opcode === PTY_OP_DATA) {
          const payload = bytes.subarray(1);
          for (const handler of this.dataHandlers) {
            handler(payload);
          }
          return;
        }

        if (opcode === PTY_OP_EXIT && bytes.length >= 5) {
          this.finishWait(bytes.readInt32BE(1));
        }
      });

      socket.on("close", (code: number, reason: Buffer) => {
        const closeReason = Buffer.isBuffer(reason)
          ? reason.toString("utf8")
          : String(reason);

        if (this.socket === socket) {
          this.socket = null;
        }
        this.connectPromise = null;

        if (this.exitCode != null) {
          this.finishWait(this.exitCode);
          return;
        }

        if (closeReason.startsWith("exit:")) {
          const parsed = Number.parseInt(closeReason.slice(5), 10);
          this.finishWait(Number.isNaN(parsed) ? -1 : parsed);
          return;
        }

        if (this.intentionalDisconnect) {
          this.intentionalDisconnect = false;
          return;
        }

        if (!opened) {
          reject(new SandboxError(
            `PTY websocket closed before READY completed: ${code} ${closeReason || "no reason"}`,
          ));
          return;
        }

        if (closeReason === "session terminated") {
          this.failWait(new SandboxError("PTY session terminated"));
          return;
        }

        this.failWait(
          new SandboxError(
            `PTY websocket closed unexpectedly: ${code} ${closeReason || "no reason"}`,
          ),
        );
      });

      socket.on("error", (error: Error) => {
        if (!opened) {
          reject(error);
        }
      });
    });

    return this.connectPromise;
  }

  async sendInput(input: string | Uint8Array): Promise<void> {
    const socket = this.requireOpenSocket();
    await sendPtyFrame(socket, encodePtyInput(input));
  }

  async resize(cols: number, rows: number): Promise<void> {
    const socket = this.requireOpenSocket();
    await sendPtyFrame(socket, encodePtyResize(cols, rows));
  }

  disconnect(code = 1000, reason = "client disconnect"): void {
    if (!this.socket) return;
    this.intentionalDisconnect = true;
    this.socket.close(code, reason);
  }

  wait(): Promise<number> {
    return this.waitPromise;
  }

  async kill(): Promise<void> {
    await this.killSession();
  }

  private requireOpenSocket(): WebSocket {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      throw new SandboxError("PTY is not connected");
    }
    return this.socket;
  }

  private finishWait(exitCode: number): void {
    if (this.waitSettled) return;
    this.waitSettled = true;
    this.exitCode = exitCode;
    for (const handler of this.exitHandlers) {
      handler(exitCode);
    }
    this.resolveWait(exitCode);
  }

  private failWait(error: unknown): void {
    if (this.waitSettled) return;
    this.waitSettled = true;
    this.rejectWait(error);
  }
}

function normalizePtyMessage(message: RawData): Buffer {
  if (Buffer.isBuffer(message)) return message;
  if (Array.isArray(message)) {
    return Buffer.concat(message.map((part) => Buffer.from(part)));
  }
  return Buffer.from(message);
}

function encodePtyInput(input: string | Uint8Array): Buffer {
  const payload =
    typeof input === "string" ? Buffer.from(input, "utf8") : Buffer.from(input);
  return Buffer.concat([Buffer.from([PTY_OP_DATA]), payload]);
}

function encodePtyResize(cols: number, rows: number): Buffer {
  const frame = Buffer.alloc(5);
  frame[0] = PTY_OP_RESIZE;
  frame.writeUInt16BE(cols, 1);
  frame.writeUInt16BE(rows, 3);
  return frame;
}

function sendPtyFrame(socket: WebSocket, frame: Buffer): Promise<void> {
  return new Promise((resolve, reject) => {
    socket.send(frame, (error?: Error) => (error ? reject(error) : resolve()));
  });
}

/**
 * Client for interacting with a running sandbox.
 *
 * Provides process management, file operations, and I/O streaming
 * through the sandbox proxy.
 */
// `getOrCreate`: how many connect-or-create attempts to make when a named
// sandbox is caught mid-handoff (for example, its previous holder is still
// terminating), and the base delay between attempts.
const GET_OR_CREATE_ATTEMPTS = 4;
const GET_OR_CREATE_RETRY_DELAY_MS = 500;

// `bindOutcome` is read-only for callers; `getOrCreate` is the one writer.
function markBindOutcome(sandbox: Sandbox, outcome: GetOrCreateOutcome): Sandbox {
  (sandbox as { bindOutcome: GetOrCreateOutcome }).bindOutcome = outcome;
  return sandbox;
}

export class Sandbox {
  traceId: string | null = null;
  /**
   * What {@link Sandbox.getOrCreate} did to bind the name: `"created"`,
   * `"attached"`, or `"resumed"`. `null` for handles from `create` or
   * `connect`.
   */
  readonly bindOutcome: GetOrCreateOutcome | null = null;
  private readonly proxy: SandboxProxyConnection;
  private ownsSandbox = false;
  private lifecycleClient: SandboxClient | null = null;
  private lifecycleIdentifier: string;
  private sandboxName: string | null = null;

  constructor(options: SandboxOptions) {
    this.lifecycleIdentifier = options.sandboxId;
    this.proxy = new SandboxProxyConnection(this, options);
  }

  /**
   * The server-assigned sandbox ID.
   *
   * A handle opened by name (`connect({ sandboxId: name })`) reports the
   * name until its first server response, then the canonical ID. Handles
   * from `create` and `getOrCreate` carry the canonical ID from the start.
   */
  get sandboxId(): string {
    return this.lifecycleIdentifier;
  }

  private get baseUrl(): string {
    return this.proxy.baseUrl;
  }

  private get wsHeaders(): Record<string, string> {
    return this.proxy.wsHeaders;
  }

  get name(): string | null {
    return this.sandboxName;
  }

  /** @internal Used by client wiring to keep locally cached name in sync. */
  _setName(name: string | null): void {
    this.sandboxName = name;
  }

  /** @internal Used by lifecycle operations to pin to canonical sandbox ID. */
  _setLifecycleIdentifier(identifier: string): void {
    this.lifecycleIdentifier = identifier;
  }

  /** @internal Used by the lazy proxy resolver. */
  _getLifecycleIdentifier(): string {
    return this.lifecycleIdentifier;
  }

  /** @internal Used by SandboxClient.createAndConnect to set ownership. */
  _setOwner(client: SandboxClient): void {
    this.ownsSandbox = true;
    this.lifecycleClient = client;
  }

  // --- Static factory methods ---

  /**
   * Create a new sandbox and return a connected, running handle.
   *
   * Covers both fresh sandbox creation and restore-from-snapshot (set
   * `snapshotId`). Blocks until the sandbox is `Running`.
   *
   * When `name` is already claimed, the server rejects the create with
   * HTTP 409 (`RemoteAPIError`). To attach to the existing sandbox
   * instead, use {@link Sandbox.getOrCreate}.
   */
  static async create(
    options?: CreateAndConnectOptions & Partial<SandboxClientOptions>,
  ): Promise<Sandbox> {
    // Dynamic import to break the circular dependency (client.ts imports Sandbox).
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    return client.createAndConnect(options);
  }

  /**
   * Attach to an existing sandbox and return a connected handle.
   *
   * The handle is lazy: when `proxyUrl` is omitted, the sandbox is resolved
   * on the first request so the handle uses the correct cloud/region ingress
   * endpoint. Connecting does not verify that the sandbox exists. Does
   * **not** auto-resume a suspended sandbox — call `sandbox.resume()`
   * explicitly.
   */
  static async connect(
    options: ConnectOptions & Partial<SandboxClientOptions>,
  ): Promise<Sandbox> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    const sandbox = client.connect(
      options.sandboxId,
      options.proxyUrl,
      options.routingHint,
      options.requestTimeout,
    );
    sandbox.lifecycleClient = client;
    return sandbox;
  }

  /**
   * Return the one sandbox bound to `name`. Create it on first use.
   *
   * This is the recommended call for an agent session that derives its
   * sandbox name from its session ID. It declares the intent "this name is
   * my session's sandbox": when a sandbox already holds the name, the call
   * attaches to it instead of failing with HTTP 409 the way `create` does.
   *
   * The steps, all handled internally:
   *
   * 1. `connect` by name — finds the sandbox, running or suspended.
   * 2. When nothing holds the name, `create` with the given configuration.
   * 3. When that create loses a race against another caller (HTTP 409),
   *    connect to the winner.
   * 4. When the sandbox is still starting (another caller's create is in
   *    flight), wait until it is `Running`, the same way `create` blocks
   *    for its own caller.
   * 5. When the sandbox is suspended, `resume()` it. Disable with
   *    `resume: false`.
   *
   * Concurrent callers that use the same name all converge on the same
   * sandbox. The configuration options (`image`, `cpus`, ...) apply only
   * when the call creates a new sandbox; an existing sandbox is returned
   * as-is, with whatever configuration it already has.
   *
   * The returned handle carries the canonical `sandboxId` (not the name)
   * and records what the call did in `bindOutcome`: `"created"`,
   * `"attached"`, or `"resumed"`. See {@link GetOrCreateOutcome}.
   *
   * For about a second after `terminate()`, the name still resolves and
   * the sandbox still reports `running`. A `getOrCreate` in that window
   * attaches to the dying sandbox. When you recreate right after a
   * terminate, check `status()` after the call, or wait for the old
   * sandbox to report `terminated` first.
   *
   * `poolId` is not accepted: a pool claim cannot carry a name, so a
   * claimed sandbox could never be found again by a later call. To use a
   * warm pool, `create` with `poolId` and name the sandbox afterwards
   * with `client.update(sandboxId, { name })`.
   *
   * @param name Sandbox name; unique within the namespace. Derive it
   *   deterministically from your session ID, for example
   *   `agent-${sessionId}`.
   * @param options Create options plus `resume` (default true: resume a
   *   suspended sandbox before returning). Used only when a new sandbox
   *   must be created, except connection-level options.
   * @throws {SandboxError} If the name stays claimed but its sandbox cannot
   *   be attached to after several attempts (for example, the previous
   *   holder is stuck terminating), or if a sandbox that is still starting
   *   does not reach `Running` within `requestTimeout`.
   * @throws {RemoteAPIError} If the create step fails with a non-409 error.
   */
  static async getOrCreate(
    name: string,
    options?: GetOrCreateOptions & Partial<SandboxClientOptions>,
  ): Promise<Sandbox> {
    const { resume = true, ...createOptions } = options ?? {};
    // The type omits poolId, but plain-JS callers can still pass it. Reject
    // it here: the claim request has no name field on the wire, so the
    // claimed sandbox would be unnamed and every later call would claim
    // another one instead of converging.
    if ((createOptions as { poolId?: string }).poolId != null) {
      throw new SandboxError(
        "getOrCreate does not accept poolId: a pool claim cannot name the " +
          "sandbox. Use create({ poolId }) and then " +
          "client.update(sandboxId, { name }).",
      );
    }
    let lastConflict: RemoteAPIError | undefined;
    for (let attempt = 0; attempt < GET_OR_CREATE_ATTEMPTS; attempt++) {
      if (attempt > 0) {
        // Reachable only after a lost create race whose winner is not
        // connectable yet (for example, the name's previous holder is
        // still terminating). Give the server a moment.
        await sleep(GET_OR_CREATE_RETRY_DELAY_MS * attempt);
      }
      try {
        const sandbox = await Sandbox.connect({
          ...createOptions,
          sandboxId: name,
        });
        // connect returns a lazy handle without checking that the name
        // exists; this status fetch is the existence check that routes a
        // free name to the create step below.
        const status = await sandbox.status();
        if (status === SandboxStatus.PENDING) {
          // Another caller's create is still starting this sandbox. Match
          // create(): block until Running, then pick up fresh proxy
          // routing before handing the sandbox out.
          const waitTimeout = createOptions.requestTimeout ?? 300;
          await sandbox.refreshRoutingWhenRunning(
            Date.now() + waitTimeout * 1000,
            0.5,
            waitTimeout,
          );
        } else if (
          resume &&
          (status === SandboxStatus.SUSPENDED ||
            status === SandboxStatus.SUSPENDING)
        ) {
          const waitTimeout = createOptions.requestTimeout ?? 300;
          let settled: SandboxStatus = status;
          if (settled === SandboxStatus.SUSPENDING) {
            // The server rejects resume while a suspend is still in
            // progress; wait for the suspend to settle first.
            settled = await sandbox.waitOutSuspending(waitTimeout, name);
          }
          if (settled === SandboxStatus.SUSPENDED) {
            await sandbox.resume({ timeout: waitTimeout });
          } else if (settled === SandboxStatus.TERMINATED) {
            // The sandbox died while suspending; the name may be free
            // again. Retry the loop.
            continue;
          } else {
            // Another caller resumed it first. The resumed sandbox may
            // live elsewhere, so this handle's routing from before the
            // suspend is stale. Match the PENDING branch: block until
            // Running with fresh proxy routing.
            await sandbox.refreshRoutingWhenRunning(
              Date.now() + waitTimeout * 1000,
              0.5,
              waitTimeout,
            );
          }
          return markBindOutcome(sandbox, "resumed");
        }
        return markBindOutcome(sandbox, "attached");
      } catch (err) {
        if (!(err instanceof SandboxNotFoundError)) throw err;
        try {
          const created = await Sandbox.create({ ...createOptions, name });
          return markBindOutcome(created, "created");
        } catch (createErr) {
          if (
            createErr instanceof RemoteAPIError &&
            createErr.statusCode === 409
          ) {
            // Lost the create race: another caller claimed the name first.
            // Loop back and attach to the winner.
            lastConflict = createErr;
            continue;
          }
          throw createErr;
        }
      }
    }
    const giveUp = new SandboxError(
      `getOrCreate('${name}'): the name is claimed, but its sandbox could ` +
        `not be attached to after ${GET_OR_CREATE_ATTEMPTS} attempts`,
    );
    giveUp.cause = lastConflict;
    throw giveUp;
  }


  // --- Static snapshot management ---

  /** Get information about a snapshot by ID. No sandbox handle needed. */
  static async getSnapshot(
    snapshotId: string,
    options?: Partial<SandboxClientOptions>,
  ): Promise<SnapshotInfo> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    return client.getSnapshot(snapshotId);
  }

  /** Delete a snapshot by ID. No sandbox handle needed. */
  static async deleteSnapshot(
    snapshotId: string,
    options?: Partial<SandboxClientOptions>,
  ): Promise<void> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    await client.deleteSnapshot(snapshotId);
  }

  /** List all sandboxes. No sandbox handle needed. */
  static async list(
    options?: Partial<SandboxClientOptions>,
  ): Promise<SandboxInfo[]> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    return client.list();
  }

  /** List all snapshots in the project. No sandbox handle needed. */
  static async listSnapshots(
    options?: Partial<SandboxClientOptions>,
  ): Promise<SnapshotInfo[]> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    return client.listSnapshots();
  }

  // --- Instance lifecycle methods ---

  private requireLifecycleClient(operation: string): SandboxClient {
    if (!this.lifecycleClient) {
      throw new SandboxError(
        `Cannot ${operation}: no lifecycle client available. ` +
          "Use Sandbox.create() or Sandbox.connect() to get a lifecycle-aware handle.",
      );
    }
    return this.lifecycleClient;
  }

  /** Fetch the current sandbox information from the server on demand. */
  async info(): Promise<Traced<SandboxInfo>> {
    const client = this.requireLifecycleClient("info");
    const info = await client.get(this.lifecycleIdentifier);
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return info;
  }

  /**
   * Fetch the current sandbox status from the server.
   *
   * Always hits the network — the value is not cached locally because the
   * status changes over the sandbox's lifecycle.
   */
  async status(): Promise<SandboxStatus> {
    const client = this.requireLifecycleClient("read_status");
    const info = await client.get(this.lifecycleIdentifier);
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return info.status;
  }

  /**
   * Update this sandbox's properties (name, exposed ports, proxy auth, and
   * egress network policy).
   *
   * Naming an ephemeral sandbox makes it non-ephemeral and enables
   * suspend/resume. For `network`, omit to keep the current policy, pass an
   * object to replace it, or pass `null` to clear it to unrestricted egress.
   */
  async update(options: UpdateSandboxOptions): Promise<Traced<SandboxInfo>> {
    const client = this.requireLifecycleClient("update");
    const info = await client.update(this.lifecycleIdentifier, options);
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return info;
  }

  /**
   * Suspend this sandbox.
   *
   * By default blocks until the sandbox is fully `Suspended`. Pass
   * `{ wait: false }` for fire-and-return.
   */
  async suspend(options?: SuspendResumeOptions): Promise<void> {
    const client = this.requireLifecycleClient("suspend");
    await client.suspend(this.lifecycleIdentifier, options);
  }

  /**
   * Resume this sandbox.
   *
   * By default blocks until the sandbox is `Running` and refreshes this
   * handle's cached proxy routing. Rare transient proxy errors may still occur
   * immediately after resume.
   *
   * Pass `{ wait: false }` for fire-and-return. That mode does not refresh
   * cached proxy routing on this handle; use the default wait behavior for
   * immediate follow-up operations, or wait until the sandbox is running and
   * reconnect before issuing process/file/PTY operations.
   */
  async resume(options?: SuspendResumeOptions): Promise<void> {
    const client = this.requireLifecycleClient("resume");
    const wait = options?.wait !== false;
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const deadline = Date.now() + timeout * 1000;
    await client.resume(this.lifecycleIdentifier, {
      ...options,
      timeout: Math.max(0, (deadline - Date.now()) / 1000),
    });
    if (!wait) return;
    await this.refreshRoutingWhenRunning(deadline, pollInterval, timeout);
  }

  /**
   * Poll until the sandbox leaves `Suspending`.
   *
   * The lifecycle API rejects resume while a suspend is still in progress,
   * so `getOrCreate` must wait for the suspend to settle before it can
   * resume the sandbox.
   */
  private async waitOutSuspending(
    waitTimeout: number,
    name: string,
  ): Promise<SandboxStatus> {
    const deadline = Date.now() + waitTimeout * 1000;
    while (Date.now() < deadline) {
      await sleep(500);
      const status = await this.status();
      if (status !== SandboxStatus.SUSPENDING) return status;
    }
    throw new SandboxError(
      `getOrCreate('${name}'): the sandbox holding the name did not finish ` +
        `suspending within ${waitTimeout}s`,
    );
  }

  /**
   * Poll until the sandbox is `Running` with routing info, then rebind this
   * handle's cached proxy routing. Shared by `resume()` and the
   * `getOrCreate` path that attaches to a still-starting sandbox.
   */
  private async refreshRoutingWhenRunning(
    deadline: number,
    pollInterval: number,
    timeout: number,
  ): Promise<void> {
    const client = this.requireLifecycleClient("refresh proxy routing");
    while (Date.now() < deadline) {
      const info = await client.get(this.lifecycleIdentifier);
      if (
        info.status === SandboxStatus.RUNNING &&
        (info.sandboxUrl != null || this.proxy.hasExplicitProxyUrl())
      ) {
        this.proxy.refreshFromInfo(info);
        return;
      }
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(
          `Sandbox ${this.lifecycleIdentifier} terminated while refreshing proxy routing`,
        );
      }
      await sleep(Math.min(pollInterval * 1000, Math.max(0, deadline - Date.now())));
    }
    throw new SandboxError(
      `Sandbox ${this.lifecycleIdentifier} did not provide refreshed proxy routing within ${timeout}s`,
    );
  }

  /** Live-copy this running sandbox. */
  async copy(options?: CopySandboxOptions): Promise<Traced<CopySandboxResponse>> {
    const client = this.requireLifecycleClient("copy");
    return client.copy(this.lifecycleIdentifier, options);
  }

  /**
   * Attach a registered file system to this running sandbox at `mountPath`.
   *
   * Returns the updated sandbox info; the new mount appears in
   * `fileSystems`. `options.snapshotId` pins the mount to a specific
   * filesystem snapshot and requires `options.readOnly: true`.
   */
  async attachFileSystem(
    fileSystemId: string,
    mountPath: string,
    options?: AttachFileSystemOptions,
  ): Promise<Traced<SandboxInfo>> {
    const client = this.requireLifecycleClient("attachFileSystem");
    const info = await client.attachFileSystem(
      this.lifecycleIdentifier,
      fileSystemId,
      mountPath,
      options,
    );
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return info;
  }

  /**
   * Detach the file system mounted at `mountPath` from this running
   * sandbox.
   *
   * Returns the updated sandbox info with the mount removed from
   * `fileSystems`.
   */
  async detachFileSystem(mountPath: string): Promise<Traced<SandboxInfo>> {
    const client = this.requireLifecycleClient("detachFileSystem");
    const info = await client.detachFileSystem(
      this.lifecycleIdentifier,
      mountPath,
    );
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return info;
  }

  /** List the file systems currently mounted into this sandbox. */
  async listFileSystems(): Promise<Traced<FileSystemMount[]>> {
    const client = this.requireLifecycleClient("listFileSystems");
    const info = await client.get(this.lifecycleIdentifier);
    this._setLifecycleIdentifier(info.sandboxId);
    this._setName(info.name ?? null);
    return Object.assign(info.fileSystems ?? [], { traceId: info.traceId });
  }

  /**
   * Create a checkpoint of this sandbox and wait for it to be locally ready.
   *
   * By default blocks until the checkpoint is resumable and returns
   * `SnapshotInfo`. Pass `{ wait: false }` to fire-and-return
   * (returns `undefined`).
   */
  async checkpoint(options?: CheckpointOptions): Promise<Traced<SnapshotInfo> | undefined> {
    const client = this.requireLifecycleClient("checkpoint");
    if (options?.wait === false) {
      await client.snapshot(this.lifecycleIdentifier, { snapshotType: options.checkpointType });
      return undefined;
    }
    return client.snapshotAndWait(this.lifecycleIdentifier, {
      timeout: options?.timeout,
      pollInterval: options?.pollInterval,
      snapshotType: options?.checkpointType,
      waitUntil: options?.waitUntil,
    });
  }

  /**
   * List snapshots taken from this sandbox.
   */
  async listSnapshots(): Promise<Traced<SnapshotInfo[]>> {
    const client = this.requireLifecycleClient("listSnapshots");
    const all = await client.listSnapshots();
    const filtered = all.filter((s) => s.sandboxId === this.lifecycleIdentifier);
    return Object.assign(filtered, { traceId: all.traceId });
  }

  /** Close the proxy connection. The sandbox keeps running. */
  close(): void {
    this.proxy.close();
  }

  /** Terminate the sandbox and release all resources. */
  async terminate(): Promise<void> {
    const client = this.lifecycleClient;
    this.ownsSandbox = false;
    this.lifecycleClient = null;
    this.close();
    if (client) {
      await client.delete(this.lifecycleIdentifier);
    }
  }

  // --- High-level convenience ---

  /**
   * Run a command to completion and return its output.
   *
   * Uses a single streaming `POST /api/v1/processes/run` request that starts
   * the process, streams output, and delivers the exit code over one connection.
   */
  async run(command: string, options?: RunOptions): Promise<Traced<CommandResult>> {
    const opStart = nowMs();
    const body: Record<string, unknown> = { command };
    if (options?.args) body.args = options.args;
    if (options?.env) body.env = options.env;
    if (options?.workingDir) body.working_dir = options.workingDir;
    if (options?.timeout != null) body.timeout = options.timeout;
    const user = processUserPayload(options?.user);
    if (user !== undefined) body.user = user;

    logSdkTimingEvent("sandbox.run", "start", {
      sandbox_id: this.sandboxId,
      command: sdkTimingPayloadsEnabled() ? command : undefined,
      command_length: command.length,
    });

    const proxy = await this.proxy.client();
    const { traceId, events } = await callNative(
      () => proxy.runProcess(JSON.stringify(body)),
      { sandboxId: this.sandboxId },
    );
    const { exitCode, stdout, stderr } = assembleCommandResult(events);
    logSdkTiming("sandbox.run", "complete", opStart, {
      sandbox_id: this.sandboxId,
      server_trace_id: traceId,
      command: sdkTimingPayloadsEnabled() ? command : undefined,
      command_length: command.length,
      exit_code: exitCode,
    });

    return Object.assign({ exitCode, stdout, stderr }, { traceId });
  }

  // --- Process management ---

  /**
   * Start a process in the sandbox without waiting for it to exit.
   *
   * Returns a `ProcessInfo` with the assigned `pid`. Use `getProcess()` to
   * poll status, or `followStdout()` / `followOutput()` to stream output
   * until the process exits. Use `run()` instead to block until completion
   * and get combined output in one call.
   */
  async startProcess(
    command: string,
    options?: StartProcessOptions,
  ): Promise<Traced<ProcessInfo>> {
    const payload: Record<string, unknown> = { command };
    if (options?.args != null) payload.args = options.args;
    if (options?.env != null) payload.env = options.env;
    if (options?.workingDir != null) payload.working_dir = options.workingDir;
    const user = processUserPayload(options?.user);
    if (user !== undefined) payload.user = user;
    if (options?.stdinMode != null && options.stdinMode !== StdinMode.CLOSED) {
      payload.stdin_mode = options.stdinMode;
    }
    if (options?.stdoutMode != null && options.stdoutMode !== OutputMode.CAPTURE) {
      payload.stdout_mode = options.stdoutMode;
    }
    if (options?.stderrMode != null && options.stderrMode !== OutputMode.CAPTURE) {
      payload.stderr_mode = options.stderrMode;
    }
    if (options?.name != null) {
      // Validate the managed name client-side (throws) via the single source-of-truth rule
      // in the Rust core. The daemon re-validates as the authority.
      loadNativeSandboxBinding().validateManagedName(options.name);
      payload.name = options.name;
    }
    if (options?.restart != null) payload.restart = toSnakeKeys(options.restart);
    if (options?.healthCheck != null) {
      payload.health_check = toSnakeKeys(options.healthCheck);
    }

    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.startProcess(JSON.stringify(payload)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ProcessInfo, { traceId });
  }

  /** List all processes (running and exited) tracked by the sandbox daemon. */
  async listProcesses(): Promise<Traced<ProcessInfo[]>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.listProcesses(), {
      sandboxId: this.sandboxId,
    });
    const parsed = JSON.parse(json) as { processes?: Record<string, unknown>[] };
    const processes = (parsed.processes ?? []).map(
      (p) => fromSnakeKeys(p) as ProcessInfo,
    );
    return Object.assign(processes, { traceId });
  }

  /** Read persisted logs for this sandbox from the log service. */
  async getLogs(
    options?: GetSandboxLogsOptions,
  ): Promise<Traced<SandboxLogsResponse>> {
    const client = this.requireLifecycleClient("get_logs");
    const info = await this.info();
    return client.getLogs(info.sandboxId, options);
  }

  /** List processes available as persisted-log filters for this sandbox. */
  async listLogProcesses(): Promise<Traced<SandboxProcessLogFiltersResponse>> {
    const client = this.requireLifecycleClient("list_log_processes");
    const info = await this.info();
    return client.listLogProcesses(info.sandboxId);
  }

  /** Get current status and metadata for a process. `process` is a PID or process name given on creation. */
  async getProcess(process: number | string): Promise<Traced<ProcessInfo>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.getProcess(String(process)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ProcessInfo, { traceId });
  }

  /** Send SIGKILL to a process (or stop a managed process). `process` is a PID or process name given on creation. */
  async killProcess(process: number | string): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.killProcess(String(process)), {
      sandboxId: this.sandboxId,
    });
  }

  /** Restart a managed process. `process` is a PID or process name given on creation. */
  async restartProcess(process: number | string): Promise<Traced<ProcessInfo>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.restartProcess(String(process)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ProcessInfo, { traceId });
  }

  /** Send an arbitrary signal (e.g. `15` for SIGTERM, `9` for SIGKILL). `process` is a PID or process name given on creation. */
  async sendSignal(
    process: number | string,
    signal: number,
  ): Promise<Traced<SendSignalResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.sendSignal(String(process), signal),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as SendSignalResponse, {
      traceId,
    });
  }

  // --- Process I/O ---

  /** Write bytes to a process's stdin (must be started with `stdinMode: StdinMode.PIPE`). `process` is a PID or process name given on creation. */
  async writeStdin(process: number | string, data: Uint8Array): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.writeStdin(String(process), Buffer.from(data)), {
      sandboxId: this.sandboxId,
    });
  }

  /** Close a process's stdin pipe, signalling EOF. `process` is a PID or process name given on creation. */
  async closeStdin(process: number | string): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.closeStdin(String(process)), {
      sandboxId: this.sandboxId,
    });
  }

  /** Return all captured stdout lines produced so far. `process` is a PID or process name given on creation. */
  async getStdout(process: number | string): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.getStdout(String(process)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  /** Return all captured stderr lines produced so far. `process` is a PID or process name given on creation. */
  async getStderr(process: number | string): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.getStderr(String(process)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  /** Return all captured stdout+stderr lines produced so far. `process` is a PID or process name given on creation. */
  async getOutput(process: number | string): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.getOutput(String(process)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  // --- Streaming (SSE) ---

  /** Stream stdout events until the process exits. `process` is a PID or process name given on creation. */
  async *followStdout(
    process: number | string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followStdout(String(process), emit),
      { sandboxId: this.sandboxId },
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  /** Stream stderr events until the process exits. `process` is a PID or process name given on creation. */
  async *followStderr(
    process: number | string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followStderr(String(process), emit),
      { sandboxId: this.sandboxId },
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  /** Stream combined stdout+stderr events until the process exits. `process` is a PID or process name given on creation. */
  async *followOutput(
    process: number | string,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followOutput(String(process), emit),
      { sandboxId: this.sandboxId },
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  // --- File operations ---

  /** Read a file from the sandbox and return its raw bytes. */
  async readFile(path: string): Promise<Traced<Uint8Array>> {
    const proxy = await this.proxy.client();
    const { traceId, data } = await callNative(() => proxy.readFile(path), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(Uint8Array.from(data), { traceId });
  }

  /** Write raw bytes to a file in the sandbox, creating it if it does not exist. */
  async writeFile(path: string, content: Uint8Array): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.writeFile(path, Buffer.from(content)), {
      sandboxId: this.sandboxId,
    });
  }

  /** Delete a file from the sandbox. */
  async deleteFile(path: string): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.deleteFile(path), { sandboxId: this.sandboxId });
  }

  /** List the contents of a directory in the sandbox. */
  async listDirectory(path: string): Promise<Traced<ListDirectoryResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.listDirectory(path), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ListDirectoryResponse, {
      traceId,
    });
  }

  // --- PTY ---

  /** Create an interactive PTY session. Returns a `sessionId` and `token` for WebSocket connection via `connectPty()`. */
  async createPtySession(
    options: CreatePtySessionOptions,
  ): Promise<Traced<PtySessionInfo>> {
    const payload: Record<string, unknown> = {
      command: options.command,
      rows: options.rows ?? 24,
      cols: options.cols ?? 80,
    };
    if (options.args != null) payload.args = options.args;
    if (options.env != null) payload.env = options.env;
    if (options.workingDir != null) payload.working_dir = options.workingDir;

    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.createPtySession(JSON.stringify(payload)),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as PtySessionInfo, { traceId });
  }

  /** Create a PTY session and connect to it immediately. Cleans up the session if the WebSocket connection fails. */
  async createPty(options: CreatePtyOptions): Promise<Pty> {
    const { onData, onExit, ...createOptions } = options;
    const session = await this.createPtySession(createOptions);
    try {
      return await this.connectPty(session.sessionId, session.token, { onData, onExit });
    } catch (error) {
      try {
        const proxy = await this.proxy.client();
        await proxy.deletePtySession(session.sessionId);
      } catch {}
      throw error;
    }
  }

  /** Attach to an existing PTY session by ID and token and return a connected `Pty` handle. */
  async connectPty(
    sessionId: string,
    token: string,
    options?: PtyConnectionOptions,
  ): Promise<Pty> {
    await this.proxy.ensureResolved();
    const wsUrl = new URL(this.ptyWsUrl(sessionId, token));
    const authToken = wsUrl.searchParams.get("token") ?? token;

    const pty = new Pty({
      sessionId,
      token: authToken,
      wsUrl: wsUrl.toString(),
      wsHeaders: {
        ...this.wsHeaders,
        "X-PTY-Token": authToken,
      },
      killSession: async () => {
        const proxy = await this.proxy.client();
        await proxy.deletePtySession(sessionId);
      },
    });

    if (options?.onData) {
      pty.onData(options.onData);
    }
    if (options?.onExit) {
      pty.onExit(options.onExit);
    }

    await pty.connect();
    return pty;
  }

  /** Open a TCP tunnel to a port inside the sandbox and return the local listener. */
  async createTunnel(
    remotePort: number,
    options?: CreateTunnelOptions,
  ): Promise<TcpTunnel> {
    await this.proxy.ensureResolved();
    return TcpTunnel.listen({
      baseUrl: this.baseUrl,
      wsHeaders: this.wsHeaders,
      remotePort,
      localHost: options?.localHost,
      localPort: options?.localPort,
      connectTimeout: options?.connectTimeout,
    });
  }

  /** Connect to a sandbox VNC session for programmatic desktop control. */
  async connectDesktop(options?: ConnectDesktopOptions): Promise<Desktop> {
    await this.proxy.ensureResolved();
    const port = options?.port ?? 5901;
    const connectTimeout = options?.connectTimeout ?? 10;

    // Wait for the VNC port to be reachable inside the sandbox before
    // attempting the WebSocket tunnel handshake. Without this, freshly
    // created sandboxes (where the in-VM `vncserver` systemd unit is
    // still starting) race the tunnel: the dataplane gets `Connection
    // refused` on 127.0.0.1:<port> and the proxy returns 502 before
    // VNC has had a chance to bind. The wait is bounded by
    // `connectTimeout` along with the WS handshake and VNC negotiation
    // that follow — total wall-clock is what the caller asked for.
    const startMs = Date.now();
    const deadlineMs = startMs + connectTimeout * 1000;
    await this.waitForPortReady(port, deadlineMs);
    const remainingSecs = Math.max(0.1, (deadlineMs - Date.now()) / 1000);

    return Desktop.connect({
      baseUrl: this.baseUrl,
      wsHeaders: this.wsHeaders,
      port,
      password: options?.password,
      shared: options?.shared,
      connectTimeout: remainingSecs,
    });
  }

  /**
   * Poll the in-sandbox daemon until `127.0.0.1:port` accepts a TCP connection.
   * Uses `bash`'s `/dev/tcp` builtin via `processes/run` — no extra deps in
   * the sandbox image. `bash` is present on every image we ship.
   */
  private async waitForPortReady(
    port: number,
    deadlineMs: number,
  ): Promise<void> {
    const probeIntervalMs = 250;
    const probeProcessTimeoutSecs = 2;
    let lastError: unknown;

    while (Date.now() < deadlineMs) {
      try {
        const result = await this.run("/bin/bash", {
          args: ["-c", `exec 3<>/dev/tcp/127.0.0.1/${port}`],
          timeout: probeProcessTimeoutSecs,
        });
        if (result.exitCode === 0) {
          return;
        }
      } catch (error) {
        lastError = error;
      }
      const remainingMs = deadlineMs - Date.now();
      if (remainingMs <= 0) break;
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(probeIntervalMs, remainingMs)),
      );
    }

    const detail =
      lastError instanceof Error ? `: ${lastError.message}` : "";
    throw new SandboxError(
      `port ${port} did not become reachable inside sandbox within the connect timeout${detail}`,
    );
  }

  ptyWsUrl(sessionId: string, token: string): string {
    let wsBase: string;
    if (this.baseUrl.startsWith("https://")) {
      wsBase = "wss://" + this.baseUrl.slice(8);
    } else if (this.baseUrl.startsWith("http://")) {
      wsBase = "ws://" + this.baseUrl.slice(7);
    } else {
      wsBase = this.baseUrl;
    }
    return `${wsBase}/api/v1/pty/${sessionId}/ws?token=${token}`;
  }

  // --- Health ---

  /** Check the sandbox daemon health. */
  async health(): Promise<Traced<HealthResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.health(), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as HealthResponse, { traceId });
  }

  /** Get sandbox daemon info (version, uptime, process counts). */
  async daemonInfo(): Promise<Traced<DaemonInfo>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.info(), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as DaemonInfo, { traceId });
  }
}
