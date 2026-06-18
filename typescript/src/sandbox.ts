import type { SandboxClient } from "./client.js";
import {
  type ConnectDesktopOptions,
  Desktop,
} from "./desktop.js";
import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
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
  type DirectoryEntry,
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
  type SandboxOptions,
  type SendSignalResponse,
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
import { resolveProxyTarget } from "./url.js";
import WebSocket, { type RawData } from "ws";

class SandboxProxyConnection {
  baseUrl = "";
  wsHeaders: Record<string, string> = {};

  private nativeProxy: NativeSandboxProxyClient;
  private resolveProxyInfo?: (
    identifier: string,
  ) => Promise<Traced<SandboxInfo>>;
  private resolvePromise: Promise<void> | null = null;
  private routingHint?: string;

  constructor(
    private readonly sandbox: Sandbox,
    private readonly options: SandboxOptions,
  ) {
    this.routingHint = options.routingHint;
    this.nativeProxy = this.configureProxy(
      options.proxyUrl ?? defaults.SANDBOX_PROXY_URL,
      options.sandboxId,
      options.routingHint,
    );
    this.resolveProxyInfo = options.resolveProxyInfo;
  }

  async ensureResolved(): Promise<void> {
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

    this.resolvePromise = this.resolveProxyInfo(identifier)
      .then((info) => {
        this.resolveProxyInfo = undefined;
        this.sandbox.traceId = info.traceId;
        this.sandbox._setLifecycleIdentifier(info.sandboxId);
        this.sandbox._setName(info.name ?? null);
        this.routingHint = this.routingHint ?? info.routingHint;
        const proxyUrl =
          info.ingressEndpoint ?? this.options.proxyUrl ?? defaults.SANDBOX_PROXY_URL;
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
        });
      })
      .finally(() => {
        this.resolvePromise = null;
      });

    return this.resolvePromise;
  }

  /** Await proxy resolution and return the Rust-backed proxy client. */
  async client(): Promise<NativeSandboxProxyClient> {
    await this.ensureResolved();
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
    // `baseUrl`/`wsHeaders` are still computed here for the WebSocket consumers
    // (PTY, tunnel, desktop), which do not flow through the native HTTP client.
    const { baseUrl, hostHeader, sandboxIdHeader } = resolveProxyTarget(
      proxyUrl,
      sandboxId,
    );
    this.baseUrl = baseUrl;
    this.wsHeaders = {};
    if (this.options.apiKey) {
      this.wsHeaders.Authorization = `Bearer ${this.options.apiKey}`;
    }
    if (this.options.organizationId) {
      this.wsHeaders["X-Forwarded-Organization-Id"] = this.options.organizationId;
    }
    if (this.options.projectId) {
      this.wsHeaders["X-Forwarded-Project-Id"] = this.options.projectId;
    }
    if (hostHeader) {
      this.wsHeaders.Host = hostHeader;
    }
    if (sandboxIdHeader) {
      this.wsHeaders["X-Tensorlake-Sandbox-Id"] = sandboxIdHeader;
    }

    // Prefer minting from the shared lifecycle client so the proxy reuses its
    // connection pool; fall back to a standalone client when none was wired.
    if (this.options.nativeClient) {
      return this.options.nativeClient.connectProxy(
        proxyUrl,
        sandboxId,
        routingHint ?? null,
        this.proxyRequestTimeoutSec(),
      );
    }
    const binding = loadNativeSandboxBinding();
    return new binding.NativeSandboxProxyClient(
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
export class Sandbox {
  readonly sandboxId: string;
  traceId: string | null = null;
  private readonly proxy: SandboxProxyConnection;
  private ownsSandbox = false;
  private lifecycleClient: SandboxClient | null = null;
  private lifecycleIdentifier: string;
  private sandboxName: string | null = null;

  constructor(options: SandboxOptions) {
    this.sandboxId = options.sandboxId;
    this.lifecycleIdentifier = options.sandboxId;
    this.proxy = new SandboxProxyConnection(this, options);
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
   * When `proxyUrl` is omitted, resolves the sandbox first so the handle uses
   * the correct cloud/region ingress endpoint. Does **not** auto-resume a
   * suspended sandbox — call `sandbox.resume()` explicitly.
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
   * Update this sandbox's properties (name, exposed ports, proxy auth).
   *
   * Naming an ephemeral sandbox makes it non-ephemeral and enables
   * suspend/resume.
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
   * By default blocks until the sandbox is `Running` and routable. Pass
   * `{ wait: false }` for fire-and-return.
   */
  async resume(options?: SuspendResumeOptions): Promise<void> {
    const client = this.requireLifecycleClient("resume");
    await client.resume(this.lifecycleIdentifier, options);
  }

  /** Live-copy this running sandbox. */
  async copy(options?: CopySandboxOptions): Promise<Traced<CopySandboxResponse>> {
    const client = this.requireLifecycleClient("copy");
    return client.copy(this.lifecycleIdentifier, options);
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
    if (options?.name != null) payload.name = options.name;
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

  /** Get current status and metadata for a process by PID. */
  async getProcess(pid: number): Promise<Traced<ProcessInfo>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.getProcess(pid), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ProcessInfo, { traceId });
  }

  /** Send SIGKILL to a process. */
  async killProcess(pid: number): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.killProcess(pid), { sandboxId: this.sandboxId });
  }

  /** Restart a managed process by PID. */
  async restartProcess(pid: number): Promise<Traced<ProcessInfo>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.restartProcess(pid), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as ProcessInfo, { traceId });
  }

  /** Send an arbitrary signal to a process (e.g. `15` for SIGTERM, `9` for SIGKILL). */
  async sendSignal(pid: number, signal: number): Promise<Traced<SendSignalResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(
      () => proxy.sendSignal(pid, signal),
      { sandboxId: this.sandboxId },
    );
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as SendSignalResponse, {
      traceId,
    });
  }

  // --- Process I/O ---

  /** Write bytes to a process's stdin. The process must have been started with `stdinMode: StdinMode.PIPE`. */
  async writeStdin(pid: number, data: Uint8Array): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.writeStdin(pid, Buffer.from(data)), {
      sandboxId: this.sandboxId,
    });
  }

  /** Close a process's stdin pipe, signalling EOF to the process. */
  async closeStdin(pid: number): Promise<void> {
    const proxy = await this.proxy.client();
    await callNative(() => proxy.closeStdin(pid), { sandboxId: this.sandboxId });
  }

  /** Return all captured stdout lines produced so far by a process. */
  async getStdout(pid: number): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.getStdout(pid), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  /** Return all captured stderr lines produced so far by a process. */
  async getStderr(pid: number): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.getStderr(pid), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  /** Return all captured stdout+stderr lines produced so far by a process. */
  async getOutput(pid: number): Promise<Traced<OutputResponse>> {
    const proxy = await this.proxy.client();
    const { traceId, json } = await callNative(() => proxy.getOutput(pid), {
      sandboxId: this.sandboxId,
    });
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as OutputResponse, { traceId });
  }

  // --- Streaming (SSE) ---

  /** Stream stdout events from a process until it exits. Yields one `OutputEvent` per line. */
  async *followStdout(
    pid: number,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followStdout(pid, emit),
      { sandboxId: this.sandboxId },
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  /** Stream stderr events from a process until it exits. Yields one `OutputEvent` per line. */
  async *followStderr(
    pid: number,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followStderr(pid, emit),
      { sandboxId: this.sandboxId },
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  /** Stream combined stdout+stderr events from a process until it exits. Yields one `OutputEvent` per line. */
  async *followOutput(
    pid: number,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const proxy = await this.proxy.client();
    for await (const raw of nativeEventStream(
      (emit) => proxy.followOutput(pid, emit),
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
