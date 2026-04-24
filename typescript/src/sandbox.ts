import type { SandboxClient } from "./client.js";
import {
  type ConnectDesktopOptions,
  Desktop,
} from "./desktop.js";
import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { HttpClient } from "./http.js";
import {
  type CheckpointOptions,
  type CommandResult,
  type ConnectOptions,
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
  type PtySessionInfo,
  type RunOptions,
  type SandboxClientOptions,
  type SandboxOptions,
  type SendSignalResponse,
  type SnapshotInfo,
  type StartProcessOptions,
  SandboxStatus,
  SnapshotStatus,
  StdinMode,
  type SuspendResumeOptions,
  fromSnakeKeys,
} from "./models.js";
import {
  type CreateTunnelOptions,
  TcpTunnel,
} from "./tunnel.js";
import { parseSSEStream } from "./sse.js";
import { resolveProxyTarget } from "./url.js";
import WebSocket, { type RawData } from "ws";

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
  private readonly http: HttpClient;
  private readonly baseUrl: string;
  private readonly wsHeaders: Record<string, string>;
  private ownsSandbox = false;
  private lifecycleClient: SandboxClient | null = null;

  constructor(options: SandboxOptions) {
    this.sandboxId = options.sandboxId;

    const proxyUrl = options.proxyUrl ?? defaults.SANDBOX_PROXY_URL;
    const { baseUrl, hostHeader } = resolveProxyTarget(proxyUrl, options.sandboxId);
    this.baseUrl = baseUrl;
    this.wsHeaders = {};
    if (options.apiKey) {
      this.wsHeaders.Authorization = `Bearer ${options.apiKey}`;
    }
    if (options.organizationId) {
      this.wsHeaders["X-Forwarded-Organization-Id"] = options.organizationId;
    }
    if (options.projectId) {
      this.wsHeaders["X-Forwarded-Project-Id"] = options.projectId;
    }
    if (hostHeader) {
      this.wsHeaders.Host = hostHeader;
    }

    this.http = new HttpClient({
      baseUrl,
      apiKey: options.apiKey,
      organizationId: options.organizationId,
      projectId: options.projectId,
      hostHeader,
      routingHint: options.routingHint,
    });
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
    const sandbox = await client.createAndConnect(options);
    sandbox.lifecycleClient = client;
    return sandbox;
  }

  /**
   * Attach to an existing sandbox and return a connected handle.
   *
   * Verifies the sandbox exists via a server GET call, then returns a handle
   * in whatever state the sandbox is in. Does **not** auto-resume a suspended
   * sandbox — call `sandbox.resume()` explicitly.
   */
  static async connect(
    options: ConnectOptions & Partial<SandboxClientOptions>,
  ): Promise<Sandbox> {
    const { SandboxClient } = await import("./client.js");
    const client = new SandboxClient(options, /* _internal */ true);
    await client.get(options.sandboxId); // throws SandboxNotFoundError if not found
    const sandbox = client.connect(options.sandboxId, options.proxyUrl, options.routingHint);
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

  /**
   * Suspend this sandbox.
   *
   * By default blocks until the sandbox is fully `Suspended`. Pass
   * `{ wait: false }` for fire-and-return.
   */
  async suspend(options?: SuspendResumeOptions): Promise<void> {
    const client = this.requireLifecycleClient("suspend");
    await client.suspend(this.sandboxId, options);
  }

  /**
   * Resume this sandbox.
   *
   * By default blocks until the sandbox is `Running` and routable. Pass
   * `{ wait: false }` for fire-and-return.
   */
  async resume(options?: SuspendResumeOptions): Promise<void> {
    const client = this.requireLifecycleClient("resume");
    await client.resume(this.sandboxId, options);
  }

  /**
   * Create a snapshot of this sandbox's filesystem and wait for it to
   * be committed.
   *
   * By default blocks until the snapshot artifact is ready and returns
   * the completed `SnapshotInfo`. Pass `{ wait: false }` to fire-and-return
   * (returns `undefined`).
   */
  async checkpoint(options?: CheckpointOptions): Promise<SnapshotInfo | undefined> {
    const client = this.requireLifecycleClient("checkpoint");
    if (options?.wait === false) {
      await client.snapshot(this.sandboxId, { contentMode: options.contentMode });
      return undefined;
    }
    return client.snapshotAndWait(this.sandboxId, {
      timeout: options?.timeout,
      pollInterval: options?.pollInterval,
      contentMode: options?.contentMode,
    });
  }

  /**
   * List snapshots taken from this sandbox.
   */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    const client = this.requireLifecycleClient("listSnapshots");
    const all = await client.listSnapshots();
    return all.filter((s) => s.sandboxId === this.sandboxId);
  }

  /** Close the HTTP client. The sandbox keeps running. */
  close(): void {
    this.http.close();
  }

  /** Terminate the sandbox and release all resources. */
  async terminate(): Promise<void> {
    const client = this.lifecycleClient;
    this.ownsSandbox = false;
    this.lifecycleClient = null;
    this.close();
    if (client) {
      await client.delete(this.sandboxId);
    }
  }

  // --- High-level convenience ---

  /**
   * Run a command to completion and return its output.
   *
   * Uses a single streaming `POST /api/v1/processes/run` request that starts
   * the process, streams output, and delivers the exit code over one connection.
   */
  async run(command: string, options?: RunOptions): Promise<CommandResult> {
    const body: Record<string, unknown> = { command };
    if (options?.args) body.args = options.args;
    if (options?.env) body.env = options.env;
    if (options?.workingDir) body.working_dir = options.workingDir;
    if (options?.timeout != null) body.timeout = options.timeout;

    const sseStream = await this.http.requestStream(
      "POST",
      "/api/v1/processes/run",
      { json: body },
    );

    const stdoutLines: string[] = [];
    const stderrLines: string[] = [];
    let exitCode = -1;

    for await (const raw of parseSSEStream<Record<string, unknown>>(sseStream)) {
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

    return {
      exitCode,
      stdout: stdoutLines.join("\n"),
      stderr: stderrLines.join("\n"),
    };
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
  ): Promise<ProcessInfo> {
    const payload: Record<string, unknown> = { command };
    if (options?.args != null) payload.args = options.args;
    if (options?.env != null) payload.env = options.env;
    if (options?.workingDir != null) payload.working_dir = options.workingDir;
    if (options?.stdinMode != null && options.stdinMode !== StdinMode.CLOSED) {
      payload.stdin_mode = options.stdinMode;
    }
    if (options?.stdoutMode != null && options.stdoutMode !== OutputMode.CAPTURE) {
      payload.stdout_mode = options.stdoutMode;
    }
    if (options?.stderrMode != null && options.stderrMode !== OutputMode.CAPTURE) {
      payload.stderr_mode = options.stderrMode;
    }

    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      "/api/v1/processes",
      { body: payload },
    );
    return fromSnakeKeys(raw) as ProcessInfo;
  }

  /** List all processes (running and exited) tracked by the sandbox daemon. */
  async listProcesses(): Promise<ProcessInfo[]> {
    const raw = await this.http.requestJson<{ processes: Record<string, unknown>[] }>(
      "GET",
      "/api/v1/processes",
    );
    return (raw.processes ?? []).map((p) => fromSnakeKeys(p) as ProcessInfo);
  }

  /** Get current status and metadata for a process by PID. */
  async getProcess(pid: number): Promise<ProcessInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}`,
    );
    return fromSnakeKeys(raw) as ProcessInfo;
  }

  /** Send SIGKILL to a process. */
  async killProcess(pid: number): Promise<void> {
    await this.http.requestJson("DELETE", `/api/v1/processes/${pid}`);
  }

  /** Send an arbitrary signal to a process (e.g. `15` for SIGTERM, `9` for SIGKILL). */
  async sendSignal(pid: number, signal: number): Promise<SendSignalResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      `/api/v1/processes/${pid}/signal`,
      { body: { signal } },
    );
    return fromSnakeKeys(raw) as SendSignalResponse;
  }

  // --- Process I/O ---

  /** Write bytes to a process's stdin. The process must have been started with `stdinMode: StdinMode.PIPE`. */
  async writeStdin(pid: number, data: Uint8Array): Promise<void> {
    await this.http.requestBytes("POST", `/api/v1/processes/${pid}/stdin`, {
      body: data,
      contentType: "application/octet-stream",
    });
  }

  /** Close a process's stdin pipe, signalling EOF to the process. */
  async closeStdin(pid: number): Promise<void> {
    await this.http.requestJson("POST", `/api/v1/processes/${pid}/stdin/close`);
  }

  /** Return all captured stdout lines produced so far by a process. */
  async getStdout(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/stdout`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  /** Return all captured stderr lines produced so far by a process. */
  async getStderr(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/stderr`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  /** Return all captured stdout+stderr lines produced so far by a process. */
  async getOutput(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/output`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  // --- Streaming (SSE) ---

  /** Stream stdout events from a process until it exits. Yields one `OutputEvent` per line. */
  async *followStdout(
    pid: number,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<OutputEvent> {
    const stream = await this.http.requestStream(
      "GET",
      `/api/v1/processes/${pid}/stdout/follow`,
      options,
    );
    for await (const raw of parseSSEStream<Record<string, unknown>>(
      stream,
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
    const stream = await this.http.requestStream(
      "GET",
      `/api/v1/processes/${pid}/stderr/follow`,
      options,
    );
    for await (const raw of parseSSEStream<Record<string, unknown>>(
      stream,
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
    const stream = await this.http.requestStream(
      "GET",
      `/api/v1/processes/${pid}/output/follow`,
      options,
    );
    for await (const raw of parseSSEStream<Record<string, unknown>>(
      stream,
      options?.signal,
    )) {
      yield fromSnakeKeys(raw) as OutputEvent;
    }
  }

  // --- File operations ---

  /** Read a file from the sandbox and return its raw bytes. */
  async readFile(path: string): Promise<Uint8Array> {
    return this.http.requestBytes(
      "GET",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
    );
  }

  /** Write raw bytes to a file in the sandbox, creating it if it does not exist. */
  async writeFile(path: string, content: Uint8Array): Promise<void> {
    await this.http.requestBytes(
      "PUT",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
      { body: content, contentType: "application/octet-stream" },
    );
  }

  /** Delete a file from the sandbox. */
  async deleteFile(path: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
    );
  }

  /** List the contents of a directory in the sandbox. */
  async listDirectory(path: string): Promise<ListDirectoryResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/files/list?path=${encodeURIComponent(path)}`,
    );
    return fromSnakeKeys(raw) as ListDirectoryResponse;
  }

  // --- PTY ---

  /** Create an interactive PTY session. Returns a `sessionId` and `token` for WebSocket connection via `connectPty()`. */
  async createPtySession(
    options: CreatePtySessionOptions,
  ): Promise<PtySessionInfo> {
    const payload: Record<string, unknown> = {
      command: options.command,
      rows: options.rows ?? 24,
      cols: options.cols ?? 80,
    };
    if (options.args != null) payload.args = options.args;
    if (options.env != null) payload.env = options.env;
    if (options.workingDir != null) payload.working_dir = options.workingDir;

    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      "/api/v1/pty",
      { body: payload },
    );
    return fromSnakeKeys(raw) as PtySessionInfo;
  }

  /** Create a PTY session and connect to it immediately. Cleans up the session if the WebSocket connection fails. */
  async createPty(options: CreatePtyOptions): Promise<Pty> {
    const { onData, onExit, ...createOptions } = options;
    const session = await this.createPtySession(createOptions);
    try {
      return await this.connectPty(session.sessionId, session.token, { onData, onExit });
    } catch (error) {
      try {
        await this.http.requestResponse("DELETE", `/api/v1/pty/${session.sessionId}`);
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
        await this.http.requestResponse("DELETE", `/api/v1/pty/${sessionId}`);
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
    return Desktop.connect({
      baseUrl: this.baseUrl,
      wsHeaders: this.wsHeaders,
      port: options?.port,
      password: options?.password,
      shared: options?.shared,
      connectTimeout: options?.connectTimeout,
    });
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
  async health(): Promise<HealthResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      "/api/v1/health",
    );
    return fromSnakeKeys(raw) as HealthResponse;
  }

  /** Get sandbox daemon info (version, uptime, process counts). */
  async info(): Promise<DaemonInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      "/api/v1/info",
    );
    return fromSnakeKeys(raw) as DaemonInfo;
  }
}
