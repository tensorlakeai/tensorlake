import type { SandboxClient } from "./client.js";
import {
  type ConnectDesktopOptions,
  Desktop,
} from "./desktop.js";
import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { HttpClient } from "./http.js";
import {
  type CommandResult,
  type CreateAndConnectOptions,
  type CreatePtySessionOptions,
  type CreateSnapshotResponse,
  type DaemonInfo,
  type DirectoryEntry,
  type HealthResponse,
  type ListDirectoryResponse,
  OutputMode,
  type OutputEvent,
  type OutputResponse,
  type ProcessInfo,
  type PtySessionInfo,
  type RunOptions,
  type SandboxClientOptions,
  type SandboxOptions,
  type SendSignalResponse,
  type SnapshotAndWaitOptions,
  type SnapshotContentMode,
  type SnapshotInfo,
  type StartProcessOptions,
  StdinMode,
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
  private readonly http: HttpClient;
  private readonly baseUrl: string;
  private readonly wsHeaders: Record<string, string>;
  private ownsSandbox = false;
  private lifecycleClient: SandboxClient | null = null;
  private ownsLifecycleClient = false;
  private resolvedSandboxId: string | null = null;

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

  /** @internal Used by SandboxClient.connect to wire the lifecycle client without taking ownership. */
  _setLifecycleClient(client: SandboxClient): void {
    this.lifecycleClient = client;
  }

  /**
   * Close the proxy connection. The sandbox keeps running.
   *
   * The lifecycle client (if any) is preserved so that subsequent
   * `terminate()` calls on this handle still work.
   */
  close(): void {
    this.http.close();
  }

  async terminate(): Promise<void> {
    this.ownsSandbox = false;
    try {
      if (this.lifecycleClient) {
        await this.lifecycleClient.delete(this.sandboxId);
      }
    } finally {
      this.close();
      this.releaseOwnedLifecycleClient();
    }
  }

  private releaseOwnedLifecycleClient(): void {
    if (this.ownsLifecycleClient && this.lifecycleClient) {
      this.lifecycleClient.close();
    }
    this.ownsLifecycleClient = false;
    this.lifecycleClient = null;
  }

  /** @internal Used by factory paths to mark that this Sandbox owns (and must close) its lifecycle client. */
  _markLifecycleClientOwned(): void {
    this.ownsLifecycleClient = true;
  }

  // --- Class-level factories ---

  /** Create a new sandbox (or restore from `snapshotId`) and return a ready handle, blocking until Running. */
  static async create(
    options?: CreateAndConnectOptions &
      SandboxClientOptions & { _client?: SandboxClient },
  ): Promise<Sandbox> {
    const ownsClient = options?._client == null;
    const client = options?._client ?? (await resolveLifecycleClient(options));
    const sandbox = await client.createAndConnect(options);
    if (ownsClient) sandbox._markLifecycleClientOwned();
    return sandbox;
  }

  /** Attach to an existing sandbox by ID or name without changing its state. */
  static async connect(
    options: {
      identifier?: string;
      sandboxId?: string;
      proxyUrl?: string;
      routingHint?: string;
      _client?: SandboxClient;
    } & SandboxClientOptions,
  ): Promise<Sandbox> {
    if (
      options.identifier != null &&
      options.sandboxId != null &&
      options.identifier !== options.sandboxId
    ) {
      throw new SandboxError(
        "Provide only one of `identifier` or `sandboxId`, not both.",
      );
    }
    const identifier = options.identifier ?? options.sandboxId;
    if (!identifier) {
      throw new SandboxError(
        "Sandbox.connect requires `identifier` (sandbox ID or name).",
      );
    }
    const ownsClient = options._client == null;
    const client = options._client ?? (await resolveLifecycleClient(options));
    const sandbox = client.connect(identifier, options.proxyUrl, options.routingHint);
    if (ownsClient) sandbox._markLifecycleClientOwned();
    return sandbox;
  }

  /** Get a snapshot by ID. */
  static async getSnapshot(
    snapshotId: string,
    options?: SandboxClientOptions & { _client?: SandboxClient },
  ): Promise<SnapshotInfo> {
    const owned = options?._client == null;
    const client = options?._client ?? (await resolveLifecycleClient(options));
    try {
      return await client.getSnapshot(snapshotId);
    } finally {
      if (owned) client.close();
    }
  }

  /** Delete a snapshot by ID. */
  static async deleteSnapshot(
    snapshotId: string,
    options?: SandboxClientOptions & { _client?: SandboxClient },
  ): Promise<void> {
    const owned = options?._client == null;
    const client = options?._client ?? (await resolveLifecycleClient(options));
    try {
      await client.deleteSnapshot(snapshotId);
    } finally {
      if (owned) client.close();
    }
  }

  // --- Lifecycle: suspend / resume / checkpoint ---

  private requireLifecycleClient(operation: string): SandboxClient {
    if (this.lifecycleClient == null) {
      throw new SandboxError(
        `Cannot ${operation}: this Sandbox was constructed without a lifecycle client. ` +
          "Use Sandbox.create() / Sandbox.connect() (or SandboxClient.createAndConnect() / SandboxClient.connect()).",
      );
    }
    return this.lifecycleClient;
  }

  /** Suspend this sandbox, blocking until Suspended when `wait` is true. */
  async suspend(options?: {
    timeout?: number;
    pollInterval?: number;
    wait?: boolean;
  }): Promise<void> {
    const client = this.requireLifecycleClient("suspend");
    await client.suspend(this.sandboxId, options);
  }

  /** Resume this sandbox, blocking until Running when `wait` is true. No-op if already Running. */
  async resume(options?: {
    timeout?: number;
    pollInterval?: number;
    wait?: boolean;
  }): Promise<void> {
    const client = this.requireLifecycleClient("resume");
    await client.resume(this.sandboxId, options);
  }

  /**
   * Take a snapshot of this sandbox. Returns `SnapshotInfo` when `wait` is
   * true, else the immediate `CreateSnapshotResponse`.
   */
  async checkpoint(
    options?: SnapshotAndWaitOptions & { wait?: boolean },
  ): Promise<SnapshotInfo | CreateSnapshotResponse> {
    const client = this.requireLifecycleClient("checkpoint");
    if (options?.wait === false) {
      return client.snapshot(this.sandboxId, { contentMode: options?.contentMode });
    }
    return client.snapshotAndWait(this.sandboxId, {
      timeout: options?.timeout,
      pollInterval: options?.pollInterval,
      contentMode: options?.contentMode,
    });
  }

  /** List snapshots taken from this sandbox. */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    const client = this.requireLifecycleClient("listSnapshots");
    const [resolvedId, all] = await Promise.all([
      this.resolveSandboxId(client),
      client.listSnapshots(),
    ]);
    return all.filter((s) => s.sandboxId === resolvedId);
  }

  private async resolveSandboxId(client: SandboxClient): Promise<string> {
    if (this.resolvedSandboxId !== null) return this.resolvedSandboxId;
    const info = await client.get(this.sandboxId);
    this.resolvedSandboxId = info.sandboxId;
    return this.resolvedSandboxId;
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

  async listProcesses(): Promise<ProcessInfo[]> {
    const raw = await this.http.requestJson<{ processes: Record<string, unknown>[] }>(
      "GET",
      "/api/v1/processes",
    );
    return (raw.processes ?? []).map((p) => fromSnakeKeys(p) as ProcessInfo);
  }

  async getProcess(pid: number): Promise<ProcessInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}`,
    );
    return fromSnakeKeys(raw) as ProcessInfo;
  }

  async killProcess(pid: number): Promise<void> {
    await this.http.requestJson("DELETE", `/api/v1/processes/${pid}`);
  }

  async sendSignal(pid: number, signal: number): Promise<SendSignalResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      `/api/v1/processes/${pid}/signal`,
      { body: { signal } },
    );
    return fromSnakeKeys(raw) as SendSignalResponse;
  }

  // --- Process I/O ---

  async writeStdin(pid: number, data: Uint8Array): Promise<void> {
    await this.http.requestBytes("POST", `/api/v1/processes/${pid}/stdin`, {
      body: data,
      contentType: "application/octet-stream",
    });
  }

  async closeStdin(pid: number): Promise<void> {
    await this.http.requestJson("POST", `/api/v1/processes/${pid}/stdin/close`);
  }

  async getStdout(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/stdout`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  async getStderr(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/stderr`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  async getOutput(pid: number): Promise<OutputResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/processes/${pid}/output`,
    );
    return fromSnakeKeys(raw) as OutputResponse;
  }

  // --- Streaming (SSE) ---

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

  async readFile(path: string): Promise<Uint8Array> {
    return this.http.requestBytes(
      "GET",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
    );
  }

  async writeFile(path: string, content: Uint8Array): Promise<void> {
    await this.http.requestBytes(
      "PUT",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
      { body: content, contentType: "application/octet-stream" },
    );
  }

  async deleteFile(path: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      `/api/v1/files?path=${encodeURIComponent(path)}`,
    );
  }

  async listDirectory(path: string): Promise<ListDirectoryResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/api/v1/files/list?path=${encodeURIComponent(path)}`,
    );
    return fromSnakeKeys(raw) as ListDirectoryResponse;
  }

  // --- PTY ---

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

  async health(): Promise<HealthResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      "/api/v1/health",
    );
    return fromSnakeKeys(raw) as HealthResponse;
  }

  async info(): Promise<DaemonInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      "/api/v1/info",
    );
    return fromSnakeKeys(raw) as DaemonInfo;
  }
}

async function resolveLifecycleClient(
  options: SandboxClientOptions | undefined,
): Promise<SandboxClient> {
  const { SandboxClient: ClientCtor } = await import("./client.js");
  return new ClientCtor({
    apiUrl: options?.apiUrl,
    apiKey: options?.apiKey,
    organizationId: options?.organizationId,
    projectId: options?.projectId,
    namespace: options?.namespace,
    maxRetries: options?.maxRetries,
    retryBackoffMs: options?.retryBackoffMs,
    _internal: true,
  } as SandboxClientOptions & { _internal: true });
}
