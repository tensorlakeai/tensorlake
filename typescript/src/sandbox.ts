import type { SandboxClient } from "./client.js";
import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { HttpClient } from "./http.js";
import {
  type CommandResult,
  type CreatePtySessionOptions,
  type DaemonInfo,
  type DirectoryEntry,
  type HealthResponse,
  type ListDirectoryResponse,
  OutputMode,
  type OutputEvent,
  type OutputResponse,
  type ProcessInfo,
  ProcessStatus,
  type PtySessionInfo,
  type RunOptions,
  type SandboxOptions,
  type SendSignalResponse,
  type StartProcessOptions,
  StdinMode,
  fromSnakeKeys,
} from "./models.js";
import { parseSSEStream } from "./sse.js";
import { resolveProxyTarget } from "./url.js";

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
  private ownsSandbox = false;
  private lifecycleClient: SandboxClient | null = null;

  constructor(options: SandboxOptions) {
    this.sandboxId = options.sandboxId;

    const proxyUrl = options.proxyUrl ?? defaults.SANDBOX_PROXY_URL;
    const { baseUrl, hostHeader } = resolveProxyTarget(proxyUrl, options.sandboxId);
    this.baseUrl = baseUrl;

    this.http = new HttpClient({
      baseUrl,
      apiKey: options.apiKey,
      organizationId: options.organizationId,
      projectId: options.projectId,
      hostHeader,
    });
  }

  /** @internal Used by SandboxClient.createAndConnect to set ownership. */
  _setOwner(client: SandboxClient): void {
    this.ownsSandbox = true;
    this.lifecycleClient = client;
  }

  close(): void {
    this.http.close();
  }

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

  async run(command: string, options?: RunOptions): Promise<CommandResult> {
    const proc = await this.startProcess(command, {
      args: options?.args,
      env: options?.env,
      workingDir: options?.workingDir,
    });

    const deadline = options?.timeout
      ? Date.now() + options.timeout * 1000
      : null;

    let info: ProcessInfo;
    while (true) {
      info = await this.getProcess(proc.pid);
      if (info.status !== ProcessStatus.RUNNING) break;
      if (deadline && Date.now() > deadline) {
        await this.killProcess(proc.pid);
        throw new SandboxError(`Command timed out after ${options!.timeout}s`);
      }
      await sleep(100);
    }

    const stdoutResp = await this.getStdout(proc.pid);
    const stderrResp = await this.getStderr(proc.pid);

    let exitCode: number;
    if (info.exitCode != null) {
      exitCode = info.exitCode;
    } else if (info.signal != null) {
      exitCode = -info.signal;
    } else {
      exitCode = -1;
    }

    return {
      exitCode,
      stdout: stdoutResp.lines.join("\n"),
      stderr: stderrResp.lines.join("\n"),
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

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
