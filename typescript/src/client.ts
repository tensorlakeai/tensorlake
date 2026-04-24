import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { type Traced, HttpClient } from "./http.js";
import {
  type CheckpointOptions,
  type ConnectOptions,
  type CreateAndConnectOptions,
  type CreatePoolOptions,
  type CreateSandboxOptions,
  type CreateSandboxPoolResponse,
  type CreateSandboxResponse,
  type CreateSnapshotResponse,
  type SandboxClientOptions,
  type SandboxInfo,
  type SandboxPortAccess,
  type SandboxPoolInfo,
  SandboxStatus,
  type SnapshotAndWaitOptions,
  type SnapshotInfo,
  type SnapshotOptions,
  SnapshotStatus,
  type SuspendResumeOptions,
  type UpdatePoolOptions,
  type UpdateSandboxOptions,
  fromSnakeKeys,
  toSnakeKeys,
} from "./models.js";
import { Sandbox } from "./sandbox.js";
import { isLocalhost, lifecyclePath, resolveProxyUrl } from "./url.js";

/**
 * Client for managing TensorLake sandboxes, pools, and snapshots.
 *
 * Use `SandboxClient.forCloud()` or `SandboxClient.forLocalhost()` for
 * clearer construction depending on your deployment target.
 */
export class SandboxClient {
  private readonly http: HttpClient;
  private readonly apiUrl: string;
  private readonly apiKey: string | undefined;
  private readonly organizationId: string | undefined;
  private readonly projectId: string | undefined;
  private readonly namespace: string;
  private readonly local: boolean;

  /** @internal Pass `true` to suppress the deprecation warning when used by `Sandbox.create()` / `Sandbox.connect()`. */
  constructor(options?: SandboxClientOptions, _internal = false) {
    if (!_internal) {
      console.warn(
        "[tensorlake] SandboxClient is deprecated; use Sandbox.create() / Sandbox.connect() instead.",
      );
    }
    this.apiUrl = options?.apiUrl ?? defaults.API_URL;
    this.apiKey = options?.apiKey ?? defaults.API_KEY;
    this.organizationId = options?.organizationId;
    this.projectId = options?.projectId;
    this.namespace = options?.namespace ?? defaults.NAMESPACE;
    this.local = isLocalhost(this.apiUrl);

    this.http = new HttpClient({
      baseUrl: this.apiUrl,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
      maxRetries: options?.maxRetries ?? defaults.MAX_RETRIES,
      retryBackoffMs: options?.retryBackoffMs ?? defaults.RETRY_BACKOFF_MS,
    });
  }

  /** Create a client for the TensorLake cloud platform. */
  static forCloud(options?: {
    apiKey?: string;
    organizationId?: string;
    projectId?: string;
    apiUrl?: string;
  }): SandboxClient {
    return new SandboxClient({
      apiUrl: options?.apiUrl ?? "https://api.tensorlake.ai",
      apiKey: options?.apiKey,
      organizationId: options?.organizationId,
      projectId: options?.projectId,
    });
  }

  /** Create a client for a local Indexify server. */
  static forLocalhost(options?: {
    apiUrl?: string;
    namespace?: string;
  }): SandboxClient {
    return new SandboxClient({
      apiUrl: options?.apiUrl ?? "http://localhost:8900",
      namespace: options?.namespace ?? "default",
    });
  }

  close(): void {
    this.http.close();
  }

  // --- Path helper ---

  private path(subpath: string): string {
    return lifecyclePath(subpath, this.local, this.namespace);
  }

  // --- Sandbox CRUD ---

  /** Create a new sandbox. Returns immediately; the sandbox may still be starting. Use `createAndConnect()` for a blocking, ready-to-use handle. */
  async create(options?: CreateSandboxOptions): Promise<Traced<CreateSandboxResponse>> {
    const body: Record<string, unknown> = {
      resources: {
        cpus: options?.cpus ?? 1.0,
        memory_mb: options?.memoryMb ?? 1024,
        ...(options?.diskMb != null ? { disk_mb: options.diskMb } : {}),
      },
    };

    if (options?.image != null) body.image = options.image;
    if (options?.secretNames != null) body.secret_names = options.secretNames;
    if (options?.timeoutSecs != null) body.timeout_secs = options.timeoutSecs;
    if (options?.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options?.snapshotId != null) body.snapshot_id = options.snapshotId;
    if (options?.name != null) body.name = options.name;

    if (
      options?.allowInternetAccess === false ||
      options?.allowOut != null ||
      options?.denyOut != null
    ) {
      body.network = {
        allow_internet_access: options?.allowInternetAccess ?? true,
        allow_out: options?.allowOut ?? [],
        deny_out: options?.denyOut ?? [],
      };
    }

    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path("sandboxes"),
      { body },
    );
    const result = fromSnakeKeys(raw, "sandboxId") as CreateSandboxResponse;
    return Object.assign(result, { traceId: raw.traceId }) as Traced<CreateSandboxResponse>;
  }

  /** Get current state and metadata for a sandbox by ID. */
  async get(sandboxId: string): Promise<SandboxInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`sandboxes/${sandboxId}`),
    );
    return fromSnakeKeys(raw, "sandboxId") as SandboxInfo;
  }

  /** List all sandboxes in the namespace. */
  async list(): Promise<SandboxInfo[]> {
    const raw = await this.http.requestJson<{ sandboxes: Record<string, unknown>[] }>(
      "GET",
      this.path("sandboxes"),
    );
    return (raw.sandboxes ?? []).map(
      (s) => fromSnakeKeys(s, "sandboxId") as SandboxInfo,
    );
  }

  /** Update sandbox properties such as name, exposed ports, and proxy auth settings. */
  async update(sandboxId: string, options: UpdateSandboxOptions): Promise<SandboxInfo> {
    const body: Record<string, unknown> = {};
    if (options.name != null) body.name = options.name;
    if (options.allowUnauthenticatedAccess != null) {
      body.allow_unauthenticated_access = options.allowUnauthenticatedAccess;
    }
    if (options.exposedPorts != null) {
      body.exposed_ports = normalizeUserPorts(options.exposedPorts);
    }
    if (Object.keys(body).length === 0) {
      throw new SandboxError("At least one sandbox update field must be provided.");
    }
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "PATCH",
      this.path(`sandboxes/${sandboxId}`),
      { body },
    );
    return fromSnakeKeys(raw, "sandboxId") as SandboxInfo;
  }

  /** Get the current proxy port settings for a sandbox. */
  async getPortAccess(sandboxId: string): Promise<SandboxPortAccess> {
    const info = await this.get(sandboxId);
    return {
      allowUnauthenticatedAccess: info.allowUnauthenticatedAccess ?? false,
      exposedPorts: dedupeAndSortPorts(info.exposedPorts ?? []),
      sandboxUrl: info.sandboxUrl,
    };
  }

  /** Add one or more user ports to the sandbox proxy allowlist. */
  async exposePorts(
    sandboxId: string,
    ports: number[],
    options?: { allowUnauthenticatedAccess?: boolean },
  ): Promise<SandboxInfo> {
    const requestedPorts = normalizeUserPorts(ports);
    const current = await this.getPortAccess(sandboxId);
    const desiredPorts = dedupeAndSortPorts([
      ...current.exposedPorts,
      ...requestedPorts,
    ]);
    return this.update(sandboxId, {
      allowUnauthenticatedAccess:
        options?.allowUnauthenticatedAccess ??
        current.allowUnauthenticatedAccess,
      exposedPorts: desiredPorts,
    });
  }

  /** Remove one or more user ports from the sandbox proxy allowlist. */
  async unexposePorts(
    sandboxId: string,
    ports: number[],
  ): Promise<SandboxInfo> {
    const requestedPorts = normalizeUserPorts(ports);
    const current = await this.getPortAccess(sandboxId);
    const toRemove = new Set(requestedPorts);
    const desiredPorts = current.exposedPorts.filter((port) => !toRemove.has(port));
    return this.update(sandboxId, {
      allowUnauthenticatedAccess: desiredPorts.length
        ? current.allowUnauthenticatedAccess
        : false,
      exposedPorts: desiredPorts,
    });
  }

  /** Terminate and delete a sandbox. */
  async delete(sandboxId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`sandboxes/${sandboxId}`),
    );
  }

  /**
   * Suspend a named sandbox, preserving its state for later resume.
   *
   * Only sandboxes created with a `name` can be suspended; ephemeral sandboxes
   * cannot. By default blocks until the sandbox is fully `Suspended`. Pass
   * `{ wait: false }` to return immediately after the request is sent
   * (fire-and-return); the server processes the suspend asynchronously.
   *
   * @param sandboxId - ID or name of the sandbox.
   * @param options.wait - If `true` (default), poll until `Suspended`. Pass `false` to fire-and-return.
   * @param options.timeout - Max seconds to wait when `wait=true` (default 300).
   * @param options.pollInterval - Seconds between status polls when `wait=true` (default 1).
   * @throws {SandboxError} If `wait=true` and the sandbox does not reach `Suspended` within `timeout`.
   */
  async suspend(sandboxId: string, options?: SuspendResumeOptions): Promise<void> {
    await this.http.requestResponse(
      "POST",
      this.path(`sandboxes/${sandboxId}/suspend`),
    );
    if (options?.wait === false) return;
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const deadline = Date.now() + timeout * 1000;
    while (Date.now() < deadline) {
      const info = await this.get(sandboxId);
      if (info.status === SandboxStatus.SUSPENDED) return;
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(`Sandbox ${sandboxId} terminated while waiting for suspend`);
      }
      await sleep(pollInterval * 1000);
    }
    throw new SandboxError(`Sandbox ${sandboxId} did not suspend within ${timeout}s`);
  }

  /**
   * Resume a suspended sandbox and bring it back to `Running`.
   *
   * By default blocks until the sandbox is `Running` and routable. Pass
   * `{ wait: false }` to return immediately after the request is sent
   * (fire-and-return); the server processes the resume asynchronously.
   *
   * @param sandboxId - ID or name of the sandbox.
   * @param options.wait - If `true` (default), poll until `Running`. Pass `false` to fire-and-return.
   * @param options.timeout - Max seconds to wait when `wait=true` (default 300).
   * @param options.pollInterval - Seconds between status polls when `wait=true` (default 1).
   * @throws {SandboxError} If `wait=true` and the sandbox does not reach `Running` within `timeout`.
   */
  async resume(sandboxId: string, options?: SuspendResumeOptions): Promise<void> {
    await this.http.requestResponse(
      "POST",
      this.path(`sandboxes/${sandboxId}/resume`),
    );
    if (options?.wait === false) return;
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const deadline = Date.now() + timeout * 1000;
    while (Date.now() < deadline) {
      const info = await this.get(sandboxId);
      if (info.status === SandboxStatus.RUNNING) return;
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(`Sandbox ${sandboxId} terminated while waiting for resume`);
      }
      await sleep(pollInterval * 1000);
    }
    throw new SandboxError(`Sandbox ${sandboxId} did not resume within ${timeout}s`);
  }

  /** Claim a warm sandbox from a pool, creating one if no warm containers are available. */
  async claim(poolId: string): Promise<Traced<CreateSandboxResponse>> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path(`sandbox-pools/${poolId}/sandboxes`),
    );
    const result = fromSnakeKeys(raw, "sandboxId") as CreateSandboxResponse;
    return Object.assign(result, { traceId: raw.traceId }) as Traced<CreateSandboxResponse>;
  }

  // --- Snapshots ---

  /**
   * Request a snapshot of a running sandbox's filesystem.
   *
   * This call **returns immediately** with a `snapshotId` and `in_progress`
   * status — the snapshot is created asynchronously. Poll `getSnapshot()` until
   * `completed` or `failed`, or use `snapshotAndWait()` to block automatically.
   *
   * @param options.contentMode - `"filesystem_only"` for cold-boot snapshots (e.g. image builds).
   *   Omit to use the server default (full VM snapshot).
   */
  async snapshot(
    sandboxId: string,
    options?: SnapshotOptions,
  ): Promise<CreateSnapshotResponse> {
    // Preserve today's wire shape (no body) when contentMode is not set.
    const requestOptions =
      options?.contentMode != null
        ? { body: { snapshot_content_mode: options.contentMode } }
        : undefined;
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path(`sandboxes/${sandboxId}/snapshot`),
      requestOptions,
    );
    return fromSnakeKeys(raw, "snapshotId") as CreateSnapshotResponse;
  }

  /** Get current status and metadata for a snapshot by ID. */
  async getSnapshot(snapshotId: string): Promise<SnapshotInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`snapshots/${snapshotId}`),
    );
    return fromSnakeKeys(raw, "snapshotId") as SnapshotInfo;
  }

  /** List all snapshots in the namespace. */
  async listSnapshots(): Promise<SnapshotInfo[]> {
    const raw = await this.http.requestJson<{ snapshots: Record<string, unknown>[] }>(
      "GET",
      this.path("snapshots"),
    );
    return (raw.snapshots ?? []).map(
      (s) => fromSnakeKeys(s, "snapshotId") as SnapshotInfo,
    );
  }

  /** Delete a snapshot by ID. */
  async deleteSnapshot(snapshotId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`snapshots/${snapshotId}`),
    );
  }

  /**
   * Create a snapshot and block until it is committed.
   *
   * Combines `snapshot()` with polling `getSnapshot()` until `completed`.
   * Prefer `sandbox.checkpoint()` on a `Sandbox` handle for the same behavior
   * without managing the client separately.
   *
   * @param sandboxId - ID of the running sandbox to snapshot.
   * @param options.timeout - Max seconds to wait (default 300).
   * @param options.pollInterval - Seconds between status polls (default 1).
   * @param options.contentMode - Content mode passed through to `snapshot()`.
   * @throws {SandboxError} If the snapshot fails or `timeout` elapses.
   */
  async snapshotAndWait(
    sandboxId: string,
    options?: SnapshotAndWaitOptions,
  ): Promise<SnapshotInfo> {
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;

    const result = await this.snapshot(sandboxId, {
      contentMode: options?.contentMode,
    });
    const deadline = Date.now() + timeout * 1000;

    while (Date.now() < deadline) {
      const info = await this.getSnapshot(result.snapshotId);
      if (info.status === SnapshotStatus.COMPLETED) return info;
      if (info.status === SnapshotStatus.FAILED) {
        throw new SandboxError(
          `Snapshot ${result.snapshotId} failed: ${info.error}`,
        );
      }
      await sleep(pollInterval * 1000);
    }

    throw new SandboxError(
      `Snapshot ${result.snapshotId} did not complete within ${timeout}s`,
    );
  }

  // --- Pools ---

  /** Create a new sandbox pool with warm pre-booted containers. */
  async createPool(options: CreatePoolOptions): Promise<CreateSandboxPoolResponse> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 1024,
        ephemeral_disk_mb: options.ephemeralDiskMb ?? 1024,
      },
      timeout_secs: options.timeoutSecs ?? 0,
    };

    if (options.secretNames != null) body.secret_names = options.secretNames;
    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null) body.max_containers = options.maxContainers;
    if (options.warmContainers != null) body.warm_containers = options.warmContainers;

    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path("sandbox-pools"),
      { body },
    );
    return fromSnakeKeys(raw, "poolId") as CreateSandboxPoolResponse;
  }

  /** Get current state and metadata for a sandbox pool by ID. */
  async getPool(poolId: string): Promise<SandboxPoolInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`sandbox-pools/${poolId}`),
    );
    return fromSnakeKeys(raw, "poolId") as SandboxPoolInfo;
  }

  /** List all sandbox pools in the namespace. */
  async listPools(): Promise<SandboxPoolInfo[]> {
    const raw = await this.http.requestJson<{ pools: Record<string, unknown>[] }>(
      "GET",
      this.path("sandbox-pools"),
    );
    return (raw.pools ?? []).map(
      (p) => fromSnakeKeys(p, "poolId") as SandboxPoolInfo,
    );
  }

  /** Replace the configuration of an existing sandbox pool. */
  async updatePool(
    poolId: string,
    options: UpdatePoolOptions,
  ): Promise<SandboxPoolInfo> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 1024,
        ephemeral_disk_mb: options.ephemeralDiskMb ?? 1024,
      },
      timeout_secs: options.timeoutSecs ?? 0,
    };

    if (options.secretNames != null) body.secret_names = options.secretNames;
    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null) body.max_containers = options.maxContainers;
    if (options.warmContainers != null) body.warm_containers = options.warmContainers;

    const raw = await this.http.requestJson<Record<string, unknown>>(
      "PUT",
      this.path(`sandbox-pools/${poolId}`),
      { body },
    );
    return fromSnakeKeys(raw, "poolId") as SandboxPoolInfo;
  }

  /** Delete a sandbox pool. Fails if the pool has active containers. */
  async deletePool(poolId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`sandbox-pools/${poolId}`),
    );
  }

  // --- Connect ---

  /** Return a `Sandbox` handle for an existing running sandbox without verifying it exists. */
  connect(identifier: string, proxyUrl?: string, routingHint?: string): Sandbox {
    const resolvedProxy = proxyUrl ?? resolveProxyUrl(this.apiUrl);
    return new Sandbox({
      sandboxId: identifier,
      proxyUrl: resolvedProxy,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
      routingHint,
    });
  }

  /**
   * Create a sandbox, wait for it to reach `Running`, and return a connected handle.
   *
   * Blocks until the sandbox is ready or `startupTimeout` elapses. The returned
   * `Sandbox` auto-terminates when `terminate()` is called.
   *
   * @param options.startupTimeout - Max seconds to wait for `Running` status (default 60).
   * @throws {SandboxError} If the sandbox terminates during startup or the timeout elapses.
   */
  async createAndConnect(
    options?: CreateAndConnectOptions,
  ): Promise<Sandbox> {
    const startupTimeout = options?.startupTimeout ?? 60;

    let result: Traced<CreateSandboxResponse>;
    if (options?.poolId != null) {
      result = await this.claim(options.poolId);
    } else {
      result = await this.create(options);
    }

    // Fast path: the blocking create/claim response already carries Running status
    // and a short-lived routing hint. Use it immediately to skip an extra poll RTT
    // and let the proxy route the first request without a placement lookup.
    if (result.status === SandboxStatus.RUNNING) {
      const sandbox = this.connect(result.sandboxId, options?.proxyUrl, result.routingHint);
      sandbox._setOwner(this);
      sandbox.traceId = result.traceId;
      return sandbox;
    }

    const deadline = Date.now() + startupTimeout * 1000;

    while (Date.now() < deadline) {
      const info = await this.get(result.sandboxId);
      if (info.status === SandboxStatus.RUNNING) {
        const sandbox = this.connect(result.sandboxId, options?.proxyUrl, info.routingHint);
        sandbox._setOwner(this);
        sandbox.traceId = result.traceId;
        return sandbox;
      }
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(
          `Sandbox ${result.sandboxId} terminated during startup`,
        );
      }
      await sleep(500);
    }

    // Timed out — clean up
    try {
      await this.delete(result.sandboxId);
    } catch {
      // ignore cleanup failures
    }
    throw new SandboxError(
      `Sandbox ${result.sandboxId} did not start within ${startupTimeout}s`,
    );
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const RESERVED_SANDBOX_MANAGEMENT_PORT = 9501;

function normalizeUserPorts(ports: number[]): number[] {
  return dedupeAndSortPorts(ports.map(validateUserPort));
}

function validateUserPort(port: number): number {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new SandboxError(`invalid port '${port}'`);
  }
  if (port === RESERVED_SANDBOX_MANAGEMENT_PORT) {
    throw new SandboxError("port 9501 is reserved for sandbox management");
  }
  return port;
}

function dedupeAndSortPorts(ports: number[]): number[] {
  return [...new Set(ports)].sort((a, b) => a - b);
}
