import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import type { Traced } from "./http.js";
import {
  callNative,
  loadNativeSandboxBinding,
  type NativeErrorContext,
  type NativeSandboxClient,
} from "./native-sandbox.js";
import {
  type ArchivedSandboxInfo,
  type CopySandboxOptions,
  type CopySandboxResponse,
  type CreateAndConnectOptions,
  type CreatePoolOptions,
  type CreateSandboxOptions,
  type CreateSandboxPoolResponse,
  type CreateSandboxResponse,
  type CreateSnapshotResponse,
  type GetSandboxLogsOptions,
  type ListArchivedSandboxesOptions,
  type ListArchivedSandboxesResponse,
  type SandboxClientOptions,
  type SandboxInfo,
  type SandboxLogsResponse,
  type SandboxPortAccess,
  type SandboxPoolInfo,
  type SandboxProcessLogFiltersResponse,
  SandboxStatus,
  type SnapshotAndWaitOptions,
  type SnapshotInfo,
  type SnapshotOptions,
  SnapshotStatus,
  type SnapshotWaitCondition,
  type SuspendResumeOptions,
  type UpdatePoolOptions,
  type UpdateSandboxOptions,
  fromSnakeKeys,
} from "./models.js";
import { Sandbox } from "./sandbox.js";
import { nowMs, logSdkTimingEvent, logSdkTiming } from "./sdk-timings.js";
import { resolveProxyUrl } from "./url.js";

function gpuRequest(
  gpus: number | undefined,
  gpuModel: string | undefined,
): Array<{ count: number; model: string }> | undefined {
  if (gpus == null) return undefined;
  if (!Number.isInteger(gpus) || gpus < 1) {
    throw new SandboxError("gpus must be a positive integer");
  }
  gpuModel = gpuModel ?? "A10";
  if (gpuModel !== "A10") {
    throw new SandboxError("only A10 GPU sandboxes are supported for now");
  }
  return [{ count: gpus, model: gpuModel }];
}

/**
 * Client for managing TensorLake sandboxes, pools, and snapshots.
 *
 * This is a thin shim over the Rust core ({@link NativeSandboxClient}): every
 * call marshals a request to JSON, delegates the RPC (URL resolution,
 * namespacing, retries, connection pooling) to Rust, and reshapes the JSON
 * response. There is no TypeScript-side HTTP transport.
 */
export class SandboxClient {
  private readonly native: NativeSandboxClient;
  private readonly apiUrl: string;
  private readonly apiKey: string | undefined;
  private readonly organizationId: string | undefined;
  private readonly projectId: string | undefined;
  private readonly namespace: string;
  private readonly requestTimeoutMs: number;

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
    this.requestTimeoutMs = resolveRequestTimeoutMs(options);

    const binding = loadNativeSandboxBinding();
    this.native = new binding.NativeSandboxClient(
      this.apiUrl,
      this.apiKey ?? null,
      this.organizationId ?? null,
      this.projectId ?? null,
      this.namespace,
      null,
      this.requestTimeoutMs / 1000,
    );
  }

  /** Create a client for the TensorLake cloud platform. */
  static forCloud(options?: {
    apiKey?: string;
    organizationId?: string;
    projectId?: string;
    apiUrl?: string;
    requestTimeout?: number;
    timeoutMs?: number;
  }): SandboxClient {
    return new SandboxClient({
      apiUrl: options?.apiUrl ?? "https://api.tensorlake.ai",
      apiKey: options?.apiKey,
      organizationId: options?.organizationId,
      projectId: options?.projectId,
      requestTimeout: options?.requestTimeout,
      timeoutMs: options?.timeoutMs,
    });
  }

  /** Create a client for a local Indexify server. */
  static forLocalhost(options?: {
    apiUrl?: string;
    namespace?: string;
    requestTimeout?: number;
    timeoutMs?: number;
  }): SandboxClient {
    return new SandboxClient({
      apiUrl: options?.apiUrl ?? "http://localhost:8900",
      namespace: options?.namespace ?? "default",
      requestTimeout: options?.requestTimeout,
      timeoutMs: options?.timeoutMs,
    });
  }

  close(): void {
    // The native client releases its connection pool on GC; nothing to do.
  }

  private withRequestTimeout(requestTimeout: number | undefined): SandboxClient {
    if (requestTimeout == null) {
      return this;
    }
    const timeoutMs = secondsToMillis(requestTimeout);
    if (timeoutMs === this.requestTimeoutMs) {
      return this;
    }
    return new SandboxClient({
      apiUrl: this.apiUrl,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
      namespace: this.namespace,
      timeoutMs,
    }, /* _internal */ true);
  }

  // --- Native marshalling helpers ---

  /** Run a native JSON call and reshape it into a `Traced<T>`. */
  private async tracedJson<T extends object>(
    fn: () => Promise<{ traceId: string; json: string }>,
    idField?: string,
    context?: NativeErrorContext,
  ): Promise<Traced<T>> {
    const { traceId, json } = await callNative(fn, context);
    return Object.assign(fromSnakeKeys(JSON.parse(json), idField) as T, {
      traceId,
    }) as Traced<T>;
  }

  /** Run a native JSON call and reshape it into a plain `T` (no trace id). */
  private async plainJson<T>(
    fn: () => Promise<{ traceId: string; json: string }>,
    idField?: string,
    context?: NativeErrorContext,
  ): Promise<T> {
    const { json } = await callNative(fn, context);
    return fromSnakeKeys(JSON.parse(json), idField) as T;
  }

  // --- Sandbox CRUD ---

  /** Create a new sandbox. Returns immediately; the sandbox may still be starting. Use `createAndConnect()` for a blocking, ready-to-use handle. */
  async create(options?: CreateSandboxOptions): Promise<Traced<CreateSandboxResponse>> {
    const gpuResources = gpuRequest(options?.gpus, options?.gpuModel);
    const body: Record<string, unknown> = {
      resources: {
        cpus: options?.cpus ?? 1.0,
        memory_mb: options?.memoryMb ?? 1024,
        ...(options?.diskMb != null ? { disk_mb: options.diskMb } : {}),
        ...(gpuResources != null ? { gpus: gpuResources } : {}),
      },
    };

    if (options?.image != null) body.image = options.image;
    if (options?.timeoutSecs != null) body.timeout_secs = options.timeoutSecs;
    if (options?.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options?.snapshotId != null) body.snapshot_id = options.snapshotId;
    if (options?.name != null) body.name = options.name;
    if (
      options?.fileSystems != null &&
      options.fileSystems.length > 0
    ) {
      body.file_systems = options.fileSystems.map((fs) => ({
        file_system_id: fs.fileSystemId,
        mount_path: fs.mountPath,
      }));
    }

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

    return this.tracedJson<CreateSandboxResponse>(
      () => this.native.createSandbox(JSON.stringify(body)),
      "sandboxId",
    );
  }

  /** Get current state and metadata for a sandbox by ID. */
  async get(sandboxId: string): Promise<Traced<SandboxInfo>> {
    return this.tracedJson<SandboxInfo>(
      () => this.native.getSandbox(sandboxId),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** List all sandboxes in the namespace. */
  async list(): Promise<Traced<SandboxInfo[]>> {
    const { traceId, json } = await callNative(() => this.native.listSandboxes());
    const parsed = JSON.parse(json) as { sandboxes?: Record<string, unknown>[] };
    const sandboxes = (parsed.sandboxes ?? []).map(
      (s) => fromSnakeKeys(s, "sandboxId") as SandboxInfo,
    );
    return Object.assign(sandboxes, { traceId });
  }

  /**
   * List archived (terminated) sandboxes in the namespace.
   *
   * Archived sandboxes are terminated sandboxes parked in the server's
   * archived sandboxes store until the server-configured TTL expires.
   */
  async listArchived(
    options?: ListArchivedSandboxesOptions,
  ): Promise<Traced<ListArchivedSandboxesResponse>> {
    const { traceId, json } = await callNative(() =>
      this.native.listArchivedSandboxes(
        options?.limit ?? null,
        options?.cursor ?? null,
        options?.direction ?? null,
      ),
    );
    const parsed = JSON.parse(json) as {
      sandboxes?: Record<string, unknown>[];
      prev_cursor?: string;
      next_cursor?: string;
    };
    const sandboxes = (parsed.sandboxes ?? []).map(
      (s) => fromSnakeKeys(s, "sandboxId") as ArchivedSandboxInfo,
    );
    const response: ListArchivedSandboxesResponse = {
      sandboxes,
      prevCursor: parsed.prev_cursor,
      nextCursor: parsed.next_cursor,
    };
    return Object.assign(response, { traceId });
  }

  /** Get a single archived sandbox by id. */
  async getArchived(sandboxId: string): Promise<Traced<ArchivedSandboxInfo>> {
    return this.tracedJson<ArchivedSandboxInfo>(
      () => this.native.getArchivedSandbox(sandboxId),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Read persisted logs for a sandbox. */
  async getLogs(
    sandboxId: string,
    options?: GetSandboxLogsOptions,
  ): Promise<Traced<SandboxLogsResponse>> {
    const body = {
      sandbox_id: sandboxId,
      levels: options?.levels ?? [],
      process_ids: options?.processIds ?? [],
      next_token: options?.nextToken,
      head: options?.head,
      tail: options?.tail,
      body: options?.body,
    };
    const { traceId, json } = await callNative(
      () => this.native.getSandboxLogs(JSON.stringify(body)),
      { sandboxId, notFoundKind: "sandbox" },
    );
    return Object.assign(JSON.parse(json) as SandboxLogsResponse, { traceId });
  }

  /** List sandbox processes available as persisted-log filters. */
  async listLogProcesses(
    sandboxId: string,
  ): Promise<Traced<SandboxProcessLogFiltersResponse>> {
    const { traceId, json } = await callNative(
      () => this.native.listSandboxLogProcesses(sandboxId),
      { sandboxId, notFoundKind: "sandbox" },
    );
    return Object.assign(JSON.parse(json) as SandboxProcessLogFiltersResponse, {
      traceId,
    });
  }

  /** Update sandbox properties such as name, exposed ports, and proxy auth settings. */
  async update(sandboxId: string, options: UpdateSandboxOptions): Promise<Traced<SandboxInfo>> {
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
    return this.tracedJson<SandboxInfo>(
      () => this.native.updateSandbox(sandboxId, JSON.stringify(body)),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Get the current proxy port settings for a sandbox. */
  async getPortAccess(sandboxId: string): Promise<SandboxPortAccess> {
    const info = await this.get(sandboxId);
    return {
      allowUnauthenticatedAccess: info.allowUnauthenticatedAccess ?? false,
      exposedPorts: dedupeAndSortPorts(info.exposedPorts ?? []),
      ingressEndpoint: info.ingressEndpoint,
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
    await callNative(() => this.native.deleteSandbox(sandboxId), { sandboxId, notFoundKind: "sandbox" });
  }

  /**
   * Suspend a named sandbox, preserving its state for later resume.
   *
   * Only sandboxes created with a `name` can be suspended; ephemeral sandboxes
   * cannot. By default blocks until the sandbox is fully `Suspended`. Pass
   * `{ wait: false }` to return immediately after the request is sent
   * (fire-and-return); the server processes the suspend asynchronously.
   */
  async suspend(sandboxId: string, options?: SuspendResumeOptions): Promise<void> {
    await callNative(() => this.native.suspendSandbox(sandboxId), { sandboxId, notFoundKind: "sandbox" });
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
   */
  async resume(sandboxId: string, options?: SuspendResumeOptions): Promise<void> {
    await callNative(() => this.native.resumeSandbox(sandboxId), { sandboxId, notFoundKind: "sandbox" });
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

  /**
   * Attach a registered file system to a running sandbox at `mountPath`.
   *
   * The mount completes asynchronously on the dataplane; the returned
   * `SandboxInfo` already reflects the new entry in `fileSystems`.
   */
  async attachFileSystem(
    sandboxId: string,
    fileSystemId: string,
    mountPath: string,
  ): Promise<Traced<SandboxInfo>> {
    return this.tracedJson<SandboxInfo>(
      () =>
        this.native.attachFileSystem(sandboxId, fileSystemId, mountPath),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /**
   * Detach the file system mounted at `mountPath` from a running sandbox.
   *
   * The unmount completes asynchronously on the dataplane; the returned
   * `SandboxInfo` already reflects the removed `fileSystems` entry.
   */
  async detachFileSystem(
    sandboxId: string,
    mountPath: string,
  ): Promise<Traced<SandboxInfo>> {
    return this.tracedJson<SandboxInfo>(
      () => this.native.detachFileSystem(sandboxId, mountPath),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Claim a warm sandbox from a pool, creating one if no warm containers are available. */
  async claim(poolId: string): Promise<Traced<CreateSandboxResponse>> {
    return this.tracedJson<CreateSandboxResponse>(
      () => this.native.claimSandbox(poolId),
      "sandboxId",
      { poolId, notFoundKind: "pool" },
    );
  }

  /**
   * Live-copy a running sandbox.
   *
   * The server creates `times` running copies from the source sandbox. Partial
   * responses can include failed copies; inspect each returned sandbox's
   * `status` and `reason`.
   */
  async copy(
    sandboxId: string,
    options?: CopySandboxOptions,
  ): Promise<Traced<CopySandboxResponse>> {
    const times = options?.times ?? 1;
    if (!Number.isInteger(times) || times < 1) {
      throw new SandboxError("times must be a positive integer");
    }
    const client = this.withRequestTimeout(options?.requestTimeout);
    return client.tracedJson<CopySandboxResponse>(
      () => client.native.copySandbox(sandboxId, times),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  // --- Snapshots ---

  /**
   * Request a snapshot of a running sandbox's filesystem.
   *
   * This call **returns immediately** with a `snapshotId` and `in_progress`
   * status — the snapshot is created asynchronously. Poll `getSnapshot()` until
   * `local_ready`, `completed`, or `failed`, or use `snapshotAndWait()` to
   * block automatically.
   */
  async snapshot(
    sandboxId: string,
    options?: SnapshotOptions,
  ): Promise<CreateSnapshotResponse> {
    return this.plainJson<CreateSnapshotResponse>(
      () => this.native.createSnapshot(sandboxId, options?.snapshotType ?? null),
      "snapshotId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Get current status and metadata for a snapshot by ID. */
  async getSnapshot(snapshotId: string): Promise<Traced<SnapshotInfo>> {
    return this.tracedJson<SnapshotInfo>(
      () => this.native.getSnapshot(snapshotId),
      "snapshotId",
    );
  }

  /** List all snapshots in the namespace. */
  async listSnapshots(): Promise<Traced<SnapshotInfo[]>> {
    const { traceId, json } = await callNative(() => this.native.listSnapshots());
    const parsed = JSON.parse(json) as { snapshots?: Record<string, unknown>[] };
    const snapshots = (parsed.snapshots ?? []).map(
      (s) => fromSnakeKeys(s, "snapshotId") as SnapshotInfo,
    );
    return Object.assign(snapshots, { traceId });
  }

  /** Delete a snapshot by ID. */
  async deleteSnapshot(snapshotId: string): Promise<void> {
    await callNative(() => this.native.deleteSnapshot(snapshotId));
  }

  /**
   * Create a snapshot and block until it is locally ready.
   *
   * Combines `snapshot()` with polling `getSnapshot()` until `local_ready`
   * or `completed`. Pass `{ waitUntil: "completed" }` when durable
   * `snapshotUri` metadata is required.
   */
  async snapshotAndWait(
    sandboxId: string,
    options?: SnapshotAndWaitOptions,
  ): Promise<Traced<SnapshotInfo>> {
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const waitUntil = options?.waitUntil ?? "local_ready";

    const result = await this.snapshot(sandboxId, {
      snapshotType: options?.snapshotType,
    });
    const deadline = Date.now() + timeout * 1000;

    while (Date.now() < deadline) {
      const info = await this.getSnapshot(result.snapshotId);
      if (snapshotStatusSatisfiesWaitCondition(info.status, waitUntil)) return info;
      if (info.status === SnapshotStatus.FAILED) {
        throw new SandboxError(
          `Snapshot ${result.snapshotId} failed: ${info.error}`,
        );
      }
      await sleep(pollInterval * 1000);
    }

    throw new SandboxError(
      `Snapshot ${result.snapshotId} did not reach ${waitUntil} within ${timeout}s`,
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

    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null) body.max_containers = options.maxContainers;
    if (options.warmContainers != null) body.warm_containers = options.warmContainers;

    return this.plainJson<CreateSandboxPoolResponse>(
      () => this.native.createPool(JSON.stringify(body)),
      "poolId",
    );
  }

  /** Get current state and metadata for a sandbox pool by ID. */
  async getPool(poolId: string): Promise<SandboxPoolInfo> {
    return this.plainJson<SandboxPoolInfo>(
      () => this.native.getPool(poolId),
      "poolId",
      { poolId, notFoundKind: "pool" },
    );
  }

  /** List all sandbox pools in the namespace. */
  async listPools(): Promise<Traced<SandboxPoolInfo[]>> {
    const { traceId, json } = await callNative(() => this.native.listPools());
    const parsed = JSON.parse(json) as { pools?: Record<string, unknown>[] };
    const pools = (parsed.pools ?? []).map(
      (p) => fromSnakeKeys(p, "poolId") as SandboxPoolInfo,
    );
    return Object.assign(pools, { traceId });
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

    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null) body.max_containers = options.maxContainers;
    if (options.warmContainers != null) body.warm_containers = options.warmContainers;

    return this.plainJson<SandboxPoolInfo>(
      () => this.native.updatePool(poolId, JSON.stringify(body)),
      "poolId",
      { poolId, notFoundKind: "pool" },
    );
  }

  /** Delete a sandbox pool. Fails if the pool has active containers. */
  async deletePool(poolId: string): Promise<void> {
    await callNative(() => this.native.deletePool(poolId), { poolId, notFoundKind: "pool" });
  }

  // --- Connect ---

  /** Return a `Sandbox` handle for an existing running sandbox without verifying it exists. */
  connect(
    identifier: string,
    proxyUrl?: string,
    routingHint?: string,
    requestTimeout?: number,
  ): Sandbox {
    const resolvedProxy = proxyUrl ?? resolveProxyUrl(this.apiUrl);
    return new Sandbox({
      sandboxId: identifier,
      proxyUrl: resolvedProxy,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
      routingHint,
      resolveProxyInfo: proxyUrl == null
        ? async (currentIdentifier) => this.get(currentIdentifier)
        : undefined,
      requestTimeout,
      nativeClient: this.native,
    });
  }

  /**
   * Create a sandbox, wait for it to reach `Running`, and return a connected handle.
   *
   * Blocks until the sandbox is ready or `requestTimeout` elapses. The returned
   * `Sandbox` auto-terminates when `terminate()` is called.
   */
  async createAndConnect(
    options?: CreateAndConnectOptions,
  ): Promise<Sandbox> {
    const opStart = nowMs();
    const requestTimeout =
      options?.requestTimeout ??
      options?.startupTimeout ??
      this.requestTimeoutMs / 1000;
    const requestClient = this.withRequestTimeout(requestTimeout);
    logSdkTimingEvent("sandbox.create", "start", {
      request_timeout_s: requestTimeout,
      image: options?.image,
      pool_id: options?.poolId,
    });

    // claim() never sends options.name to the server, so only create() should fall
    // back to it locally when the server response omits a name.
    const createStart = nowMs();
    const result = options?.poolId != null
      ? await requestClient.claim(options.poolId)
      : await requestClient.create(options);
    logSdkTiming("sandbox.create", options?.poolId != null ? "claim_response" : "create_response", createStart, {
      sandbox_id: result.sandboxId,
      status: result.status,
      server_trace_id: result.traceId,
    });
    const requestedName = options?.poolId != null ? null : options?.name ?? null;

    const finishConnect = (
      routingHint: string | undefined,
      name: string | null | undefined,
      ingressEndpoint: string | undefined,
    ) => {
      const sandbox = requestClient.connect(
        result.sandboxId,
        options?.proxyUrl ?? ingressEndpoint,
        routingHint,
        requestTimeout,
      );
      sandbox._setOwner(requestClient);
      sandbox.traceId = result.traceId;
      sandbox._setLifecycleIdentifier(result.sandboxId);
      sandbox._setName(name ?? requestedName);
      logSdkTiming("sandbox.create", "complete", opStart, {
        sandbox_id: result.sandboxId,
        status: SandboxStatus.RUNNING,
        server_trace_id: result.traceId,
        routing_hint: routingHint,
        ingress_endpoint: ingressEndpoint,
      });
      return sandbox;
    };

    // Fast path: the blocking create/claim response already carries Running status
    // and a short-lived routing hint. Use it immediately to skip an extra poll RTT
    // and let the proxy route the first request without a placement lookup.
    if (result.status === SandboxStatus.RUNNING) {
      return finishConnect(result.routingHint, result.name, result.ingressEndpoint);
    }
    if (
      result.status === SandboxStatus.SUSPENDED ||
      result.status === SandboxStatus.TERMINATED
    ) {
      throw new SandboxError(
        formatStartupFailureMessage(result.sandboxId, result.status, {
          errorDetails: result.errorDetails,
          terminationReason: result.terminationReason,
        }),
      );
    }
    if (result.status === SandboxStatus.TIMEOUT) {
      try {
        await requestClient.delete(result.sandboxId);
      } catch {
        // ignore cleanup failures
      }
      throw new SandboxError(
        `Sandbox ${result.sandboxId} did not start within ${requestTimeout}s`,
      );
    }

    const deadline = Date.now() + secondsToMillis(requestTimeout);

    while (Date.now() < deadline) {
      const pollStart = nowMs();
      const info = await requestClient.get(result.sandboxId);
      logSdkTiming("sandbox.create", "poll_response", pollStart, {
        sandbox_id: result.sandboxId,
        status: info.status,
        server_trace_id: info.traceId,
      });
      if (info.status === SandboxStatus.RUNNING) {
        return finishConnect(info.routingHint, info.name, info.ingressEndpoint);
      }
      if (
        info.status === SandboxStatus.SUSPENDED ||
        info.status === SandboxStatus.TERMINATED
      ) {
        throw new SandboxError(
          formatStartupFailureMessage(result.sandboxId, info.status, {
            errorDetails: info.errorDetails,
            terminationReason: info.terminationReason,
          }),
        );
      }
      await sleep(500);
    }

    // Timed out — clean up
    try {
      await requestClient.delete(result.sandboxId);
    } catch {
      // ignore cleanup failures
    }
    throw new SandboxError(
      `Sandbox ${result.sandboxId} did not start within ${requestTimeout}s`,
    );
  }
}

function resolveRequestTimeoutMs(
  options?: { requestTimeout?: number; timeoutMs?: number },
): number {
  if (options?.requestTimeout != null) {
    return secondsToMillis(options.requestTimeout);
  }
  if (options?.timeoutMs != null) {
    validateTimeoutMs(options.timeoutMs);
    return options.timeoutMs;
  }
  return defaults.DEFAULT_HTTP_TIMEOUT_MS;
}

function secondsToMillis(seconds: number): number {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    throw new SandboxError("requestTimeout must be a positive number of seconds");
  }
  return Math.ceil(seconds * 1000);
}

function validateTimeoutMs(timeoutMs: number): void {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new SandboxError("timeoutMs must be a positive number of milliseconds");
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function snapshotStatusSatisfiesWaitCondition(
  status: SnapshotStatus | string,
  waitUntil: SnapshotWaitCondition,
): boolean {
  if (waitUntil === "local_ready") {
    return status === SnapshotStatus.LOCAL_READY || status === SnapshotStatus.COMPLETED;
  }
  return status === SnapshotStatus.COMPLETED;
}

function formatStartupFailureMessage(
  sandboxId: string,
  status: SandboxStatus | string,
  options: {
    errorDetails?: unknown;
    terminationReason?: string;
  },
): string {
  const prefix = status === SandboxStatus.TERMINATED
    ? `Sandbox ${sandboxId} terminated during startup`
    : `Sandbox ${sandboxId} became ${status} during startup`;
  const detail = formatErrorDetails(options.errorDetails);
  if (detail) {
    return `${prefix}: ${detail}`;
  }
  if (options.terminationReason) {
    return `${prefix}: termination reason: ${options.terminationReason}`;
  }
  return prefix;
}

function formatErrorDetails(errorDetails: unknown): string | undefined {
  if (errorDetails == null) return undefined;
  if (typeof errorDetails === "string") {
    const detail = errorDetails.trim();
    return detail || undefined;
  }
  if (Array.isArray(errorDetails)) {
    const parts = errorDetails
      .map((item) => formatErrorDetails(item))
      .filter((item): item is string => Boolean(item));
    return parts.length > 0 ? parts.join("; ") : JSON.stringify(errorDetails);
  }
  if (typeof errorDetails === "object") {
    for (const key of ["message", "detail", "error", "reason"]) {
      const value = (errorDetails as Record<string, unknown>)[key];
      if (typeof value === "string" && value.trim()) {
        return value.trim();
      }
    }
    return JSON.stringify(errorDetails);
  }
  return String(errorDetails);
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
