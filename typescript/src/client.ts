import * as defaults from "./defaults.js";
import { SandboxError, SandboxPending, formatErrorDetails } from "./errors.js";
import type { Traced } from "./traced.js";
import { releaseNativeHandle } from "./native-worker-client.js";
import {
  callNative,
  loadNativeSandboxBinding,
  type NativeErrorContext,
  type NativeSandboxClient,
} from "./native-sandbox.js";
import {
  type ArchivedSandboxInfo,
  type AttachFileSystemOptions,
  type CopySandboxOptions,
  type CopySandboxResponse,
  type ClaimSandboxOptions,
  type CreateAndConnectOptions,
  type CreatePoolOptions,
  type CreateSandboxOptions,
  type CreateSandboxPoolResponse,
  type CreateSandboxResponse,
  type CreateSnapshotResponse,
  type FileSystemMount,
  type GetSandboxLogsOptions,
  type GpuRequest,
  type ListArchivedSandboxesOptions,
  type ListArchivedSandboxesResponse,
  type SandboxClientOptions,
  type SandboxInfo,
  type SandboxLogsResponse,
  type PendingSandboxRecord,
  type ReadyOptions,
  type SandboxPortAccess,
  type SandboxPoolInfo,
  type SandboxProcessLogFiltersResponse,
  SandboxStatus,
  isSandboxPending,
  type SnapshotAndWaitOptions,
  type SnapshotInfo,
  type SnapshotOptions,
  SnapshotStatus,
  type SnapshotWaitCondition,
  type SuspendResumeOptions,
  type UpdatePoolOptions,
  type UpdateSandboxOptions,
  type ResourceResizeWaitOptions,
  fromSnakeKeys,
  toSnakeKeys,
} from "./models.js";
import { PendingSandbox, type PendingSandboxBinding } from "./pending-sandbox.js";
import { Sandbox } from "./sandbox.js";
import { nowMs, logSdkTimingEvent, logSdkTiming } from "./sdk-timings.js";
import { explicitProxyUrlOverride } from "./url.js";

/** Seconds between `GET /sandboxes/{id}` polls while waiting for a sandbox to run (ADR 0086). */
export const DEFAULT_WAIT_POLL_INTERVAL_SEC = 2;

const GPU_MODELS = new Set<string>([
  "A100-40GB",
  "A100-80GB",
  "H100",
  "T4",
  "A6000",
  "RTX-PRO-6000",
  "L40",
  "A10",
]);

/**
 * Client-side mirror of the server's HTTP 400 for invalid snapshot pins: a
 * mount that pins `snapshotId` must also be `readOnly`, so callers fail fast
 * (and offline) instead of round-tripping to the server.
 */
function requireReadOnlySnapshotPin(
  fileSystemId: string,
  mountPath: string,
  readOnly: boolean | undefined,
  snapshotId: string | undefined,
): void {
  if (snapshotId != null && readOnly !== true) {
    throw new SandboxError(
      `file system mount '${fileSystemId}' at '${mountPath}' sets snapshotId ` +
        "without readOnly: snapshot-pinned mounts are read-only",
    );
  }
}

/**
 * Map a `FileSystemMount` to its wire form. `read_only` and `prefetch` are
 * included only when `true`, and `snapshot_id` only when set: older servers
 * deserialize mount bodies with `deny_unknown_fields` and would reject an
 * explicit `false` (or an unknown pin field).
 */
function fileSystemMountToWire(fs: FileSystemMount): Record<string, unknown> {
  requireReadOnlySnapshotPin(fs.fileSystemId, fs.mountPath, fs.readOnly, fs.snapshotId);
  return {
    file_system_id: fs.fileSystemId,
    mount_path: fs.mountPath,
    ...(fs.readOnly === true ? { read_only: true } : {}),
    ...(fs.prefetch === true ? { prefetch: true } : {}),
    ...(fs.snapshotId != null ? { snapshot_id: fs.snapshotId } : {}),
    ...(fs.owner != null ? { owner: fs.owner } : {}),
  };
}

function gpuRequest(
  gpu: GpuRequest | undefined,
  gpus: number | undefined,
  gpuModel: string | undefined,
): Array<{ count: number; model: string }> | undefined {
  if (gpu != null) {
    if (gpus != null || gpuModel != null) {
      throw new SandboxError("gpu cannot be combined with gpus or gpuModel");
    }
    gpus = gpu.count;
    gpuModel = gpu.model;
  }
  if (gpus == null) return undefined;
  if (!Number.isInteger(gpus) || gpus < 1) {
    throw new SandboxError("gpus must be a positive integer");
  }
  gpuModel = gpuModel ?? "A10";
  if (!GPU_MODELS.has(gpuModel)) {
    throw new SandboxError(`unsupported GPU model: ${gpuModel}`);
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

  /**
   * Create an API-key client for TensorLake Cloud. Ingress selects the authorized
   * organization/project; explicit scope options are forwarded only as compatibility headers.
   */
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
    releaseNativeHandle(this.native);
  }

  private withRequestTimeout(
    requestTimeout: number | undefined,
  ): SandboxClient {
    if (requestTimeout == null) {
      return this;
    }
    const timeoutMs = secondsToMillis(requestTimeout);
    if (timeoutMs === this.requestTimeoutMs) {
      return this;
    }
    return new SandboxClient(
      {
        apiUrl: this.apiUrl,
        apiKey: this.apiKey,
        organizationId: this.organizationId,
        projectId: this.projectId,
        namespace: this.namespace,
        timeoutMs,
      },
      /* _internal */ true,
    );
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

  /**
   * Create a new sandbox.
   *
   * With `wait` unset or `true` the server waits for readiness for at most
   * the request timeout and answers the blocking create's record (`status`
   * may already be `running`); this is the wire behaviour unchanged. With
   * `wait: false` the server answers as soon as the sandbox is durable and
   * this returns a {@link PendingSandbox} at once: `pending.ready()` waits
   * for it, `pending.status()` looks at it, and `connect(sandboxId)`
   * collects it from any other process. Give the sandbox a `name` when a
   * retry of this call must not create a second one: a repeated name is an
   * HTTP 409, and `Sandbox.getOrCreate` resolves it. Use
   * `createAndConnect()` for a blocking, ready-to-use handle.
   */
  async create<O extends CreateSandboxOptions = CreateSandboxOptions>(
    options?: O,
  ): Promise<
    O extends { wait: false } ? PendingSandbox : Traced<CreateSandboxResponse>
  >;
  async create(
    options?: CreateSandboxOptions,
  ): Promise<PendingSandbox | Traced<CreateSandboxResponse>> {
    if (options?.wait === false) {
      return this.requestPending(options, { ownsSandbox: false });
    }
    const body = SandboxClient.buildCreateRequestBody(options);
    return this.tracedJson<CreateSandboxResponse>(
      () => this.native.createSandbox(JSON.stringify(body)),
      "sandboxId",
    );
  }

  /**
   * @internal Send the create with `wait: false` and return the pending
   * handle, bound to this client so `ready()` / `status()` work.
   */
  async requestPending(
    options: CreateSandboxOptions | undefined,
    binding: {
      proxyUrl?: string;
      requestTimeout?: number;
      ownsSandbox: boolean;
    },
  ): Promise<PendingSandbox> {
    const body = SandboxClient.buildCreateRequestBody({
      ...options,
      wait: false,
    });
    const record = await this.tracedJson<PendingSandboxRecord>(() =>
      this.native.createSandboxNoWait(JSON.stringify(body)),
    );
    return new PendingSandbox(record, {
      client: this,
      proxyUrl: binding.proxyUrl,
      requestTimeout: binding.requestTimeout,
      requestedName: options?.name ?? null,
      ownsSandbox: binding.ownsSandbox,
      traceId: record.traceId,
    });
  }

  /** @internal Implementation of `PendingSandbox.ready`. */
  async _ready(
    sandboxId: string,
    binding: PendingSandboxBinding,
    options?: ReadyOptions,
  ): Promise<Sandbox> {
    const budget = options?.timeout ?? this.requestTimeoutMs / 1000;
    const observed = await this.waitForSandbox(
      sandboxId,
      budget,
      options?.pollInterval ?? DEFAULT_WAIT_POLL_INTERVAL_SEC,
    );
    return this.settleWait(observed, {
      budget,
      proxyUrl: binding.proxyUrl,
      requestTimeout: binding.requestTimeout,
      cancelOnTimeout: options?.cancelOnTimeout ?? false,
      requestedName: binding.requestedName,
      ownsSandbox: binding.ownsSandbox,
      traceId: binding.traceId,
    });
  }

  /**
   * Poll `get` every `pollInterval` seconds until the sandbox leaves
   * `pending` or `timeout` elapses, resolving with the last observation.
   * Never rejects for a timeout and never deletes.
   */
  private async waitForSandbox(
    sandboxId: string,
    timeout: number,
    pollInterval: number,
  ): Promise<Traced<SandboxInfo>> {
    if (!Number.isFinite(timeout) || timeout < 0) {
      throw new SandboxError("timeout must be a non-negative number of seconds");
    }
    if (!Number.isFinite(pollInterval) || pollInterval <= 0) {
      throw new SandboxError("pollInterval must be a positive number of seconds");
    }
    return this.tracedJson<SandboxInfo>(
      () => this.native.waitForSandbox(sandboxId, timeout, pollInterval),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Turn the last observed sandbox state into a handle or an error. */
  private async settleWait(
    observed: Traced<SandboxInfo>,
    options: {
      budget: number;
      proxyUrl?: string;
      requestTimeout?: number;
      cancelOnTimeout: boolean;
      requestedName?: string | null;
      ownsSandbox?: boolean;
      traceId?: string;
    },
  ): Promise<Sandbox> {
    const { status, sandboxId } = observed;
    if (isSandboxPending(status)) {
      if (options.cancelOnTimeout) {
        try {
          await this.delete(sandboxId);
        } catch {
          // ignore cleanup failures
        }
        throw new SandboxError(
          `Sandbox ${sandboxId} did not start within ${options.budget}s`,
          { reason: observed.pendingReason, sandboxId },
        );
      }
      throw new SandboxPending(sandboxId, {
        pendingReason: observed.pendingReason,
        timeout: options.budget,
      });
    }
    if (status === SandboxStatus.RUNNING) {
      const explicitProxyUrl =
        options.proxyUrl ?? explicitProxyUrlOverride() ?? undefined;
      const selectedProxyUrl = await this.native.selectSandboxProxyUrl(
        sandboxId,
        observed.sandboxUrl ?? null,
        observed.ingressEndpoint ?? null,
        explicitProxyUrl ?? null,
      );
      const sandbox = this.connect(
        sandboxId,
        selectedProxyUrl,
        observed.routingHint,
        options.requestTimeout,
        explicitProxyUrl,
      );
      if (options.ownsSandbox) sandbox._setOwner(this);
      else sandbox._setLifecycleClient(this);
      sandbox.traceId = options.traceId ?? observed.traceId;
      sandbox._setLifecycleIdentifier(sandboxId);
      sandbox._setName(observed.name ?? options.requestedName ?? null);
      return sandbox;
    }
    if (status === SandboxStatus.TERMINATED || status === SandboxStatus.FAILED) {
      throw startupFailure(sandboxId, status, {
        errorDetails: observed.errorDetails,
        terminationReason: observed.terminationReason,
      });
    }
    throw new SandboxError(
      `Sandbox ${sandboxId} is ${status}, not running; ` +
        (status === SandboxStatus.SUSPENDED
          ? "resume it to run it again"
          : "wait for it to settle"),
      { reason: String(status), sandboxId },
    );
  }

  /**
   * Routing for a lazy `connect` handle. A sandbox that is still pending is
   * waited on here rather than failing: readiness is a property of the
   * sandbox, not of the create call that requested it.
   */
  private async resolveRoutingInfo(
    identifier: string,
    requestTimeout?: number,
  ): Promise<Traced<SandboxInfo>> {
    const info = await this.get(identifier);
    if (info.status !== SandboxStatus.PENDING) return info;
    const budget = requestTimeout ?? this.requestTimeoutMs / 1000;
    const observed = await this.waitForSandbox(
      info.sandboxId,
      budget,
      DEFAULT_WAIT_POLL_INTERVAL_SEC,
    );
    if (isSandboxPending(observed.status)) {
      throw new SandboxPending(observed.sandboxId, {
        pendingReason: observed.pendingReason,
        timeout: budget,
      });
    }
    if (
      observed.status === SandboxStatus.TERMINATED ||
      observed.status === SandboxStatus.FAILED
    ) {
      throw startupFailure(observed.sandboxId, observed.status, {
        errorDetails: observed.errorDetails,
        terminationReason: observed.terminationReason,
      });
    }
    return observed;
  }

  /** @internal Build the create wire body; shared by `create` and `createAndConnect`. */
  static buildCreateRequestBody(
    options?: CreateSandboxOptions,
  ): Record<string, unknown> {
    const gpuResources = gpuRequest(
      options?.gpu,
      options?.gpus,
      options?.gpuModel,
    );
    const restoringSnapshot = options?.snapshotId != null;
    const resources: Record<string, unknown> = {
      ...(!restoringSnapshot || options?.cpus != null
        ? { cpus: options?.cpus ?? 1.0 }
        : {}),
      ...(!restoringSnapshot || options?.memoryMb != null
        ? { memory_mb: options?.memoryMb ?? 1024 }
        : {}),
      ...(options?.diskMb != null ? { disk_mb: options.diskMb } : {}),
      ...(gpuResources != null ? { gpus: gpuResources } : {}),
    };
    const body: Record<string, unknown> = {};
    if (Object.keys(resources).length > 0) body.resources = resources;

    if (options?.image != null) body.image = options.image;
    if (options?.timeoutSecs != null) body.timeout_secs = options.timeoutSecs;
    if (options?.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options?.snapshotId != null) body.snapshot_id = options.snapshotId;
    if (options?.name != null) body.name = options.name;
    if (options?.fileSystems != null && options.fileSystems.length > 0) {
      body.file_systems = options.fileSystems.map(fileSystemMountToWire);
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
    if (options?.maxPendingSecs != null) {
      if (
        !Number.isInteger(options.maxPendingSecs) ||
        options.maxPendingSecs < 0
      ) {
        throw new SandboxError("maxPendingSecs must be a non-negative integer");
      }
      body.max_pending_secs = options.maxPendingSecs;
    }
    // Sent only when false so older servers keep accepting the body.
    if (options?.wait === false) body.wait = false;
    return body;
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
    const { traceId, json } = await callNative(() =>
      this.native.listSandboxes(),
    );
    const parsed = JSON.parse(json) as {
      sandboxes?: Record<string, unknown>[];
    };
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

  /**
   * Update properties or resize a Running Cloud Hypervisor sandbox.
   * Resource names match create. Resize waits by default; wait=false returns
   * admission. Timeout leaves the resize running and reports its generation.
   * Wait, timeout, and pollInterval are ignored on non-resource updates.
   * A no-op returns current resources with absent or earlier resize metadata.
   */
  async update(
    sandboxId: string,
    options: UpdateSandboxOptions,
  ): Promise<Traced<SandboxInfo>> {
    const body: Record<string, unknown> = {};
    const resources: Record<string, number> = {};
    for (const [field, wire, value] of [
      ["cpus", "cpus", options.cpus],
      ["memoryMb", "memory_mb", options.memoryMb],
      ["diskMb", "disk_mb", options.diskMb],
    ] as const) {
      if (value === undefined) continue;
      if (typeof value !== "number" || !Number.isSafeInteger(value) || value <= 0) {
        throw new SandboxError(
          `${field} ${String(value)} must be a finite positive ${field === "cpus" ? "whole-vCPU count" : "integer number of MiB"}`,
        );
      }
      resources[wire] = value;
    }
    const resizing = Object.keys(resources).length > 0;
    if (resizing) {
      validateResizeWait(options);
      if (
        options.name != null ||
        options.allowUnauthenticatedAccess != null ||
        options.exposedPorts != null ||
        options.network !== undefined
      ) {
        throw new SandboxError(
          "resources cannot be combined with other sandbox update fields",
        );
      }
      body.resources = resources;
    }
    if (options.name != null) body.name = options.name;
    if (options.allowUnauthenticatedAccess != null) {
      body.allow_unauthenticated_access = options.allowUnauthenticatedAccess;
    }
    if (options.exposedPorts != null) {
      body.exposed_ports = normalizeUserPorts(options.exposedPorts);
    }
    // Tri-state network policy: omit to keep the current policy, `null` to
    // clear it to unrestricted egress, an object to replace it.
    if (options.network !== undefined) {
      body.network =
        options.network === null ? null : toSnakeKeys(options.network);
    }
    if (Object.keys(body).length === 0) {
      throw new SandboxError(
        "At least one sandbox update field must be provided.",
      );
    }
    return this.tracedJson<SandboxInfo>(
      () => resizing
        ? this.native.updateSandbox(
            sandboxId, JSON.stringify(body), options.wait ?? true,
            options.timeout ?? 300, options.pollInterval ?? 1,
          )
        : this.native.updateSandbox(sandboxId, JSON.stringify(body)),
      "sandboxId",
      { sandboxId, notFoundKind: "sandbox" },
    );
  }

  /** Wait for this exact generation. Timeout leaves the resize running. */
  async waitForResourceResize(
    sandboxId: string,
    generation: number,
    options: ResourceResizeWaitOptions = {},
  ): Promise<Traced<SandboxInfo>> {
    validateResizeWait(options);
    if (!Number.isSafeInteger(generation) || generation < 1) {
      throw new SandboxError("generation must be a positive safe integer");
    }
    return this.tracedJson<SandboxInfo>(
      () => this.native.waitForResourceResize(
        sandboxId, generation, options.timeout ?? 300, options.pollInterval ?? 1,
      ),
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
    const desiredPorts = current.exposedPorts.filter(
      (port) => !toRemove.has(port),
    );
    return this.update(sandboxId, {
      allowUnauthenticatedAccess: desiredPorts.length
        ? current.allowUnauthenticatedAccess
        : false,
      exposedPorts: desiredPorts,
    });
  }

  /** Terminate and delete a sandbox. */
  async delete(sandboxId: string): Promise<void> {
    await callNative(() => this.native.deleteSandbox(sandboxId), {
      sandboxId,
      notFoundKind: "sandbox",
    });
  }

  /**
   * Suspend a named sandbox, preserving its state for later resume.
   *
   * Only sandboxes created with a `name` can be suspended; ephemeral sandboxes
   * cannot. By default blocks until the sandbox is fully `Suspended`. Pass
   * `{ wait: false }` to return immediately after the request is sent
   * (fire-and-return); the server processes the suspend asynchronously.
   */
  async suspend(
    sandboxId: string,
    options?: SuspendResumeOptions,
  ): Promise<void> {
    await callNative(() => this.native.suspendSandbox(sandboxId), {
      sandboxId,
      notFoundKind: "sandbox",
    });
    if (options?.wait === false) return;
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const deadline = Date.now() + timeout * 1000;
    while (Date.now() < deadline) {
      const info = await this.get(sandboxId);
      if (info.status === SandboxStatus.SUSPENDED) return;
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(
          `Sandbox ${sandboxId} terminated while waiting for suspend`,
        );
      }
      await sleep(pollInterval * 1000);
    }
    throw new SandboxError(
      `Sandbox ${sandboxId} did not suspend within ${timeout}s`,
    );
  }

  /**
   * Resume a suspended sandbox and bring it back to `Running`.
   *
   * By default blocks until the sandbox is `Running` and routable. Pass
   * `{ wait: false }` to return immediately after the request is sent
   * (fire-and-return); the server processes the resume asynchronously.
   */
  async resume(
    sandboxId: string,
    options?: SuspendResumeOptions,
  ): Promise<void> {
    await callNative(() => this.native.resumeSandbox(sandboxId), {
      sandboxId,
      notFoundKind: "sandbox",
    });
    if (options?.wait === false) return;
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;
    const deadline = Date.now() + timeout * 1000;
    while (Date.now() < deadline) {
      const info = await this.get(sandboxId);
      if (info.status === SandboxStatus.RUNNING) return;
      if (info.status === SandboxStatus.TERMINATED) {
        throw new SandboxError(
          `Sandbox ${sandboxId} terminated while waiting for resume`,
        );
      }
      await sleep(pollInterval * 1000);
    }
    throw new SandboxError(
      `Sandbox ${sandboxId} did not resume within ${timeout}s`,
    );
  }

  /**
   * Attach a registered file system to a running sandbox at `mountPath`.
   *
   * The mount completes asynchronously on the dataplane; the returned
   * `SandboxInfo` already reflects the new entry in `fileSystems`.
   *
   * `options.snapshotId` pins the mount to a specific filesystem snapshot
   * and requires `options.readOnly: true`.
   */
  async attachFileSystem(
    sandboxId: string,
    fileSystemId: string,
    mountPath: string,
    options?: AttachFileSystemOptions,
  ): Promise<Traced<SandboxInfo>> {
    requireReadOnlySnapshotPin(
      fileSystemId,
      mountPath,
      options?.readOnly,
      options?.snapshotId,
    );
    return this.tracedJson<SandboxInfo>(
      () =>
        this.native.attachFileSystem(
          sandboxId,
          fileSystemId,
          mountPath,
          options?.readOnly === true,
          options?.prefetch === true,
          options?.snapshotId ?? null,
          options?.owner ?? null,
        ),
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

  /**
   * Claim a warm sandbox from a pool, creating one if no warm containers are
   * available. Claim-specific file systems are ready before the sandbox is
   * reported as running.
   */
  async claim(
    poolId: string,
    options?: ClaimSandboxOptions,
  ): Promise<Traced<CreateSandboxResponse>> {
    const requestJson =
      options?.fileSystems != null && options.fileSystems.length > 0
        ? JSON.stringify({
            file_systems: options.fileSystems.map(fileSystemMountToWire),
          })
        : undefined;
    return this.tracedJson<CreateSandboxResponse>(
      () =>
        requestJson == null
          ? this.native.claimSandbox(poolId)
          : this.native.claimSandbox(poolId, requestJson),
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
      () =>
        this.native.createSnapshot(sandboxId, options?.snapshotType ?? null),
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
    const { traceId, json } = await callNative(() =>
      this.native.listSnapshots(),
    );
    const parsed = JSON.parse(json) as {
      snapshots?: Record<string, unknown>[];
    };
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
      if (snapshotStatusSatisfiesWaitCondition(info.status, waitUntil))
        return info;
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
  async createPool(
    options: CreatePoolOptions,
  ): Promise<CreateSandboxPoolResponse> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 1024,
        ...(options.diskMb != null ? { disk_mb: options.diskMb } : {}),
      },
      timeout_secs: options.timeoutSecs ?? 0,
    };

    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null)
      body.max_containers = options.maxContainers;
    if (options.warmContainers != null)
      body.warm_containers = options.warmContainers;
    if (options.network != null) body.network = toSnakeKeys(options.network);

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

  /**
   * Replace a pool configuration. Omit `network` to keep the pool's current
   * network policy, set it to replace the policy, or pass `null` to remove the
   * policy entirely. On a change the service recycles the pool's unclaimed
   * warm containers onto the new policy, while containers already claimed by
   * sandboxes keep the policy they booted with. CPU, memory, disk, image, and
   * entrypoint changes likewise recycle unclaimed warm containers
   * asynchronously. If suitable capacity is unavailable, stale warm
   * containers are not used as a fallback.
   */
  async updatePool(
    poolId: string,
    options: UpdatePoolOptions,
  ): Promise<SandboxPoolInfo> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 1024,
        ...(options.diskMb != null ? { disk_mb: options.diskMb } : {}),
      },
      timeout_secs: options.timeoutSecs ?? 0,
    };

    if (options.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options.maxContainers != null)
      body.max_containers = options.maxContainers;
    if (options.warmContainers != null)
      body.warm_containers = options.warmContainers;
    // Tri-state: omit to keep the current policy, `null` to clear it, an
    // object to replace it.
    if (options.network !== undefined) {
      body.network =
        options.network === null ? null : toSnakeKeys(options.network);
    }

    return this.plainJson<SandboxPoolInfo>(
      () => this.native.updatePool(poolId, JSON.stringify(body)),
      "poolId",
      { poolId, notFoundKind: "pool" },
    );
  }

  /** Delete a sandbox pool. Fails if the pool has active containers. */
  async deletePool(poolId: string): Promise<void> {
    await callNative(() => this.native.deletePool(poolId), {
      poolId,
      notFoundKind: "pool",
    });
  }

  // --- Connect ---

  /** Return a `Sandbox` handle for an existing running sandbox without verifying it exists. */
  connect(
    identifier: string,
    proxyUrl?: string,
    routingHint?: string,
    requestTimeout?: number,
    explicitProxyUrl?: string,
  ): Sandbox {
    const explicitProxyUrlProvided = arguments.length >= 5;
    const resolvedExplicitProxyUrl = explicitProxyUrlProvided
      ? (explicitProxyUrl ?? undefined)
      : (proxyUrl ?? explicitProxyUrlOverride() ?? undefined);
    return new Sandbox({
      sandboxId: identifier,
      proxyUrl,
      explicitProxyUrl: resolvedExplicitProxyUrl,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
      routingHint,
      resolveProxyInfo: async (currentIdentifier) =>
        this.resolveRoutingInfo(currentIdentifier, requestTimeout),
      requestTimeout,
      nativeClient: this.native,
    });
  }

  /**
   * Create a sandbox, wait for it to reach `Running`, and return a connected handle.
   *
   * This is `create({ wait: false })` followed by `PendingSandbox.ready()`
   * polling every `pollInterval` seconds, with `requestTimeout` as the wait
   * budget, in one call. The returned `Sandbox` auto-terminates when
   * `terminate()` is called.
   *
   * **When the wait runs out the sandbox is not deleted.** A `SandboxPending`
   * is thrown with the sandbox id, and the sandbox keeps its place in the
   * queue and starts whenever capacity arrives; `connect` with that id to
   * collect it, or `delete` it to give up. Pass
   * `cancelOnTimeout: true` for the previous behaviour (delete, then throw
   * `SandboxError`), or set `maxPendingSecs` to let the server fail it after
   * a bound. A pool claim (`poolId`) answers from the server-side wait; a
   * claim that is still starting is then waited on the same way.
   */
  async createAndConnect(options?: CreateAndConnectOptions): Promise<Sandbox> {
    const opStart = nowMs();
    const requestTimeout =
      options?.requestTimeout ??
      options?.startupTimeout ??
      this.requestTimeoutMs / 1000;
    const requestClient = this.withRequestTimeout(requestTimeout);
    const deadline = Date.now() + secondsToMillis(requestTimeout);
    logSdkTimingEvent("sandbox.create", "start", {
      request_timeout_s: requestTimeout,
      image: options?.image,
      pool_id: options?.poolId,
    });

    // claim() never sends options.name to the server, so only create() should fall
    // back to it locally when the server response omits a name.
    const requestedName =
      options?.poolId != null ? null : (options?.name ?? null);
    let sandboxId: string;
    let traceId: string;
    const createStart = nowMs();
    if (options?.poolId != null) {
      // Pool claims have no `wait: false`: the server answers from its own
      // wait, so a claim is often already running.
      const result = await requestClient.claim(options.poolId, {
        fileSystems: options.fileSystems,
      });
      logSdkTiming("sandbox.create", "claim_response", createStart, {
        sandbox_id: result.sandboxId,
        status: result.status,
        server_trace_id: result.traceId,
      });
      if (result.status === SandboxStatus.RUNNING) {
        const explicitProxyUrl =
          options?.proxyUrl ?? explicitProxyUrlOverride() ?? undefined;
        const selectedProxyUrl = await this.native.selectSandboxProxyUrl(
          result.sandboxId,
          result.sandboxUrl ?? null,
          result.ingressEndpoint ?? null,
          explicitProxyUrl ?? null,
        );
        const sandbox = requestClient.connect(
          result.sandboxId,
          selectedProxyUrl,
          result.routingHint,
          requestTimeout,
          explicitProxyUrl,
        );
        sandbox._setOwner(requestClient);
        sandbox.traceId = result.traceId;
        sandbox._setLifecycleIdentifier(result.sandboxId);
        sandbox._setName(result.name ?? requestedName);
        logSdkTiming("sandbox.create", "complete", opStart, {
          sandbox_id: result.sandboxId,
          status: SandboxStatus.RUNNING,
          server_trace_id: result.traceId,
        });
        return sandbox;
      }
      if (
        result.status === SandboxStatus.SUSPENDED ||
        result.status === SandboxStatus.TERMINATED ||
        result.status === SandboxStatus.FAILED
      ) {
        throw startupFailure(result.sandboxId, result.status, {
          errorDetails: result.errorDetails,
          terminationReason: result.terminationReason ?? result.reason,
        });
      }
      sandboxId = result.sandboxId;
      traceId = result.traceId;
    } else {
      const pending = await requestClient.requestPending(options, {
        ownsSandbox: true,
      });
      logSdkTiming("sandbox.create", "create_response", createStart, {
        sandbox_id: pending.sandboxId,
        status: pending.state,
        server_trace_id: pending.traceId,
      });
      sandboxId = pending.sandboxId;
      traceId = pending.traceId;
      // Fast path: a server without `wait: false` may already answer running,
      // with a short-lived routing hint worth using directly.
      if (pending.state === SandboxStatus.RUNNING) {
        const explicitProxyUrl =
          options?.proxyUrl ?? explicitProxyUrlOverride() ?? undefined;
        const selectedProxyUrl = await this.native.selectSandboxProxyUrl(
          sandboxId,
          pending.sandboxUrl ?? null,
          pending.ingressEndpoint ?? null,
          explicitProxyUrl ?? null,
        );
        const sandbox = requestClient.connect(
          sandboxId,
          selectedProxyUrl,
          pending.routingHint,
          requestTimeout,
          explicitProxyUrl,
        );
        sandbox._setOwner(requestClient);
        sandbox.traceId = traceId;
        sandbox._setLifecycleIdentifier(sandboxId);
        sandbox._setName(pending.name ?? requestedName);
        logSdkTiming("sandbox.create", "complete", opStart, {
          sandbox_id: sandboxId,
          status: SandboxStatus.RUNNING,
          server_trace_id: traceId,
        });
        return sandbox;
      }
      if (
        pending.state === SandboxStatus.TERMINATED ||
        pending.state === SandboxStatus.FAILED
      ) {
        throw startupFailure(sandboxId, pending.state, {
          errorDetails: pending.errorDetails,
          terminationReason: pending.terminationReason ?? pending.reason,
        });
      }
    }

    // A `timeout` claim or a pending create: the sandbox exists and keeps its
    // place in the queue. Poll it with whatever budget is left (a zero budget
    // observes its state once without sleeping).
    const remaining = Math.max(0, (deadline - Date.now()) / 1000);
    const observed = await requestClient.waitForSandbox(
      sandboxId,
      remaining,
      options?.pollInterval ?? DEFAULT_WAIT_POLL_INTERVAL_SEC,
    );
    const sandbox = await requestClient.settleWait(observed, {
      budget: requestTimeout,
      proxyUrl: options?.proxyUrl,
      requestTimeout,
      cancelOnTimeout: options?.cancelOnTimeout ?? false,
      requestedName,
      ownsSandbox: true,
      traceId,
    });
    logSdkTiming("sandbox.create", "complete", opStart, {
      sandbox_id: sandboxId,
      status: SandboxStatus.RUNNING,
      server_trace_id: traceId,
      routing_hint: observed.routingHint,
      ingress_endpoint: observed.ingressEndpoint,
      sandbox_url: observed.sandboxUrl,
    });
    return sandbox;
  }
}

function startupFailure(
  sandboxId: string,
  status: SandboxStatus | string,
  options: { errorDetails?: unknown; terminationReason?: string },
): SandboxError {
  return new SandboxError(formatStartupFailureMessage(sandboxId, status, options), {
    reason: options.terminationReason,
    sandboxId,
  });
}

function resolveRequestTimeoutMs(options?: {
  requestTimeout?: number;
  timeoutMs?: number;
}): number {
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
    throw new SandboxError(
      "requestTimeout must be a positive number of seconds",
    );
  }
  return Math.ceil(seconds * 1000);
}

function validateTimeoutMs(timeoutMs: number): void {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new SandboxError(
      "timeoutMs must be a positive number of milliseconds",
    );
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
    return (
      status === SnapshotStatus.LOCAL_READY ||
      status === SnapshotStatus.COMPLETED
    );
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
  let prefix =
    status === SandboxStatus.TERMINATED
      ? `Sandbox ${sandboxId} terminated during startup`
      : `Sandbox ${sandboxId} became ${status} during startup`;
  if (options.terminationReason) {
    prefix += ` (${options.terminationReason})`;
  }
  const detail = formatErrorDetails(options.errorDetails);
  if (detail) {
    return `${prefix}: ${detail}`;
  }
  return prefix;
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

function validateResizeWait(options: ResourceResizeWaitOptions): void {
  const timeout = options.timeout ?? 300;
  const poll = options.pollInterval ?? 1;
  if (typeof timeout !== "number" || !Number.isFinite(timeout) || timeout < 0) {
    throw new SandboxError("timeout must be finite and non-negative");
  }
  if (typeof poll !== "number" || !Number.isFinite(poll) || poll <= 0) {
    throw new SandboxError("pollInterval must be finite and positive");
  }
}
