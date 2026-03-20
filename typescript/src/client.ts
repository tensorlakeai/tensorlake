import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { HttpClient } from "./http.js";
import {
  type CreateAndConnectOptions,
  type CreatePoolOptions,
  type CreateSandboxOptions,
  type CreateSandboxPoolResponse,
  type CreateSandboxResponse,
  type CreateSnapshotResponse,
  type SandboxClientOptions,
  type SandboxInfo,
  type SandboxPoolInfo,
  SandboxStatus,
  type SnapshotAndWaitOptions,
  type SnapshotInfo,
  SnapshotStatus,
  type UpdatePoolOptions,
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

  constructor(options?: SandboxClientOptions) {
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

  async create(options?: CreateSandboxOptions): Promise<CreateSandboxResponse> {
    const body: Record<string, unknown> = {
      resources: {
        cpus: options?.cpus ?? 1.0,
        memory_mb: options?.memoryMb ?? 512,
        ephemeral_disk_mb: options?.ephemeralDiskMb ?? 1024,
      },
    };

    if (options?.image != null) body.image = options.image;
    if (options?.secretNames != null) body.secret_names = options.secretNames;
    if (options?.timeoutSecs != null) body.timeout_secs = options.timeoutSecs;
    if (options?.entrypoint != null) body.entrypoint = options.entrypoint;
    if (options?.snapshotId != null) body.snapshot_id = options.snapshotId;

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
    return fromSnakeKeys(raw, "sandboxId") as CreateSandboxResponse;
  }

  async get(sandboxId: string): Promise<SandboxInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`sandboxes/${sandboxId}`),
    );
    return fromSnakeKeys(raw, "sandboxId") as SandboxInfo;
  }

  async list(): Promise<SandboxInfo[]> {
    const raw = await this.http.requestJson<{ sandboxes: Record<string, unknown>[] }>(
      "GET",
      this.path("sandboxes"),
    );
    return (raw.sandboxes ?? []).map(
      (s) => fromSnakeKeys(s, "sandboxId") as SandboxInfo,
    );
  }

  async delete(sandboxId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`sandboxes/${sandboxId}`),
    );
  }

  async claim(poolId: string): Promise<CreateSandboxResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path(`sandbox-pools/${poolId}/sandboxes`),
    );
    return fromSnakeKeys(raw, "sandboxId") as CreateSandboxResponse;
  }

  // --- Snapshots ---

  async snapshot(sandboxId: string): Promise<CreateSnapshotResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      this.path(`sandboxes/${sandboxId}/snapshot`),
    );
    return fromSnakeKeys(raw, "snapshotId") as CreateSnapshotResponse;
  }

  async getSnapshot(snapshotId: string): Promise<SnapshotInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`snapshots/${snapshotId}`),
    );
    return fromSnakeKeys(raw, "snapshotId") as SnapshotInfo;
  }

  async listSnapshots(): Promise<SnapshotInfo[]> {
    const raw = await this.http.requestJson<{ snapshots: Record<string, unknown>[] }>(
      "GET",
      this.path("snapshots"),
    );
    return (raw.snapshots ?? []).map(
      (s) => fromSnakeKeys(s, "snapshotId") as SnapshotInfo,
    );
  }

  async deleteSnapshot(snapshotId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`snapshots/${snapshotId}`),
    );
  }

  async snapshotAndWait(
    sandboxId: string,
    options?: SnapshotAndWaitOptions,
  ): Promise<SnapshotInfo> {
    const timeout = options?.timeout ?? 300;
    const pollInterval = options?.pollInterval ?? 1;

    const result = await this.snapshot(sandboxId);
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

  async createPool(options: CreatePoolOptions): Promise<CreateSandboxPoolResponse> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 512,
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

  async getPool(poolId: string): Promise<SandboxPoolInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.path(`sandbox-pools/${poolId}`),
    );
    return fromSnakeKeys(raw, "poolId") as SandboxPoolInfo;
  }

  async listPools(): Promise<SandboxPoolInfo[]> {
    const raw = await this.http.requestJson<{ pools: Record<string, unknown>[] }>(
      "GET",
      this.path("sandbox-pools"),
    );
    return (raw.pools ?? []).map(
      (p) => fromSnakeKeys(p, "poolId") as SandboxPoolInfo,
    );
  }

  async updatePool(
    poolId: string,
    options: UpdatePoolOptions,
  ): Promise<SandboxPoolInfo> {
    const body: Record<string, unknown> = {
      image: options.image,
      resources: {
        cpus: options.cpus ?? 1.0,
        memory_mb: options.memoryMb ?? 512,
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

  async deletePool(poolId: string): Promise<void> {
    await this.http.requestJson(
      "DELETE",
      this.path(`sandbox-pools/${poolId}`),
    );
  }

  // --- Connect ---

  connect(sandboxId: string, proxyUrl?: string): Sandbox {
    const resolvedProxy = proxyUrl ?? resolveProxyUrl(this.apiUrl);
    return new Sandbox({
      sandboxId,
      proxyUrl: resolvedProxy,
      apiKey: this.apiKey,
      organizationId: this.organizationId,
      projectId: this.projectId,
    });
  }

  async createAndConnect(
    options?: CreateAndConnectOptions,
  ): Promise<Sandbox> {
    const startupTimeout = options?.startupTimeout ?? 60;

    let result: CreateSandboxResponse;
    if (options?.poolId != null) {
      result = await this.claim(options.poolId);
    } else {
      result = await this.create(options);
    }

    const deadline = Date.now() + startupTimeout * 1000;

    while (Date.now() < deadline) {
      const info = await this.get(result.sandboxId);
      if (info.status === SandboxStatus.RUNNING) {
        const sandbox = this.connect(result.sandboxId, options?.proxyUrl);
        sandbox._setOwner(this);
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
