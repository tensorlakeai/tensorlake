// --- Enums ---

export enum SandboxStatus {
  PENDING = "pending",
  RUNNING = "running",
  SNAPSHOTTING = "snapshotting",
  SUSPENDING = "suspending",
  SUSPENDED = "suspended",
  TERMINATED = "terminated",
  TIMEOUT = "timeout",
}

export enum SnapshotStatus {
  IN_PROGRESS = "in_progress",
  LOCAL_READY = "local_ready",
  COMPLETED = "completed",
  FAILED = "failed",
}

/**
 * Snapshot type for sandbox snapshot creation.
 *
 * - `"memory"`: Capture VM memory + filesystem state. Sandboxes restored
 *   from this snapshot warm-restore VM memory.
 * - `"filesystem"`: Capture filesystem state only. Sandboxes restored from
 *   this snapshot cold-boot from the snapshot tarball instead of warm-
 *   restoring VM state. Use this for sandbox image builds so that the
 *   restored sandbox bypasses Firecracker's overlay-path constraints.
 */
export type SnapshotType = "memory" | "filesystem";

/**
 * Checkpoint type for {@link Sandbox.checkpoint}.
 *
 * - `"memory"`: Capture VM memory + filesystem state. Sandboxes restored
 *   from this checkpoint warm-restore VM memory and running processes.
 * - `"filesystem"`: Capture filesystem state only. Sandboxes restored from
 *   this checkpoint cold-boot from the snapshot tarball.
 */
export type CheckpointType = "memory" | "filesystem";

export type SnapshotWaitCondition = "local_ready" | "completed";

export enum ProcessStatus {
  RUNNING = "running",
  EXITED = "exited",
  SIGNALED = "signaled",
  OOM_KILLED = "oom_killed",
}

export enum StdinMode {
  CLOSED = "closed",
  PIPE = "pipe",
}

export enum OutputMode {
  CAPTURE = "capture",
  DISCARD = "discard",
}

export enum ContainerState {
  IDLE = "Idle",
  RUNNING = "Running",
}

// --- Resource / Network ---

export interface ContainerResourcesInfo {
  cpus: number;
  memoryMb: number;
  ephemeralDiskMb: number;
}

export interface GPUResources {
  count: number;
  model: string;
}

export interface NetworkConfig {
  allowInternetAccess: boolean;
  allowOut: string[];
  denyOut: string[];
}

/**
 * One file system mounted into a sandbox at an absolute guest path.
 *
 * `fileSystemId` is the registered file system's id (e.g.
 * `file_system_...`) and `mountPath` is an absolute, unique guest path
 * (e.g. `/mnt/skills`).
 */
export interface FileSystemMount {
  fileSystemId: string;
  mountPath: string;
}

// --- Sandbox lifecycle ---

export interface CreateSandboxOptions {
  /** Optional sandbox image name, such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image. When omitted, Tensorlake uses the default managed environment. */
  image?: string;
  cpus?: number;
  memoryMb?: number;
  /** Root disk size in megabytes. When omitted, the server uses its default disk size. */
  diskMb?: number;
  /** Number of GPUs to allocate. When provided, defaults to A10 unless gpuModel is set. */
  gpus?: number;
  /** GPU model to allocate. Only "A10" is supported. */
  gpuModel?: string;
  timeoutSecs?: number;
  entrypoint?: string[];
  allowInternetAccess?: boolean;
  allowOut?: string[];
  denyOut?: string[];
  snapshotId?: string;
  /** Optional name for the sandbox. Named sandboxes support suspend/resume. When absent the sandbox is ephemeral. */
  name?: string;
  /** File systems to mount into the sandbox at boot, each at its own absolute, unique guest mount path. */
  fileSystems?: FileSystemMount[];
}

export interface UpdateSandboxOptions {
  /** New name for the sandbox. Naming an ephemeral sandbox enables suspend/resume. */
  name?: string;
  /** Whether exposed user ports should be reachable without TensorLake auth. */
  allowUnauthenticatedAccess?: boolean;
  /** User ports that should be routable through the sandbox proxy. Port 9501 is reserved. */
  exposedPorts?: number[];
}

export interface CreateSandboxResponse {
  sandboxId: string;
  status: SandboxStatus;
  reason?: string;
  routingHint?: string;
  ingressEndpoint?: string;
  name?: string | null;
  terminationReason?: string;
  errorDetails?: unknown;
}

export interface CopySandboxOptions {
  /** Number of running copies to create. Defaults to 1. */
  times?: number;
  /** Per-request timeout in seconds for the blocking live-copy request. */
  requestTimeout?: number;
}

export interface CopiedSandboxResponse {
  sandboxId: string;
  /** Raw server status. Partial copy responses can include statuses such as `"failed"`. */
  status: string;
  reason?: string;
  routingHint?: string;
  ingressEndpoint?: string;
  name?: string | null;
  terminationReason?: string;
  errorDetails?: unknown;
}

export interface CopySandboxResponse {
  sourceSandboxId: string;
  sandboxes: CopiedSandboxResponse[];
}

export interface SandboxInfo {
  sandboxId: string;
  namespace: string;
  status: SandboxStatus;
  /** Resolved sandbox image name. */
  image?: string;
  resources: ContainerResourcesInfo;
  timeoutSecs?: number;
  entrypoint?: string[];
  network?: NetworkConfig;
  poolId?: string;
  outcome?: string;
  terminationReason?: string;
  errorDetails?: unknown;
  createdAt?: Date;
  terminatedAt?: Date;
  name?: string;
  allowUnauthenticatedAccess?: boolean;
  exposedPorts?: number[];
  ingressEndpoint?: string;
  sandboxUrl?: string;
  routingHint?: string;
  /** File systems currently mounted into the sandbox, each at its own guest mount path. Empty when none are mounted. */
  fileSystems?: FileSystemMount[];
}

export interface SandboxPortAccess {
  allowUnauthenticatedAccess: boolean;
  exposedPorts: number[];
  ingressEndpoint?: string;
  sandboxUrl?: string;
}

/** Sandbox information plus the archival timestamp. */
export interface ArchivedSandboxInfo extends SandboxInfo {
  archivedAt: Date;
}

/** Pagination params for `listArchivedSandboxes`. */
export interface ListArchivedSandboxesOptions {
  limit?: number;
  /** Base64-encoded pagination cursor returned by a prior call. */
  cursor?: string;
  /** Pagination direction. */
  direction?: "forward" | "backward";
}

export interface ListArchivedSandboxesResponse {
  sandboxes: ArchivedSandboxInfo[];
  prevCursor?: string;
  nextCursor?: string;
}

// --- Snapshots ---

export interface CreateSnapshotResponse {
  snapshotId: string;
  status: SnapshotStatus;
}

export interface SnapshotInfo {
  snapshotId: string;
  namespace: string;
  sandboxId: string;
  baseImage: string;
  status: SnapshotStatus;
  snapshotType?: SnapshotType;
  error?: string;
  snapshotUri?: string;
  snapshotFormatVersion?: string;
  sizeBytes?: number;
  rootfsDiskBytes?: number;
  createdAt?: Date;
}

export interface SnapshotOptions {
  /**
   * Optional snapshot type. When omitted the server picks its default. Use
   * `"filesystem"` for snapshots intended for sandbox image builds so that
   * restored sandboxes cold-boot.
   */
  snapshotType?: SnapshotType;
}

export interface SnapshotAndWaitOptions extends SnapshotOptions {
  timeout?: number;
  pollInterval?: number;
  /** Defaults to `"local_ready"`, which is enough to resume from a snapshot. */
  waitUntil?: SnapshotWaitCondition;
}

// --- Persisted sandbox logs ---

export type SandboxLogLevel =
  | "trace"
  | "debug"
  | "info"
  | "warn"
  | "error"
  | "fatal";

export interface GetSandboxLogsOptions {
  levels?: SandboxLogLevel[];
  processIds?: string[];
  nextToken?: string;
  head?: number;
  tail?: number;
  body?: string;
}

export interface SandboxLogSignal {
  timestamp: number;
  uuid: string;
  namespace: string;
  application: string;
  sandboxId?: string;
  resourceAttributes: Array<[string, string]>;
  body: string;
  logAttributes: string;
  allocations?: string[];
  functionRuns?: string[];
  level?: number;
  retention?: number;
}

export interface SandboxLogsResponse {
  logs: SandboxLogSignal[];
  nextToken?: string;
}

export interface SandboxProcessLogFilter {
  processId: string;
  processPid: string;
  processCommand: string;
  processManagedId: string;
  processManagedName: string;
  firstSeen: number;
  lastSeen: number;
  logCount: number;
}

export interface SandboxProcessLogFiltersResponse {
  processes: SandboxProcessLogFilter[];
}

// --- Pools ---

export interface CreatePoolOptions {
  /** Sandbox image name, such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image. */
  image: string;
  cpus?: number;
  memoryMb?: number;
  ephemeralDiskMb?: number;
  timeoutSecs?: number;
  entrypoint?: string[];
  maxContainers?: number;
  warmContainers?: number;
}

export interface UpdatePoolOptions {
  /** Sandbox image name, such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image. */
  image: string;
  cpus?: number;
  memoryMb?: number;
  ephemeralDiskMb?: number;
  timeoutSecs?: number;
  entrypoint?: string[];
  maxContainers?: number;
  warmContainers?: number;
}

export interface CreateSandboxPoolResponse {
  poolId: string;
  namespace: string;
}

export interface PoolContainerInfo {
  id: string;
  state: string;
  sandboxId?: string;
  executorId: string;
}

export interface SandboxPoolInfo {
  poolId: string;
  namespace: string;
  /** Sandbox image name backing the pool. */
  image: string;
  resources: ContainerResourcesInfo;
  timeoutSecs: number;
  entrypoint?: string[];
  maxContainers?: number;
  warmContainers?: number;
  containers?: PoolContainerInfo[];
  createdAt?: Date;
  updatedAt?: Date;
}

// --- Process management ---

export interface ProcessUserSpec {
  name?: string;
  uid?: number;
  gid?: number;
}

export type ProcessUser = string | ProcessUserSpec;

export type RestartPolicy = "never" | "on_failure" | "always";

export interface RestartPolicyConfig {
  policy?: RestartPolicy;
  maxRestarts?: number;
  initialBackoffMs?: number;
  maxBackoffMs?: number;
}

export type ProcessHealthCheckType = "http" | "tcp";

export interface ProcessHealthCheck {
  type: ProcessHealthCheckType;
  port: number;
  path?: string;
  initialDelayMs?: number;
  intervalMs?: number;
  timeoutMs?: number;
  failureThreshold?: number;
}

export type ManagedProcessStatus =
  | "starting"
  | "running"
  | "backing_off"
  | "stopped";

export type ManagedProcessHealthStatus =
  | "disabled"
  | "starting"
  | "healthy"
  | "unhealthy";

export interface ManagedProcessExit {
  exitCode?: number;
  signal?: number;
  oomKilled: boolean;
  endedAt: Date;
}

/**
 * Managed-process metadata embedded into {@link ProcessInfo}.
 *
 * A managed process is addressable by either its current PID or, if a `name` was given at
 * creation, that name (process APIs accept a PID or process name). `id` is a stable
 * daemon-local identifier: it equals `name` when one was set, otherwise a daemon-assigned
 * opaque id (the process is then addressable only by PID, not by `id`).
 */
export interface ManagedProcessInfo {
  id: string;
  name?: string;
  status: ManagedProcessStatus;
  restartCount: number;
  restart: RestartPolicyConfig;
  healthCheck?: ProcessHealthCheck;
  healthStatus: ManagedProcessHealthStatus;
  consecutiveHealthFailures: number;
  lastExit?: ManagedProcessExit;
  lastError?: string;
  nextRestartAt?: Date;
}

export interface StartProcessOptions {
  args?: string[];
  env?: Record<string, string>;
  workingDir?: string;
  stdinMode?: StdinMode;
  stdoutMode?: OutputMode;
  stderrMode?: OutputMode;
  user?: ProcessUser;
  /**
   * Optional managed-process name. Supplying this opts into managed process behavior, and
   * lets the process be addressed by this name (in addition to its PID) in
   * `getProcess`/`killProcess`/`sendSignal`/etc. — useful because a managed process's PID
   * changes when it restarts. May contain any characters except `/` and must not be all
   * digits (numeric strings are reserved for PID addressing). If omitted, the daemon assigns
   * an opaque id (`ManagedProcessInfo.id`) and the process is addressable only by its current PID.
   */
  name?: string;
  /** Optional restart behavior. Supplying this opts into managed process behavior. */
  restart?: RestartPolicyConfig;
  /** Optional HTTP/TCP health check. Supplying this opts into managed process behavior. */
  healthCheck?: ProcessHealthCheck;
}

export interface ProcessInfo {
  handle?: number;
  pid: number;
  status: ProcessStatus;
  exitCode?: number;
  signal?: number;
  stdinWritable: boolean;
  command: string;
  args: string[];
  startedAt: Date;
  endedAt?: Date;
  managed?: ManagedProcessInfo;
}

export interface SendSignalResponse {
  success: boolean;
}

export interface OutputResponse {
  pid: number;
  lines: string[];
  lineCount: number;
}

export interface OutputEvent {
  line: string;
  timestamp: Date;
  stream?: string;
}

// --- Run ---

export interface RunOptions {
  args?: string[];
  env?: Record<string, string>;
  workingDir?: string;
  timeout?: number;
  user?: ProcessUser;
}

export interface CommandResult {
  exitCode: number;
  stdout: string;
  stderr: string;
}

// --- File operations ---

export interface DirectoryEntry {
  name: string;
  isDir: boolean;
  size?: number;
  modifiedAt?: Date;
}

export interface ListDirectoryResponse {
  path: string;
  entries: DirectoryEntry[];
}

// --- PTY ---

export interface CreatePtySessionOptions {
  command: string;
  args?: string[];
  env?: Record<string, string>;
  workingDir?: string;
  rows?: number;
  cols?: number;
}

export interface PtySessionInfo {
  sessionId: string;
  token: string;
}

// --- Health ---

export interface HealthResponse {
  healthy: boolean;
}

export interface DaemonInfo {
  version: string;
  uptimeSecs: number;
  runningProcesses: number;
  totalProcesses: number;
}

// --- Client options ---

export interface SandboxClientOptions {
  apiUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  namespace?: string;
  maxRetries?: number;
  retryBackoffMs?: number;
  /** Total HTTP request timeout in seconds. Default: 300. */
  requestTimeout?: number;
  /** @deprecated Use requestTimeout. Total HTTP request timeout in milliseconds. */
  timeoutMs?: number;
}

export interface SandboxOptions {
  sandboxId: string;
  proxyUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  routingHint?: string;
  resolveProxyInfo?: (
    identifier: string,
  ) => Promise<SandboxInfo & { readonly traceId: string }>;
  /** Optional total HTTP request timeout in seconds for sandbox proxy operations. Omit for no total proxy timeout. */
  requestTimeout?: number;
  /** @deprecated Use requestTimeout. Optional total HTTP request timeout in milliseconds for sandbox proxy operations. */
  timeoutMs?: number;
  /**
   * @internal Shared Rust-backed lifecycle client. When provided, the proxy
   * client is minted via `connectProxy` so it reuses this client's connection
   * pool (HTTP/2 coalescing across sandboxes).
   */
  nativeClient?: import("./native-sandbox.js").NativeSandboxClient;
}

export interface CreateAndConnectOptions extends CreateSandboxOptions {
  poolId?: string;
  proxyUrl?: string;
  /** Total HTTP request timeout in seconds for sandbox startup. Default: client requestTimeout. */
  requestTimeout?: number;
  /** @deprecated Use requestTimeout. */
  startupTimeout?: number;
}

export interface SuspendResumeOptions {
  /** If false, fire-and-return without waiting for completion. Default: true. */
  wait?: boolean;
  /** Max seconds to wait when wait=true. Default: 300. */
  timeout?: number;
  /** Seconds between status polls when wait=true. Default: 1. */
  pollInterval?: number;
}

export interface CheckpointOptions extends SuspendResumeOptions {
  checkpointType?: CheckpointType;
  /** Defaults to `"local_ready"`, which is enough to resume from a checkpoint. */
  waitUntil?: SnapshotWaitCondition;
}

export interface ConnectOptions {
  sandboxId: string;
  proxyUrl?: string;
  routingHint?: string;
}

// --- JSON key conversion helpers ---

const CAMEL_TO_SNAKE_RE = /[A-Z]/g;

/** Convert a camelCase string to snake_case. */
export function camelToSnake(str: string): string {
  return str.replace(CAMEL_TO_SNAKE_RE, (ch) => "_" + ch.toLowerCase());
}

/** Convert a snake_case string to camelCase. */
export function snakeToCamel(str: string): string {
  return str.replace(/_([a-z])/g, (_, ch) => ch.toUpperCase());
}

/** Recursively convert all object keys from camelCase to snake_case. */
export function toSnakeKeys(obj: unknown): unknown {
  if (Array.isArray(obj)) return obj.map(toSnakeKeys);
  if (obj !== null && typeof obj === "object" && !(obj instanceof Date)) {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      result[camelToSnake(k)] = toSnakeKeys(v);
    }
    return result;
  }
  return obj;
}

/**
 * Parse a numeric timestamp into a Date.
 *
 * Handles seconds, milliseconds, and microseconds by checking magnitude.
 */
export function parseTimestamp(v: unknown): Date | undefined {
  if (v == null) return undefined;
  if (v instanceof Date) return v;
  if (typeof v === "string") {
    const parsed = Date.parse(v);
    return Number.isNaN(parsed) ? undefined : new Date(parsed);
  }
  const ts = Number(v);
  if (isNaN(ts)) return undefined;
  if (ts > 1e15) return new Date(ts / 1000); // microseconds → ms
  if (ts > 1e12) return new Date(ts); // already ms
  return new Date(ts * 1000); // seconds → ms
}

/**
 * Recursively convert all object keys from snake_case to camelCase,
 * with special handling for `id` → contextual name and timestamp parsing.
 */
export function fromSnakeKeys(
  obj: unknown,
  idField?: string,
): unknown {
  if (Array.isArray(obj)) return obj.map((item) => fromSnakeKeys(item, idField));
  if (obj !== null && typeof obj === "object" && !(obj instanceof Date)) {
    const result: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(obj as Record<string, unknown>)) {
      let key: string;
      if (k === "id" && idField) {
        key = idField;
      } else {
        key = snakeToCamel(k);
      }

      // Parse timestamp fields
      if (
        key.endsWith("At") ||
        key === "timestamp" ||
        key === "startedAt" ||
        key === "endedAt"
      ) {
        result[key] = parseTimestamp(v);
      } else if (typeof v === "object" && v !== null && !Array.isArray(v)) {
        result[key] = fromSnakeKeys(v);
      } else if (Array.isArray(v)) {
        result[key] = v.map((item) => fromSnakeKeys(item));
      } else {
        // Normalize null → undefined so optional fields match TypeScript's ? convention.
        result[key] = v === null ? undefined : v;
      }
    }
    return result;
  }
  return obj;
}
