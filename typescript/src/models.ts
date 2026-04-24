// --- Enums ---

export enum SandboxStatus {
  PENDING = "pending",
  RUNNING = "running",
  SNAPSHOTTING = "snapshotting",
  SUSPENDING = "suspending",
  SUSPENDED = "suspended",
  TERMINATED = "terminated",
}

export enum SnapshotStatus {
  IN_PROGRESS = "in_progress",
  COMPLETED = "completed",
  FAILED = "failed",
}

/**
 * Content mode for snapshot creation.
 *
 * - `"full"`: Full VM snapshot (memory + filesystem state). Sandboxes
 *   restored from this snapshot warm-restore VM memory.
 * - `"filesystem_only"`: Filesystem-only snapshot. Sandboxes restored from
 *   this snapshot cold-boot from the snapshot tarball instead of warm-
 *   restoring VM state. Use this for sandbox image builds so that the
 *   restored sandbox bypasses Firecracker's overlay-path constraints.
 */
export type SnapshotContentMode = "full" | "filesystem_only";

export enum ProcessStatus {
  RUNNING = "running",
  EXITED = "exited",
  SIGNALED = "signaled",
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

export interface NetworkConfig {
  allowInternetAccess: boolean;
  allowOut: string[];
  denyOut: string[];
}

// --- Sandbox lifecycle ---

export interface CreateSandboxOptions {
  /** Optional sandbox image name, such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image. When omitted, Tensorlake uses the default managed environment. */
  image?: string;
  cpus?: number;
  memoryMb?: number;
  /** Root disk size in megabytes. When omitted, the server uses its default disk size. */
  diskMb?: number;
  /** @deprecated Use `diskMb` instead. */
  disk_mb?: number;
  secretNames?: string[];
  timeoutSecs?: number;
  entrypoint?: string[];
  allowInternetAccess?: boolean;
  allowOut?: string[];
  denyOut?: string[];
  snapshotId?: string;
  /** Optional name for the sandbox. Named sandboxes support suspend/resume. When absent the sandbox is ephemeral. */
  name?: string;
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
  routingHint?: string;
}

export interface SandboxInfo {
  sandboxId: string;
  namespace: string;
  status: SandboxStatus;
  /** Resolved sandbox image name. */
  image?: string;
  resources: ContainerResourcesInfo;
  secretNames: string[];
  timeoutSecs?: number;
  entrypoint?: string[];
  network?: NetworkConfig;
  poolId?: string;
  outcome?: string;
  createdAt?: Date;
  terminatedAt?: Date;
  name?: string;
  allowUnauthenticatedAccess?: boolean;
  exposedPorts?: number[];
  sandboxUrl?: string;
  routingHint?: string;
}

export interface SandboxPortAccess {
  allowUnauthenticatedAccess: boolean;
  exposedPorts: number[];
  sandboxUrl?: string;
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
  error?: string;
  snapshotUri?: string;
  sizeBytes?: number;
  createdAt?: Date;
}

export interface SnapshotOptions {
  /**
   * Optional content mode for the snapshot. When omitted the server picks
   * its default. Use `"filesystem_only"` for snapshots intended for sandbox
   * image builds so that restored sandboxes cold-boot.
   */
  contentMode?: SnapshotContentMode;
}

export interface SnapshotAndWaitOptions extends SnapshotOptions {
  timeout?: number;
  pollInterval?: number;
}

// --- Pools ---

export interface CreatePoolOptions {
  /** Sandbox image name, such as `tensorlake/ubuntu-minimal` or a registered Sandbox Image. */
  image: string;
  cpus?: number;
  memoryMb?: number;
  ephemeralDiskMb?: number;
  secretNames?: string[];
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
  secretNames?: string[];
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
  secretNames: string[];
  timeoutSecs: number;
  entrypoint?: string[];
  maxContainers?: number;
  warmContainers?: number;
  containers?: PoolContainerInfo[];
  createdAt?: Date;
  updatedAt?: Date;
}

// --- Process management ---

export interface StartProcessOptions {
  args?: string[];
  env?: Record<string, string>;
  workingDir?: string;
  stdinMode?: StdinMode;
  stdoutMode?: OutputMode;
  stderrMode?: OutputMode;
}

export interface ProcessInfo {
  pid: number;
  status: ProcessStatus;
  exitCode?: number;
  signal?: number;
  stdinWritable: boolean;
  command: string;
  args: string[];
  startedAt: Date;
  endedAt?: Date;
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
}

export interface SandboxOptions {
  sandboxId: string;
  proxyUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  routingHint?: string;
}

export interface CreateAndConnectOptions extends CreateSandboxOptions {
  poolId?: string;
  proxyUrl?: string;
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
  contentMode?: SnapshotContentMode;
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
