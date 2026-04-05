// --- Enums ---

export enum SandboxStatus {
  PENDING = "pending",
  RUNNING = "running",
  SNAPSHOTTING = "snapshotting",
  SUSPENDED = "suspended",
  TERMINATED = "terminated",
}

export enum SnapshotStatus {
  IN_PROGRESS = "in_progress",
  COMPLETED = "completed",
  FAILED = "failed",
}

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
  image?: string;
  cpus?: number;
  memoryMb?: number;
  ephemeralDiskMb?: number;
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
}

export interface CreateSandboxResponse {
  sandboxId: string;
  status: SandboxStatus;
}

export interface SandboxInfo {
  sandboxId: string;
  namespace: string;
  status: SandboxStatus;
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

export interface SnapshotAndWaitOptions {
  timeout?: number;
  pollInterval?: number;
}

// --- Pools ---

export interface CreatePoolOptions {
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
}

export interface CreateAndConnectOptions extends CreateSandboxOptions {
  poolId?: string;
  proxyUrl?: string;
  startupTimeout?: number;
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
