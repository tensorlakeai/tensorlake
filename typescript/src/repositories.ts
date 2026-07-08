import * as defaults from "./defaults.js";
import { CloudClient } from "./cloud-client.js";
import { SandboxError } from "./errors.js";
import type { Traced } from "./http.js";
import {
  callNative,
  loadNativeSandboxBinding,
  type NativeRepositoryClient,
} from "./native-sandbox.js";
import { fromSnakeKeys } from "./models.js";
import { buildContextFromEnv } from "./sandbox-image.js";

export interface RepositoryClientOptions {
  apiUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  requestTimeout?: number;
  timeoutMs?: number;
}

export interface GitRepository {
  name: string;
  fullName: string;
  defaultBranch: string;
  status: string;
}

export interface RepositoryHandle {
  repo: string;
  url: string;
  baseRepo?: string;
}

export interface GitRef {
  name: string;
  oid: string;
}

export interface Branch {
  name: string;
  refName: string;
  oid: string;
}

export interface RepositoryInfo {
  repo: string;
  url: string;
  branches: Branch[];
  refs: GitRef[];
}

export interface GitCredential {
  token: string;
  tokenType: string;
  expiresAt: string;
  gitUsername: string;
  repoPattern: string;
  scopes: string[];
}

export interface CommitJobReadBack {
  done: number;
  total: number;
}

export interface CommitJobError {
  kind: string;
  message: string;
  retryable: boolean;
}

export interface CommitJobStatus {
  jobId: string;
  state: string;
  phase?: string;
  readBack?: CommitJobReadBack;
  commit?: string;
  tree?: string;
  refName?: string;
  parent?: string;
  created?: boolean;
  error?: CommitJobError;
}

export interface PushReport {
  commit: string;
  tree: string;
  refName: string;
  created: boolean;
  files: number;
  bytesTotal: number;
  chunksTotal: number;
  chunksUploaded: number;
  bytesUploaded: number;
  fileBlobOids: Array<[string, string]>;
}

export interface PushWorktreeOptions {
  path?: string;
  branch?: string;
  message?: string;
  expectOid?: string;
}

export interface MergeEntry {
  mode: number;
  oid: string;
}

export interface MergeConflict {
  path: string;
  kind: string;
  potential: boolean;
  ours?: MergeEntry;
  base?: MergeEntry;
  theirs?: MergeEntry;
}

export interface MergeStats {
  treesRead: number;
  entriesCompared: number;
  blobsMerged: number;
  wallMs: number;
}

export interface MergeReport {
  ours: string;
  theirs: string;
  mergeBase?: string;
  clean: boolean;
  fastForward: boolean;
  alreadyMerged: boolean;
  changedPaths: number;
  conflicts: MergeConflict[];
  stats: MergeStats;
  commit?: string;
  fastForwarded: boolean;
}

export interface MergeOptions {
  preflight?: boolean;
  deep?: boolean;
  materialize?: boolean;
  message?: string;
  base?: string;
}

export interface ConflictTerm {
  mode: number;
  oid: string;
}

export interface ConflictPath {
  path: string;
  kind: string;
  terms: Array<ConflictTerm | null>;
}

export interface MergeConflictRecord {
  version: number;
  oursCommit: string;
  theirsCommit: string;
  baseCommit?: string;
  paths: ConflictPath[];
  truncatedPaths: number;
}

export interface OperationRef {
  name: string;
  old?: string;
  new?: string;
}

export interface Operation {
  opId: string;
  repo: string;
  network?: string;
  parentOpId?: string;
  actor: string;
  atSecs: number;
  kind: string;
  result: string;
  refs: OperationRef[];
  packIds: string[];
  oldPackIds: string[];
  relatedRepo?: string;
  status?: string;
  oldPackCount: number;
  objectCount: number;
  packBytes: number;
}

export class RepositoryClient {
  private readonly native: NativeRepositoryClient;
  private readonly requestTimeoutMs: number;

  constructor(options?: RepositoryClientOptions) {
    this.requestTimeoutMs = resolveRequestTimeoutMs(options);
    const binding = loadNativeSandboxBinding();
    if (typeof binding.NativeRepositoryClient !== "function") {
      throw new SandboxError(
        "native binding does not export the repository client; rebuild with 'npm run build:native'",
      );
    }
    this.native = new binding.NativeRepositoryClient(
      options?.apiUrl ?? defaults.API_URL,
      options?.apiKey ?? defaults.API_KEY ?? null,
      options?.organizationId ?? null,
      options?.projectId ?? null,
      null,
      this.requestTimeoutMs / 1000,
    );
  }

  static forCloud(options?: RepositoryClientOptions): RepositoryClient {
    return new RepositoryClient({
      apiUrl: options?.apiUrl ?? "https://api.tensorlake.ai",
      apiKey: options?.apiKey,
      organizationId: options?.organizationId,
      projectId: options?.projectId,
      requestTimeout: options?.requestTimeout,
      timeoutMs: options?.timeoutMs,
    });
  }

  static async fromEnv(): Promise<RepositoryClient> {
    const context = buildContextFromEnv();
    if (!context.apiKey) {
      if (context.personalAccessToken) {
        throw new SandboxError(
          "Repository SDKs require TENSORLAKE_API_KEY. Personal access tokens are CLI-only.",
        );
      }
      throw new SandboxError("Missing TENSORLAKE_API_KEY credentials.");
    }
    return new RepositoryClient({
      apiUrl: context.apiUrl,
      apiKey: context.apiKey,
      projectId: context.projectId ?? await projectIdFromApiKey(context.apiUrl, context.apiKey),
    });
  }

  close(): void {
    // The native client releases its connection pool on GC; nothing to do.
  }

  url(repo: string): string {
    return this.native.gitRepoUrl(repo);
  }

  async create(
    repo: string,
    options?: { defaultBranch?: string },
  ): Promise<Traced<RepositoryHandle>> {
    return this.tracedJson<RepositoryHandle>(() =>
      this.native.createRepo(repo, options?.defaultBranch ?? null),
    );
  }

  async list(): Promise<Traced<GitRepository[]>> {
    const { traceId, json } = await callNative(() => this.native.listRepos());
    const parsed = fromSnakeKeys(JSON.parse(json)) as { repos?: GitRepository[] };
    return Object.assign(parsed.repos ?? [], { traceId });
  }

  async delete(repo: string): Promise<void> {
    await callNative(() => this.native.deleteRepo(repo));
  }

  async fork(repo: string, baseRepo: string): Promise<Traced<RepositoryHandle>> {
    return this.tracedJson<RepositoryHandle>(() =>
      this.native.forkRepo(repo, baseRepo),
    );
  }

  async archive(repo: string): Promise<void> {
    await callNative(() => this.native.archiveRepo(repo));
  }

  async restore(repo: string): Promise<void> {
    await callNative(() => this.native.restoreRepo(repo));
  }

  async info(repo: string): Promise<Traced<RepositoryInfo>> {
    return this.tracedJson<RepositoryInfo>(() => this.native.repoInfo(repo));
  }

  async branches(repo: string): Promise<Traced<Branch[]>> {
    const { traceId, json } = await callNative(() => this.native.listBranches(repo));
    const parsed = fromSnakeKeys(JSON.parse(json)) as { branches?: Branch[] };
    return Object.assign(parsed.branches ?? [], { traceId });
  }

  async refs(repo: string): Promise<Traced<GitRef[]>> {
    const { traceId, json } = await callNative(() => this.native.listRefs(repo));
    const parsed = fromSnakeKeys(JSON.parse(json)) as { refs?: GitRef[] };
    return Object.assign(parsed.refs ?? [], { traceId });
  }

  async deleteBranch(repo: string, branch: string): Promise<void> {
    await callNative(() => this.native.deleteBranch(repo, branch));
  }

  async operations(repo: string): Promise<Traced<Operation[]>> {
    const { traceId, json } = await callNative(() => this.native.listOperations(repo));
    const parsed = fromSnakeKeys(JSON.parse(json)) as { operations?: Operation[] };
    return Object.assign(parsed.operations ?? [], { traceId });
  }

  async credential(repo?: string): Promise<GitCredential> {
    const json = await callNative(() => this.native.gitCredential(repo ?? null));
    return JSON.parse(json) as GitCredential;
  }

  async commitStatus(repo: string, jobId: string): Promise<Traced<CommitJobStatus>> {
    return this.tracedJson<CommitJobStatus>(() =>
      this.native.commitStatus(repo, jobId),
    );
  }

  async pushWorktree(
    repo: string,
    options?: PushWorktreeOptions,
  ): Promise<Traced<PushReport>> {
    return this.tracedJson<PushReport>(() =>
      this.native.pushWorktree(
        repo,
        options?.path ?? process.cwd(),
        options?.branch ?? "main",
        options?.message ?? "Update repository",
        options?.expectOid ?? null,
      ),
    );
  }

  async merge(
    repo: string,
    ours: string,
    theirs: string,
    options?: MergeOptions,
  ): Promise<Traced<MergeReport>> {
    return this.tracedJson<MergeReport>(() =>
      this.native.mergeRepo(
        repo,
        ours,
        theirs,
        options?.preflight ?? false,
        options?.deep ?? false,
        options?.materialize ?? false,
        options?.message ?? null,
        options?.base ?? null,
      ),
    );
  }

  async commitConflicts(
    repo: string,
    commit: string,
  ): Promise<Traced<MergeConflictRecord> | null> {
    const { traceId, json } = await callNative(() =>
      this.native.commitConflicts(repo, commit),
    );
    const parsed = JSON.parse(json) as unknown;
    if (parsed == null) {
      return null;
    }
    return Object.assign(fromSnakeKeys(parsed) as MergeConflictRecord, { traceId });
  }

  private async tracedJson<T extends object>(
    fn: () => Promise<{ traceId: string; json: string }>,
  ): Promise<Traced<T>> {
    const { traceId, json } = await callNative(fn);
    return Object.assign(fromSnakeKeys(JSON.parse(json)) as T, { traceId });
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

async function projectIdFromApiKey(apiUrl: string, apiKey: string): Promise<string> {
  const client = new CloudClient({ apiUrl, apiKey });
  try {
    const projectId = extractProjectId(await client.introspectApiKey());
    if (!projectId) {
      throw new SandboxError("Repository API key did not include project context.");
    }
    return projectId;
  } finally {
    client.close();
  }
}

function extractProjectId(value: unknown): string | undefined {
  if (value == null || typeof value !== "object") {
    return undefined;
  }
  const object = value as Record<string, unknown>;
  for (const key of ["projectId", "project_id"]) {
    const candidate = object[key];
    if (typeof candidate === "string" && candidate.length > 0) {
      return candidate;
    }
  }

  const project = object.project;
  if (typeof project === "string" && project.length > 0) {
    return project;
  }
  if (project != null && typeof project === "object") {
    const projectObject = project as Record<string, unknown>;
    for (const key of ["id", "projectId", "project_id"]) {
      const candidate = projectObject[key];
      if (typeof candidate === "string" && candidate.length > 0) {
        return candidate;
      }
    }
  }

  for (const key of ["scope", "apiKey", "api_key", "key"]) {
    const nested = extractProjectId(object[key]);
    if (nested) {
      return nested;
    }
  }

  const projects = object.projects;
  if (Array.isArray(projects) && projects.length === 1) {
    return extractProjectId(projects[0]);
  }

  return undefined;
}
