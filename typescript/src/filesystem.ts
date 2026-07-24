/**
 * Client for Tensorlake filesystems.
 *
 * A filesystem is a durable, versioned file tree that lives in Tensorlake
 * Cloud. Every write publishes a durable live version; {@link Filesystem.snapshot}
 * retains the current version permanently. Files can be read at any retained
 * version, and a filesystem can be mounted to a local path through the `tl`
 * CLI's FUSE/FSKit daemon.
 *
 * Server-side SDK writes hash complete files locally, upload missing bytes
 * directly to checksum-bound object-store targets, and atomically publish
 * one durable snapshot. Payload bytes never transit the API service.
 *
 * @example
 * const client = new FilesystemClient();   // env-based auth
 * const fs = await client.create("my-data");
 * await fs.writeFile("docs/hello.txt", "hi");
 * console.log(await fs.readText("docs/hello.txt"));
 * const snapshot = await fs.snapshot("after first write");
 *
 * const mount = await fs.mount("/mnt/my-data"); // requires the `tl` CLI
 * // ... use it as a normal directory
 * await mount.unmount();
 */

import { execFile } from "node:child_process";
import { randomBytes } from "node:crypto";
import { accessSync, constants } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import * as defaults from "./defaults.js";
import {
  CliNotFoundError,
  FileNotFoundInFilesystemError,
  FilesystemAPIError,
  FilesystemError,
  FilesystemNotFoundError,
  MountError,
  fileEntryFromWire,
  mountStatusFromRaw,
  trimSlashes,
  type FileEntry,
  type FilesystemInfo,
  type FilesystemSnapshot,
  type FilesystemSnapshotInfo,
  type FilesystemStatus,
  type FilesystemVersion,
  type MountStatus,
} from "./filesystem-models.js";
import {
  loadNativeSandboxBinding,
  type NativeRepositoryClient,
} from "./native-sandbox.js";
import { buildContextFromEnv } from "./sandbox-image.js";

const execFileAsync = promisify(execFile);

const FILESYSTEM_REPO_KIND = "filesystem";

export interface FilesystemClientOptions {
  apiKey?: string;
  apiUrl?: string;
  organizationId?: string;
  projectId?: string;
  /** @internal Test seam: bypass the native binding loader. */
  nativeClient?: NativeRepositoryClient;
}

/**
 * Translate a structured native error (`{category, status, message}` JSON in
 * the error message) into the filesystem error hierarchy. `notFound`, when
 * given, replaces the generic API error for 404s.
 */
function translateNativeError(error: unknown, notFound?: FilesystemError): Error {
  const message = error instanceof Error ? error.message : String(error);
  let payload: { category?: unknown; status?: unknown; message?: unknown };
  try {
    payload = JSON.parse(message);
  } catch {
    return error instanceof Error ? error : new FilesystemError(message);
  }
  if (!payload || typeof payload.category !== "string") {
    return error instanceof Error ? error : new FilesystemError(message);
  }
  const detail =
    typeof payload.message === "string" ? payload.message : message;
  // Connection-level failures carry a fabricated gateway status in the napi
  // layer; surface them as plain FilesystemError (matching Python, where
  // they carry no status) instead of a fake server FilesystemAPIError.
  if (payload.category === "connection") {
    return new FilesystemError(detail);
  }
  if (payload.status === 404 && notFound !== undefined) {
    return notFound;
  }
  if (typeof payload.status === "number") {
    return new FilesystemAPIError(payload.status, detail);
  }
  return new FilesystemError(detail);
}

async function callNative<T>(
  fn: () => Promise<T>,
  notFound?: FilesystemError,
): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    throw translateNativeError(error, notFound);
  }
}

/**
 * Runs `tl fs ...` commands for local mount operations.
 *
 * Mounting a filesystem to a local path is served by a FUSE (Linux) / FSKit
 * (macOS) daemon that ships only inside the Tensorlake CLI, so the SDK drives
 * mounts by invoking `tl fs mount/unmount/snapshot/status`. Everything else
 * goes through the Rust core and does not need the CLI.
 */
class FsCli {
  private binary: string | null = null;
  private readonly envOverrides: Record<string, string>;

  constructor(envOverrides: Record<string, string>) {
    this.envOverrides = envOverrides;
  }

  private async findCli(): Promise<string> {
    const candidates: string[] = [];
    if (process.env.TENSORLAKE_CLI) {
      candidates.push(process.env.TENSORLAKE_CLI);
    }
    candidates.push("tl");
    const installed = join(homedir(), ".tensorlake", "bin", "tl");
    try {
      // Existence, not executability: a present-but-broken install should be
      // probed and blamed as "upgrade required" (parity with Python), not
      // reported as missing.
      accessSync(installed, constants.F_OK);
      candidates.push(installed);
    } catch {
      // not installed at the default path
    }
    // A candidate that exists but fails the probe means "upgrade tl"; a
    // candidate that does not exist must not be blamed as outdated. The
    // Python SDK mirrors these exact semantics.
    let unsupported: string | null = null;
    for (const candidate of candidates) {
      try {
        await execFileAsync(candidate, ["fs", "--help"], { timeout: 15_000 });
        return candidate;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
          unsupported = candidate;
        }
      }
    }
    throw new CliNotFoundError(
      unsupported
        ? `\`tl\` at ${unsupported} does not support \`tl fs\` (upgrade required)`
        : "`tl` was not found on PATH",
    );
  }

  private async run(args: string[], timeoutMs = 300_000): Promise<string> {
    if (this.binary === null) {
      this.binary = await this.findCli();
    }
    try {
      const { stdout } = await execFileAsync(this.binary, ["fs", ...args], {
        timeout: timeoutMs,
        env: { ...process.env, ...this.envOverrides },
        maxBuffer: 16 * 1024 * 1024,
      });
      return stdout;
    } catch (error) {
      const failure = error as { stderr?: string; stdout?: string; killed?: boolean };
      if (failure.killed) {
        throw new MountError(`\`tl fs ${args[0]}\` timed out after ${timeoutMs}ms`);
      }
      const detail = (failure.stderr || failure.stdout || String(error)).trim();
      throw new MountError(`\`tl fs ${args.join(" ")}\` failed: ${detail}`);
    }
  }

  // Flags always precede a `--` end-of-options separator so caller-supplied
  // names/paths can never be parsed as CLI flags (e.g. a path literally
  // named "--discard" must stay a path, not become the destructive flag).
  async mount(filesystem: string, localPath: string, readonly: boolean): Promise<void> {
    const args = ["mount"];
    if (readonly) args.push("--ro");
    args.push("--", filesystem, localPath);
    await this.run(args);
  }

  async unmount(localPath: string, discard: boolean): Promise<void> {
    const args = ["unmount"];
    if (discard) args.push("--discard");
    args.push("--", localPath);
    await this.run(args);
  }

  async snapshot(localPath: string, message?: string): Promise<void> {
    const args = ["snapshot"];
    // Attached form: a detached value ("-m", "-msg") is rejected by the CLI
    // parser when the message starts with '-'.
    if (message) args.push(`--message=${message}`);
    args.push("--", localPath);
    await this.run(args);
  }

  async status(localPath?: string): Promise<Record<string, unknown>> {
    const args = ["status", "--json"];
    if (localPath) args.push("--", localPath);
    const output = await this.run(args, 60_000);
    try {
      const payload = JSON.parse(output);
      return typeof payload === "object" && payload !== null
        ? (payload as Record<string, unknown>)
        : { status: payload };
    } catch {
      throw new MountError(
        `\`tl fs status --json\` returned invalid JSON: ${output.slice(0, 200)}`,
      );
    }
  }
}

/** Manages the filesystems of one Tensorlake project. */
export class FilesystemClient {
  private readonly native: NativeRepositoryClient;
  private readonly cli: FsCli;

  /**
   * Any option left unset is resolved from the environment
   * (`TENSORLAKE_API_KEY` / `TENSORLAKE_PAT`, `TENSORLAKE_API_URL`,
   * `TENSORLAKE_ORGANIZATION_ID`, `TENSORLAKE_PROJECT_ID`).
   */
  constructor(options: FilesystemClientOptions = {}) {
    const context = buildContextFromEnv();
    const token =
      options.apiKey ?? context.apiKey ?? context.personalAccessToken;
    if (!token) {
      throw new FilesystemError(
        "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.",
      );
    }
    const organizationId = options.organizationId ?? context.organizationId;
    const projectId = options.projectId ?? context.projectId;
    if (!organizationId || !projectId) {
      throw new FilesystemError(
        "Filesystem operations require organization and project context " +
          "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID).",
      );
    }
    if (options.nativeClient) {
      this.native = options.nativeClient;
    } else {
      const binding = loadNativeSandboxBinding();
      if (typeof binding.NativeRepositoryClient !== "function") {
        throw new FilesystemError(
          "native binding does not export the repository client; rebuild with 'npm run build:native'",
        );
      }
      this.native = new binding.NativeRepositoryClient(
        options.apiUrl ?? context.apiUrl,
        token,
        organizationId,
        projectId,
        `tensorlake-typescript-sdk/${defaults.SDK_VERSION}`,
        null,
      );
    }
    const envOverrides: Record<string, string> = {
      TENSORLAKE_ORGANIZATION_ID: organizationId,
      TENSORLAKE_PROJECT_ID: projectId,
      // The CLI must target the same deployment the data plane does, or a
      // mount could resolve a same-named filesystem in the wrong environment.
      TENSORLAKE_API_URL: options.apiUrl ?? context.apiUrl,
    };
    if (options.apiKey) {
      envOverrides.TENSORLAKE_API_KEY = options.apiKey;
    }
    this.cli = new FsCli(envOverrides);
  }

  // -- lifecycle --------------------------------------------------------------

  /** Create a new filesystem and return a handle to it. */
  async create(name: string): Promise<Filesystem> {
    if (typeof name !== "string") {
      throw new TypeError("name must be a string");
    }
    if (name.length === 0) {
      throw new FilesystemError("filesystem name must not be empty");
    }
    const raw = await callNative(() => this.native.createFilesystem(name));
    // The binding reports the effective default branch; it differs from
    // "main" only when a lost-response retry adopted a pre-existing
    // filesystem.
    const created = JSON.parse(raw) as { default_branch?: string };
    return new Filesystem(
      this,
      this.native,
      this.cli,
      name,
      created.default_branch || "main",
    );
  }

  /**
   * Fork a filesystem at its live head or an explicit retained snapshot.
   * The server shares immutable content and publishes only metadata.
   */
  async fork(
    name: string,
    baseFilesystem: string,
    snapshot?: string,
  ): Promise<Filesystem> {
    if (!name || !baseFilesystem) {
      throw new FilesystemError(
        "fork and base filesystem names must not be empty",
      );
    }
    await callNative(
      () =>
        this.native.forkFilesystem(
          name,
          baseFilesystem,
          snapshot ?? null,
        ),
      new FilesystemNotFoundError(baseFilesystem),
    );
    return new Filesystem(this, this.native, this.cli, name, "main");
  }

  /** Return a handle to an existing filesystem (verifies it exists). */
  async get(name: string): Promise<Filesystem> {
    const traced = await callNative(
      () => this.native.filesystemMeta(name),
      new FilesystemNotFoundError(name),
    );
    const meta = JSON.parse(traced.json) as {
      kind?: string;
      default_branch?: string;
    };
    if ((meta.kind ?? FILESYSTEM_REPO_KIND) !== FILESYSTEM_REPO_KIND) {
      throw new FilesystemNotFoundError(name);
    }
    return new Filesystem(
      this,
      this.native,
      this.cli,
      name,
      meta.default_branch || "main",
    );
  }

  /** List all filesystems in the project. */
  async list(): Promise<FilesystemInfo[]> {
    const traced = await callNative(() => this.native.listFilesystems());
    const page = JSON.parse(traced.json) as {
      repos?: Array<Record<string, unknown>>;
    };
    return (page.repos ?? []).map((repo) => ({
      name: String(repo.name ?? ""),
      fullName: String(repo.full_name ?? ""),
      status: String(repo.status ?? ""),
      kind: String(repo.kind ?? FILESYSTEM_REPO_KIND),
    }));
  }

  /** Permanently delete a filesystem and all its snapshots. */
  async delete(name: string): Promise<void> {
    await callNative(
      () => this.native.deleteFilesystem(name),
      new FilesystemNotFoundError(name),
    );
  }

  // -- local mounts -------------------------------------------------------------

  /** Mount a filesystem to a local path (requires the `tl` CLI). */
  async mount(
    name: string,
    localPath: string,
    readonly = false,
  ): Promise<FilesystemMount> {
    await this.cli.mount(name, localPath, readonly);
    return new FilesystemMount(this, this.cli, name, localPath, readonly);
  }

  /**
   * Unmount a locally mounted filesystem.
   *
   * `discard: true` drops local changes that were not yet uploaded.
   */
  async unmount(localPath: string, discard = false): Promise<void> {
    await this.cli.unmount(localPath, discard);
  }

  /** Status of a local mount (defaults to the mount containing CWD). */
  async mountStatus(localPath?: string): Promise<MountStatus> {
    return mountStatusFromRaw(await this.cli.status(localPath), localPath);
  }
}

type FileData = Uint8Array | string;

function toBuffer(data: FileData): Buffer {
  return typeof data === "string" ? Buffer.from(data, "utf-8") : Buffer.from(data);
}

/** Handle to one filesystem; reads/writes go through the Rust core. */
export class Filesystem {
  private readonly client: FilesystemClient;
  private readonly native: NativeRepositoryClient;
  private readonly cli: FsCli;
  readonly name: string;
  private defaultBranch: string | null;

  /** @internal — obtain instances via `FilesystemClient.create()` / `.get()`. */
  constructor(
    client: FilesystemClient,
    native: NativeRepositoryClient,
    cli: FsCli,
    name: string,
    defaultBranch: string | null = null,
  ) {
    this.client = client;
    this.native = native;
    this.cli = cli;
    this.name = name;
    this.defaultBranch = defaultBranch;
  }

  /**
   * The filesystem's default branch — the target of every write and the
   * default version of every read, so writes and `status()` can never
   * silently disagree on a non-"main" filesystem.
   */
  private async branch(): Promise<string> {
    if (!this.defaultBranch) {
      const traced = await callNative(
        () => this.native.filesystemMeta(this.name),
        new FilesystemNotFoundError(this.name),
      );
      const meta = JSON.parse(traced.json) as { default_branch?: string };
      this.defaultBranch = meta.default_branch || "main";
    }
    return this.defaultBranch;
  }

  // -- writes -------------------------------------------------------------------

  /** Write one file. Returns the snapshot (version) the write produced. */
  async writeFile(
    path: string,
    data: FileData,
    message?: string,
  ): Promise<FilesystemVersion> {
    return await this.writeFiles(new Map([[path, data]]), message);
  }

  /** Write several files (and/or delete paths) in one atomic snapshot. */
  async writeFiles(
    files: Map<string, FileData> | Record<string, FileData>,
    message?: string,
    deletes: string[] = [],
  ): Promise<FilesystemVersion> {
    const entries =
      files instanceof Map ? [...files.entries()] : Object.entries(files);
    if (entries.length === 0 && deletes.length === 0) {
      throw new FilesystemError("nothing to write: no files or deletions given");
    }
    const writes = entries.map(([path, data]) => ({
      path,
      content: toBuffer(data),
    }));
    // Truthiness, not nullish: an explicit "" gets the default too (parity
    // with the Python SDK).
    const resolvedMessage = message || `write ${writes.length} file(s) via SDK`;
    return await this.publishChanges(
      writes,
      deletes,
      [],
      [],
      resolvedMessage,
    );
  }

  /**
   * Stream one local file directly to object storage in bounded parts.
   * This is the memory-bounded API for multi-GiB files.
   */
  async writeFileFromPath(
    path: string,
    localPath: string,
    message?: string,
  ): Promise<FilesystemVersion> {
    return await this.writeFilesFromPaths(
      new Map([[path, localPath]]),
      message,
    );
  }

  /**
   * Atomically publish local files without loading their complete payloads
   * into JavaScript or Rust memory.
   */
  async writeFilesFromPaths(
    files: Map<string, string> | Record<string, string>,
    message?: string,
  ): Promise<FilesystemVersion> {
    const entries =
      files instanceof Map ? [...files.entries()] : Object.entries(files);
    if (entries.length === 0) {
      throw new FilesystemError("nothing to write: no local files given");
    }
    const writes = entries.map(([path, localPath]) => ({ path, localPath }));
    const resolvedMessage =
      message || `write ${writes.length} local file(s) via SDK`;
    const branch = await this.branch();
    const traced = await callNative(
      () =>
        this.native.pushFilesystemPaths(
          this.name,
          writes,
          resolvedMessage,
          branch,
          randomBytes(16).toString("hex"),
        ),
      new FilesystemNotFoundError(this.name),
    );
    const report = JSON.parse(traced.json) as {
      version_id?: string;
      previous_version_id?: string | null;
    };
    const versionId = String(report.version_id ?? "");
    const previousVersionId =
      report.previous_version_id == null
        ? null
        : String(report.previous_version_id);
    return {
      versionId,
      previousVersionId,
      created: previousVersionId !== versionId,
      message: resolvedMessage,
    };
  }

  /**
   * Move a file or directory without downloading or re-uploading its bytes.
   * The source removal and destination creation publish as one atomic snapshot.
   */
  async moveFile(
    from: string,
    to: string,
    message?: string,
  ): Promise<FilesystemVersion> {
    return await this.publishChanges(
      [],
      [],
      [{ from, to }],
      [],
      message || `move ${from} to ${to} via SDK`,
    );
  }

  /**
   * Copy a file or directory by reusing immutable content references. No
   * payload bytes traverse the client, API server, or object store.
   */
  async copyFile(
    from: string,
    to: string,
    message?: string,
  ): Promise<FilesystemVersion> {
    return await this.publishChanges(
      [],
      [],
      [],
      [{ from, to }],
      message || `copy ${from} to ${to} via SDK`,
    );
  }

  private async publishChanges(
    writes: Array<{ path: string; content: Buffer }>,
    deletes: string[],
    moves: Array<{ from: string; to: string }>,
    copies: Array<{ from: string; to: string }>,
    message: string,
  ): Promise<FilesystemVersion> {
    const branch = await this.branch();
    const traced = await callNative(
      () =>
        this.native.pushFilesystemFiles(
          this.name,
          writes,
          deletes,
          moves,
          copies,
          message,
          branch,
          // One key per logical write: a retried submit reattaches to the
          // same durable commit job instead of double-committing.
          randomBytes(16).toString("hex"),
        ),
      new FilesystemNotFoundError(this.name),
    );
    const report = JSON.parse(traced.json) as {
      version_id?: string;
      previous_version_id?: string | null;
    };
    const versionId = String(report.version_id ?? "");
    const previousVersionId =
      report.previous_version_id == null
        ? null
        : String(report.previous_version_id);
    return {
      versionId,
      previousVersionId,
      created: previousVersionId !== versionId,
      message,
    };
  }

  /** Delete one file. Returns the durable version the deletion produced. */
  async deleteFile(path: string, message?: string): Promise<FilesystemVersion> {
    return await this.writeFiles(
      new Map(),
      message || `delete ${path} via SDK`,
      [path],
    );
  }

  /**
   * Return the filesystem's current version as a snapshot.
   *
   * Writes publish durable live versions. This retains the current head
   * permanently in one metadata-only server operation without changing or
   * copying any content.
   */
  async snapshot(message = "snapshot via SDK"): Promise<FilesystemSnapshot> {
    const resolvedMessage = message || "snapshot via SDK";
    const traced = await callNative(
      () =>
        this.native.retainFilesystemSnapshot(
          this.name,
          resolvedMessage,
          randomBytes(16).toString("hex"),
        ),
      new FilesystemNotFoundError(this.name),
    );
    const retained = JSON.parse(traced.json) as {
      snapshot_id?: string;
      message?: string;
    };
    return {
      id: String(retained.snapshot_id ?? ""),
      message: retained.message || resolvedMessage,
    };
  }

  /** List explicitly retained snapshots without reading filesystem contents. */
  async listSnapshots(): Promise<FilesystemSnapshotInfo[]> {
    const traced = await callNative(
      () => this.native.listFilesystemSnapshots(this.name),
      new FilesystemNotFoundError(this.name),
    );
    const payload = JSON.parse(traced.json) as {
      snapshots?: Array<{
        snapshot_id?: string;
        created_at_ms?: number;
        message?: string;
      }>;
    };
    return (payload.snapshots ?? []).map((snapshot) => ({
      id: String(snapshot.snapshot_id ?? ""),
      createdAt: new Date(Number(snapshot.created_at_ms ?? 0)),
      message: String(snapshot.message ?? ""),
    }));
  }

  /** Delete one permanent retention point; shared/head-reachable bytes remain. */
  async deleteSnapshot(snapshot: string): Promise<void> {
    await callNative(
      () => this.native.deleteFilesystemSnapshot(this.name, snapshot),
      new FilesystemNotFoundError(this.name),
    );
  }

  // -- reads --------------------------------------------------------------------

  /**
   * Read a file's bytes at `version` (branch, ref, or snapshot commit;
   * defaults to the filesystem's default branch).
   */
  async readFile(path: string, version?: string): Promise<Uint8Array> {
    if (trimSlashes(path) === "") {
      throw new FilesystemError("file path must not be empty");
    }
    const resolvedVersion = version || (await this.branch());
    const traced = await callNative(
      () => this.native.readFilesystemFile(this.name, path, resolvedVersion),
      new FileNotFoundInFilesystemError(this.name, path),
    );
    return new Uint8Array(traced.data);
  }

  /** Read a file as UTF-8 text at `version`. Throws on non-UTF-8 content. */
  async readText(path: string, version?: string): Promise<string> {
    // fatal: parity with Python's strict bytes.decode("utf-8") — corrupt
    // data must raise, never silently decode to replacement characters.
    // ignoreBOM keeps a leading U+FEFF, exactly as Python does.
    return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(
      await this.readFile(path, version),
    );
  }

  /** List one directory (non-recursive) at `version`. */
  async listFiles(dirPath = "", version?: string): Promise<FileEntry[]> {
    const resolvedVersion = version || (await this.branch());
    const traced = await callNative(
      () => this.native.listFilesystemTree(this.name, dirPath, resolvedVersion),
      new FileNotFoundInFilesystemError(this.name, dirPath || "/"),
    );
    const page = JSON.parse(traced.json) as {
      entries?: Array<{ name: string; oid?: string; mode?: number; size?: number | null }>;
    };
    return (page.entries ?? []).map((entry) => fileEntryFromWire(entry, dirPath));
  }

  // -- status -------------------------------------------------------------------

  /** Remote status: identity plus the current native version. */
  async status(): Promise<FilesystemStatus> {
    const metaTraced = await callNative(
      () => this.native.filesystemMeta(this.name),
      new FilesystemNotFoundError(this.name),
    );
    const meta = JSON.parse(metaTraced.json) as Record<string, unknown>;
    const defaultBranch = String(meta.default_branch || "main");
    this.defaultBranch = defaultBranch;
    let versionId: string | null = null;
    let generation: number | null = null;
    try {
      // A 404 here means "no such ref yet" (an empty filesystem), so it is
      // deliberately NOT mapped to FilesystemNotFoundError.
      const refTraced = await callNative(() =>
        this.native.filesystemRefStatus(this.name, defaultBranch),
      );
      const ref = JSON.parse(refTraced.json) as Record<string, unknown>;
      versionId =
        (ref.resolved_commit as string) || (ref.oid as string) || null;
      generation = typeof ref.generation === "number" ? ref.generation : null;
    } catch (error) {
      // Only "no such ref yet" means an empty filesystem; anything else
      // (auth, 5xx) must not masquerade as one.
      if (!(error instanceof FilesystemAPIError) || error.statusCode !== 404) {
        throw error;
      }
    }
    return {
      name: this.name,
      status: String(meta.status ?? ""),
      versionId,
      generation,
    };
  }

  // -- mounts -------------------------------------------------------------------

  /** Mount this filesystem to a local path (requires the `tl` CLI). */
  async mount(localPath: string, readonly = false): Promise<FilesystemMount> {
    return await this.client.mount(this.name, localPath, readonly);
  }
}

/** A filesystem mounted to a local path via the `tl` CLI daemon. */
export class FilesystemMount {
  private readonly client: FilesystemClient;
  private readonly cli: FsCli;
  readonly filesystem: string;
  readonly path: string;
  readonly readonly: boolean;

  /** @internal — obtain instances via `FilesystemClient.mount()`. */
  constructor(
    client: FilesystemClient,
    cli: FsCli,
    filesystem: string,
    path: string,
    readonly: boolean,
  ) {
    this.client = client;
    this.cli = cli;
    this.filesystem = filesystem;
    this.path = path;
    this.readonly = readonly;
  }

  /** Flush pending local changes into a durable snapshot. */
  async snapshot(message?: string): Promise<void> {
    await this.cli.snapshot(this.path, message);
  }

  /** Local mount status as reported by the mount daemon. */
  async status(): Promise<MountStatus> {
    return await this.client.mountStatus(this.path);
  }

  /** Unmount; `discard: true` drops changes not yet uploaded. */
  async unmount(discard = false): Promise<void> {
    await this.client.unmount(this.path, discard);
  }
}
