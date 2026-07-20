/** Models and errors for the filesystem SDK. */

/** Git tree mode for a directory entry. */
const GIT_MODE_DIR = 0o40000;
const GIT_MODE_SYMLINK = 0o120000;

/**
 * Strip leading/trailing '/' in linear time (the regex equivalent
 * `/^\/+|\/+$/g` backtracks polynomially on adversarial many-slash input).
 */
export function trimSlashes(path: string): string {
  let start = 0;
  let end = path.length;
  while (start < end && path.charCodeAt(start) === 0x2f) start += 1;
  while (end > start && path.charCodeAt(end - 1) === 0x2f) end -= 1;
  return path.slice(start, end);
}

/** One filesystem as returned by listing/point-read endpoints. */
export interface FilesystemInfo {
  name: string;
  fullName: string;
  defaultBranch: string;
  status: string;
  kind: string;
}

/** Remote status of a filesystem: identity plus current head. */
export interface FilesystemStatus {
  name: string;
  status: string;
  defaultBranch: string;
  /**
   * Commit hash the default branch currently points at; null for an empty
   * filesystem that has never been written to.
   */
  headCommit: string | null;
  /** Server-side movement counter for the default branch, when reported. */
  generation: number | null;
}

/** One directory entry from a filesystem listing. */
export interface FileEntry {
  name: string;
  /** Path of the entry relative to the filesystem root. */
  path: string;
  /** Git blob/tree object id. */
  oid: string;
  /** Raw git mode (0o100644 file, 0o100755 executable, 0o120000 symlink, 0o40000 dir). */
  mode: number;
  /** Blob size in bytes when cheaply known server-side. */
  size: number | null;
  isDir: boolean;
  isSymlink: boolean;
}

export function fileEntryFromWire(
  entry: { name: string; oid?: string; mode?: number; size?: number | null },
  dirPath: string,
): FileEntry {
  const mode = entry.mode ?? 0o100644;
  const prefix = trimSlashes(dirPath);
  return {
    name: entry.name,
    path: prefix ? `${prefix}/${entry.name}` : entry.name,
    oid: entry.oid ?? "",
    mode,
    size: entry.size ?? null,
    isDir: mode === GIT_MODE_DIR,
    isSymlink: mode === GIT_MODE_SYMLINK,
  };
}

/** A durable version of the filesystem (a commit). */
export interface Snapshot {
  /** Commit hash — pass as `version` to read the filesystem at this point. */
  commit: string;
  tree: string;
  refName: string;
  parent: string | null;
  /** False when the write was a no-op (content identical to the parent). */
  created: boolean;
  message: string;
}

/**
 * Status of a local mount as reported by `tl fs status --json`.
 *
 * The mount daemon's JSON is versioned independently of this SDK, so only
 * stable fields are typed; the full payload is preserved in `raw`.
 */
export interface MountStatus {
  /** Local mount path. */
  path: string;
  /** Filesystem name this mount serves, when reported. */
  filesystem: string | null;
  /** Whether the daemon reports the mount as healthy/active. */
  mounted: boolean;
  /** Complete parsed JSON payload from the CLI. */
  raw: Record<string, unknown>;
}

/**
 * Map one `tl fs status --json` payload to a MountStatus.
 *
 * Semantics deliberately mirror the Python SDK: `mounted` honors key
 * presence (an explicit null means "not mounted"), and path/filesystem
 * fall through empty strings, not just null/undefined.
 */
export function mountStatusFromRaw(
  raw: Record<string, unknown>,
  localPath?: string,
): MountStatus {
  const mounted =
    "mounted" in raw
      ? Boolean(raw.mounted)
      : "active" in raw
        ? Boolean(raw.active)
        : true;
  const path = String(raw.path || raw.mount_path || localPath || "");
  const filesystem = (raw.filesystem || raw.file_system || null) as
    | string
    | null;
  return { path, filesystem, mounted, raw };
}

export class FilesystemError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FilesystemError";
  }
}

export class FilesystemNotFoundError extends FilesystemError {
  readonly filesystemName: string;
  constructor(name: string) {
    super(`Filesystem not found: ${name}`);
    this.name = "FilesystemNotFoundError";
    this.filesystemName = name;
  }
}

export class FileNotFoundInFilesystemError extends FilesystemError {
  readonly filesystem: string;
  readonly path: string;
  constructor(filesystem: string, path: string) {
    super(`File not found in filesystem ${filesystem}: ${path}`);
    this.name = "FileNotFoundInFilesystemError";
    this.filesystem = filesystem;
    this.path = path;
  }
}

export class FilesystemAPIError extends FilesystemError {
  readonly statusCode: number;
  constructor(statusCode: number, message: string) {
    super(`API error (status ${statusCode}): ${message}`);
    this.name = "FilesystemAPIError";
    this.statusCode = statusCode;
  }
}

export class MountError extends FilesystemError {
  constructor(message: string) {
    super(message);
    this.name = "MountError";
  }
}

export class CliNotFoundError extends MountError {
  constructor(detail: string) {
    super(
      "Mount operations require the Tensorlake CLI (`tl`) with `tl fs` " +
        `support: ${detail}. Install or upgrade it with: ` +
        "curl -fsSL https://tensorlake.ai/install.sh | sh",
    );
    this.name = "CliNotFoundError";
  }
}
