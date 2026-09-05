import { resolve } from "node:path";
import { MountError } from "./filesystem-models.js";
import { MountCli } from "./mount-cli.js";

export interface RepositoryMountOptions {
  /** Stateless read-only view; cannot be combined with publish or workspace. */
  readOnly?: boolean;
  /** Publish each explicit snapshot onto the mounted branch. */
  publish?: boolean;
  /** Resume a private workspace; cannot be combined with publish or readOnly. */
  workspace?: string;
}

export interface RepositoryUnmountOptions {
  /** Delete the private server workspace after detaching. Defaults to false. */
  deleteWorkspace?: boolean;
  /** Drop unpublished local state. Defaults to false. */
  discard?: boolean;
}

/** Stable fields from `tl git status --json`, with the full payload in raw. */
export interface RepositoryMountStatus {
  path: string;
  repository: string | null;
  reference: string | null;
  workspaceId: string | null;
  state: string | null;
  localChanges: boolean | null;
  raw: Record<string, unknown>;
}

function requireNonEmpty(value: string, name: string): void {
  if (typeof value !== "string" || value.length === 0) {
    throw new TypeError(`${name} must be a non-empty string`);
  }
}

/** @internal CLI implementation; obtain mounts through RepositoryClient. */
export class GitCli extends MountCli {
  constructor(envOverrides: Record<string, string>) {
    super("git", envOverrides);
  }

  async mount(
    target: string,
    localPath: string,
    options: RepositoryMountOptions = {},
  ): Promise<RepositoryMount> {
    requireNonEmpty(target, "target");
    requireNonEmpty(localPath, "localPath");
    if (options.workspace !== undefined) requireNonEmpty(options.workspace, "workspace");
    if (
      (options.readOnly && (options.publish || options.workspace !== undefined)) ||
      (options.publish && options.workspace !== undefined)
    ) {
      throw new TypeError("readOnly, publish, and workspace are mutually exclusive");
    }
    // Keep the handle attached to this path if the caller later changes CWD.
    const path = resolve(localPath);
    const args = ["mount"];
    if (options.readOnly) args.push("--ro");
    if (options.publish) args.push("--publish");
    if (options.workspace !== undefined) args.push(`--workspace=${options.workspace}`);
    // No shell: preserve refs, subtrees, whitespace, and option-like names literally.
    args.push("--", target, path);
    await this.run(args);
    return new RepositoryMount(this, target, path, options.readOnly ?? false);
  }

  async unmount(localPath: string, options: RepositoryUnmountOptions = {}): Promise<void> {
    requireNonEmpty(localPath, "localPath");
    const args = ["unmount"];
    if (options.deleteWorkspace) args.push("--delete");
    if (options.discard) args.push("--discard");
    args.push("--", resolve(localPath));
    await this.run(args);
  }

  async status(localPath?: string): Promise<RepositoryMountStatus> {
    const args = ["status", "--json"];
    if (localPath !== undefined) {
      requireNonEmpty(localPath, "localPath");
      args.push("--", resolve(localPath));
    }
    const output = await this.run(args, 60_000);
    let raw: Record<string, unknown>;
    try {
      const payload: unknown = JSON.parse(output);
      if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
        throw new Error("expected a JSON object");
      }
      raw = payload as Record<string, unknown>;
    } catch {
      throw new MountError(`\`tl git status --json\` returned invalid JSON: ${output.slice(0, 200)}`);
    }
    const stringField = (key: string): string | null =>
      typeof raw[key] === "string" ? raw[key] : null;
    return {
      path: stringField("mountpoint") ?? (localPath === undefined ? "" : resolve(localPath)),
      repository: stringField("repository"),
      reference: stringField("reference"),
      workspaceId: stringField("workspace"),
      state: stringField("state"),
      localChanges: typeof raw.local_changes === "boolean" ? raw.local_changes : null,
      raw,
    };
  }

  async snapshot(path: string, message?: string): Promise<void> {
    const args = ["snapshot"];
    if (message !== undefined) args.push(`--message=${message}`);
    args.push("--", path);
    await this.run(args);
  }

  async sync(path: string, target?: string): Promise<void> {
    const args = ["sync", "--", path];
    if (target !== undefined) {
      requireNonEmpty(target, "target");
      args.push(target);
    }
    await this.run(args);
  }

  async rebase(path: string, target: string, failOnConflict: boolean): Promise<void> {
    requireNonEmpty(target, "target");
    const args = ["rebase"];
    if (failOnConflict) args.push("--fail-on-conflict");
    args.push("--", path, target);
    await this.run(args);
  }

  async promote(path: string, branch: string, merge: boolean): Promise<void> {
    requireNonEmpty(branch, "branch");
    const args = ["promote"];
    if (merge) args.push("--merge");
    args.push("--", path, branch);
    await this.run(args);
  }

  async prefetch(path: string): Promise<void> {
    await this.run(["prefetch", "--", path]);
  }
}

/** A repository mounted on the machine running Node.js via the `tl` CLI daemon. */
export class RepositoryMount {
  /** @internal Obtain instances via RepositoryClient.mount(). */
  constructor(
    private readonly cli: GitCli,
    /** Initial source (`repo[:ref][//subtree]`); sync/rebase can change the base. */
    readonly target: string,
    readonly path: string,
    readonly readOnly: boolean,
  ) {}

  /** Inspect workspace state, pending changes, and daemon diagnostics. */
  async status(): Promise<RepositoryMountStatus> {
    return this.cli.status(this.path);
  }

  /** Materialize autosaved edits as a commit; publish mounts also update their branch. */
  async snapshot(message?: string): Promise<void> {
    await this.cli.snapshot(this.path, message);
  }

  /** Refresh the current source, or switch an unsnapshotted workspace to a new base. */
  async sync(target?: string): Promise<void> {
    await this.cli.sync(this.path, target);
  }

  /** Replay snapshots and pending edits onto another base. Conflicts become diff3 markers by default. */
  async rebase(target: string, options?: { failOnConflict?: boolean }): Promise<void> {
    await this.cli.rebase(this.path, target, options?.failOnConflict ?? false);
  }

  /** Publish the workspace to a branch; merge allows landing when the branch moved. */
  async promote(branch: string, options?: { merge?: boolean }): Promise<void> {
    await this.cli.promote(this.path, branch, options?.merge ?? false);
  }

  /** Download and verify the mounted working set for network-free reads. */
  async prefetch(): Promise<void> {
    await this.cli.prefetch(this.path);
  }

  /** Detach, retaining the private workspace and unpublished state unless explicitly requested. */
  async unmount(options?: RepositoryUnmountOptions): Promise<void> {
    await this.cli.unmount(this.path, options);
  }
}
