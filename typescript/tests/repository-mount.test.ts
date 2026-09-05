import { chmodSync, existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { RepositoryClient } from "../src/repositories.js";
import { RepositoryMount } from "../src/repository-mount.js";
import { FilesystemClient } from "../src/filesystem.js";
import { CliNotFoundError, MountError } from "../src/filesystem-models.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

const cliHome = vi.hoisted(() => ({ path: "" }));
vi.mock("node:os", async (importOriginal) => ({
  ...await importOriginal<typeof import("node:os")>(),
  homedir: () => cliHome.path,
}));

interface Invocation {
  args: string[];
  env: Record<string, string>;
}

describe.skipIf(process.platform === "win32")("repository mount CLI boundary", () => {
  let directory: string;
  let binary: string;
  let log: string;
  let config: string;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "tensorlake-git-mount-"));
    cliHome.path = directory;
    binary = join(directory, "mock-tl");
    log = join(directory, "calls.jsonl");
    config = join(directory, "config.json");
    writeFileSync(config, "{}");
    // An actual child process exercises execFile's argument and environment boundary.
    writeFileSync(binary, `#!${process.execPath}
const fs = require("node:fs");
const args = process.argv.slice(2);
const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith("TENSORLAKE_")));
fs.appendFileSync(${JSON.stringify(log)}, JSON.stringify({ args, env }) + "\\n");
const config = JSON.parse(fs.readFileSync(${JSON.stringify(config)}, "utf8"));
if (args.includes("--help")) {
  process.exit(config.probeFailure ? 1 : 0);
}
if (config.error) {
  process.stderr.write(config.error);
  process.exit(1);
}
if (args[1] === "status") process.stdout.write(config.status ?? "{}");
`);
    chmodSync(binary, 0o755);
    vi.stubEnv("TENSORLAKE_CLI", binary);
    vi.stubEnv("PATH", directory);
    vi.stubEnv("TENSORLAKE_API_KEY", "ambient-key");
    vi.stubEnv("TENSORLAKE_API_URL", "https://ambient.example");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "ambient-org");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "ambient-project");
    installNativeStub();
  });

  afterEach(() => {
    clearNativeStub();
    vi.unstubAllEnvs();
    rmSync(directory, { recursive: true, force: true });
  });

  function client(): RepositoryClient {
    return new RepositoryClient({ apiKey: "key", apiUrl: "https://api.example" });
  }

  function calls(): Invocation[] {
    return existsSync(log)
      ? readFileSync(log, "utf8").trim().split("\n").map((line) => JSON.parse(line))
      : [];
  }

  function commands(): Invocation[] {
    return calls().filter((call) => !call.args.includes("--help"));
  }

  it("mounts writable by default and keeps the workspace when unmounting", async () => {
    const mount = await client().mount("monorepo:feature/auth//services/auth", "./code");
    expect(mount).toBeInstanceOf(RepositoryMount);
    expect(mount.target).toBe("monorepo:feature/auth//services/auth");
    expect(mount.path).toBe(resolve("./code"));
    expect(mount.readOnly).toBe(false);
    await mount.unmount();
    expect(commands().map((call) => call.args)).toEqual([
      ["git", "mount", "--", mount.target, mount.path],
      ["git", "unmount", "--", mount.path],
    ]);
    expect(calls().filter((call) => call.args.includes("--help"))).toHaveLength(1);
    expect(calls()[0].args).toEqual(["git", "mount", "--help"]);
  });

  it.each([
    ["refs/tags/v2", { readOnly: true }, "--ro"],
    ["a".repeat(40), { readOnly: true }, "--ro"],
    ["main", { publish: true }, "--publish"],
    ["main", { workspace: "-workspace" }, "--workspace=-workspace"],
  ])("supports %s with %j", async (ref, options, flag) => {
    const mount = await client().mount(`repo:${ref}`, "/tmp/code", options);
    expect(mount.readOnly).toBe(options.readOnly ?? false);
    expect(commands()[0].args).toEqual(["git", "mount", flag, "--", `repo:${ref}`, "/tmp/code"]);
  });

  it.each([
    { readOnly: true, publish: true },
    { readOnly: true, workspace: "ws" },
    { publish: true, workspace: "ws" },
  ])("rejects conflicting mount options before starting the CLI: %j", async (options) => {
    await expect(client().mount("repo", "/tmp/code", options)).rejects.toThrow(/mutually exclusive/);
    expect(calls()).toEqual([]);
  });

  it("rejects missing targets, paths, and workspace IDs without invoking the CLI", async () => {
    const repos = client();
    await expect(repos.mount("", "/tmp/code")).rejects.toThrow(TypeError);
    await expect(repos.mount("repo", "")).rejects.toThrow(TypeError);
    await expect(repos.mount("repo", "/tmp/code", { workspace: "" })).rejects.toThrow(TypeError);
    await expect(repos.unmount("")).rejects.toThrow(TypeError);
    await expect(repos.mountStatus("")).rejects.toThrow(TypeError);
    expect(calls()).toEqual([]);
  });

  it.each(["constructor", "forCloud", "fromEnv"])("forwards the selected CLI credentials and scope through %s", async (factory) => {
    const options = {
      apiKey: "explicit-key",
      apiUrl: "https://explicit.example",
      organizationId: "explicit-org",
      projectId: "explicit-project",
    };
    const repos = factory === "constructor"
      ? new RepositoryClient(options)
      : factory === "forCloud"
        ? RepositoryClient.forCloud(options)
        : await RepositoryClient.fromEnv();
    await repos.mount("repo", "/tmp/code");
    const prefix = factory === "fromEnv" ? "ambient" : "explicit";
    expect(commands()[0].env).toMatchObject({
      TENSORLAKE_API_KEY: `${prefix}-key`,
      TENSORLAKE_API_URL: `https://${prefix}.example`,
      TENSORLAKE_ORGANIZATION_ID: `${prefix}-org`,
      TENSORLAKE_PROJECT_ID: `${prefix}-project`,
    });
  });

  it("preserves option-like and shell-like values across the complete workspace workflow", async () => {
    const path = join(directory, "code with spaces;$(echo oops)");
    const mount = await client().mount("--publish", path);
    await mount.snapshot('-message "quoted";$(echo oops)\nsecond line');
    await mount.sync();
    await mount.sync("--discard");
    await mount.rebase("-base", { failOnConflict: true });
    await mount.promote("-branch", { merge: true });
    await mount.prefetch();
    await mount.unmount({ deleteWorkspace: true, discard: true });
    expect(commands().map((call) => call.args)).toEqual([
      ["git", "mount", "--", "--publish", path],
      ["git", "snapshot", '--message=-message "quoted";$(echo oops)\nsecond line', "--", path],
      ["git", "sync", "--", path],
      ["git", "sync", "--", path, "--discard"],
      ["git", "rebase", "--fail-on-conflict", "--", path, "-base"],
      ["git", "promote", "--merge", "--", path, "-branch"],
      ["git", "prefetch", "--", path],
      ["git", "unmount", "--delete", "--discard", "--", path],
    ]);
  });

  it("keeps snapshots, rebases, and promotions at their CLI defaults", async () => {
    const mount = await client().mount("repo", "/tmp/code");
    await mount.snapshot();
    await mount.rebase("main");
    await mount.promote("main");
    expect(commands().slice(1).map((call) => call.args)).toEqual([
      ["git", "snapshot", "--", mount.path],
      ["git", "rebase", "--", mount.path, "main"],
      ["git", "promote", "--", mount.path, "main"],
    ]);
  });

  it("maps the daemon status contract and preserves unknown fields", async () => {
    const raw = {
      surface: "git_repository",
      repository: "repo",
      reference: "refs/heads/main",
      workspace: "ws-1",
      mountpoint: "/tmp/code",
      state: "workspace_clean",
      local_changes: false,
      next_actions: ["snapshot"],
      daemon: { future_field: true },
    };
    writeFileSync(config, JSON.stringify({ status: JSON.stringify(raw) }));
    const mount = await client().mount("repo", "/tmp/code");
    expect(await mount.status()).toEqual({
      path: "/tmp/code",
      repository: "repo",
      reference: "refs/heads/main",
      workspaceId: "ws-1",
      state: "workspace_clean",
      localChanges: false,
      raw,
    });
    expect(commands().at(-1)?.args).toEqual(["git", "status", "--json", "--", "/tmp/code"]);
  });

  it("supports status and teardown of mounts created outside the SDK", async () => {
    const repos = client();
    expect(await repos.mountStatus()).toMatchObject({ workspaceId: null, localChanges: null, raw: {} });
    await repos.unmount("/tmp/existing");
    expect(commands().map((call) => call.args)).toEqual([
      ["git", "status", "--json"],
      ["git", "unmount", "--", "/tmp/existing"],
    ]);
  });

  it.each(["not json", "null", "[]", "true"])("rejects malformed status output: %s", async (status) => {
    writeFileSync(config, JSON.stringify({ status }));
    await expect(client().mountStatus("/tmp/code")).rejects.toThrow(/tl git status --json.*invalid JSON/);
  });

  it("reports mount failures with CLI stderr", async () => {
    writeFileSync(config, JSON.stringify({ error: "workspace already has a live owner" }));
    const error = await client().mount("repo", "/tmp/code").catch((error) => error);
    expect(error).toBeInstanceOf(MountError);
    expect(error.message).toContain("workspace already has a live owner");
  });

  it("distinguishes a missing CLI from an installation requiring an upgrade", async () => {
    vi.stubEnv("TENSORLAKE_CLI", join(directory, "missing"));
    const missing = await client().mount("repo", "/tmp/code").catch((error) => error);
    expect(missing).toBeInstanceOf(CliNotFoundError);
    expect(missing.message).toContain("`tl git mount`");
    expect(missing.message).toContain("not found on PATH");
    expect(missing.message).not.toContain("upgrade required");
    vi.stubEnv("TENSORLAKE_CLI", binary);
    writeFileSync(config, JSON.stringify({ probeFailure: true }));
    await expect(client().mount("repo", "/tmp/code")).rejects.toThrow(/upgrade required/);
  });

  it("falls back to a supported CLI on PATH", async () => {
    vi.stubEnv("TENSORLAKE_CLI", join(directory, "missing"));
    const pathBinary = join(directory, "tl");
    writeFileSync(pathBinary, readFileSync(binary));
    chmodSync(pathBinary, 0o755);
    await client().mount("repo", "/tmp/code");
    expect(commands()).toHaveLength(1);
  });

  it("keeps filesystem mounts working through the shared runner", async () => {
    const fs = new FilesystemClient({ apiKey: "fs-key", apiUrl: "https://fs.example" });
    const mount = await fs.mount("--discard", "/tmp/fs code");
    await mount.snapshot("-snapshot");
    await mount.status();
    await mount.unmount();
    expect(calls()[0].args).toEqual(["fs", "--help"]);
    expect(commands().map((call) => call.args)).toEqual([
      ["fs", "mount", "--", "--discard", "/tmp/fs code"],
      ["fs", "snapshot", "--message=-snapshot", "--", "/tmp/fs code"],
      ["fs", "status", "--json", "--", "/tmp/fs code"],
      ["fs", "unmount", "--", "/tmp/fs code"],
    ]);
    expect(commands()[0].env.TENSORLAKE_API_KEY).toBe("fs-key");
    expect(commands()[0].env.TENSORLAKE_API_URL).toBe("https://fs.example");
  });
});
