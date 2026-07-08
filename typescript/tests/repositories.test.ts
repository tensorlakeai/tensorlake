import { afterEach, describe, expect, it, vi } from "vitest";
import { RepositoryClient } from "../src/repositories.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

const repositoryEnvKeys = [
  "TENSORLAKE_API_URL",
  "TENSORLAKE_API_KEY",
  "TENSORLAKE_PAT",
  "TENSORLAKE_PROJECT_ID",
] as const;

function saveRepositoryEnv(): Record<string, string | undefined> {
  return Object.fromEntries(
    repositoryEnvKeys.map((key) => [key, process.env[key]]),
  );
}

function restoreRepositoryEnv(saved: Record<string, string | undefined>): void {
  for (const key of repositoryEnvKeys) {
    const value = saved[key];
    if (value == null) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
}

describe("RepositoryClient", () => {
  afterEach(() => {
    clearNativeStub();
    vi.restoreAllMocks();
  });

  it("constructs the native client with project context", () => {
    const stub = installNativeStub();
    const client = new RepositoryClient({
      apiUrl: "http://localhost:8900",
      apiKey: "k",
      organizationId: "org-1",
      projectId: "project-1",
    });

    expect(stub.repositoryCtorArgs).toEqual([
      "http://localhost:8900",
      "k",
      "org-1",
      "project-1",
      null,
      300,
    ]);
    client.close();
  });

  it("constructs from env with API key project context", async () => {
    const saved = saveRepositoryEnv();
    try {
      process.env.TENSORLAKE_API_URL = "http://localhost:8900";
      process.env.TENSORLAKE_API_KEY = "k";
      process.env.TENSORLAKE_PROJECT_ID = "project-1";
      delete process.env.TENSORLAKE_PAT;

      const stub = installNativeStub();
      const client = await RepositoryClient.fromEnv();

      expect(stub.repositoryCtorArgs).toEqual([
        "http://localhost:8900",
        "k",
        null,
        "project-1",
        null,
        300,
      ]);
      client.close();
    } finally {
      restoreRepositoryEnv(saved);
    }
  });

  it("rejects PAT-only env credentials", async () => {
    const saved = saveRepositoryEnv();
    try {
      process.env.TENSORLAKE_API_KEY = "";
      process.env.TENSORLAKE_PAT = "pat";
      process.env.TENSORLAKE_PROJECT_ID = "";

      await expect(RepositoryClient.fromEnv()).rejects.toThrow(
        "Personal access tokens are CLI-only",
      );
    } finally {
      restoreRepositoryEnv(saved);
    }
  });

  it("creates repositories and returns the clone URL", async () => {
    const createRepo = vi.fn(async (repo: string, defaultBranch: string | null) => {
      expect(repo).toBe("linux");
      expect(defaultBranch).toBe("main");
      return {
        traceId: "tr-create",
        json: JSON.stringify({
          repo: "linux",
          url: "https://git.tensorlake.ai/project-1/linux",
        }),
      };
    });
    installNativeStub({ repository: { createRepo } });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const created = await client.create("linux", { defaultBranch: "main" });

    expect(created.repo).toBe("linux");
    expect(created.url).toBe("https://git.tensorlake.ai/project-1/linux");
    expect(created.traceId).toBe("tr-create");
    expect(createRepo).toHaveBeenCalledOnce();
  });

  it("lists repositories as camel-cased models", async () => {
    installNativeStub({
      repository: {
        listRepos: vi.fn(async () => ({
          traceId: "tr-list",
          json: JSON.stringify({
            project: "project-1",
            repos: [
              {
                name: "linux",
                full_name: "project-1/linux",
                default_branch: "main",
                status: "active",
              },
            ],
          }),
        })),
      },
    });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const repos = await client.list();

    expect(repos.traceId).toBe("tr-list");
    expect(repos).toHaveLength(1);
    expect(repos[0].defaultBranch).toBe("main");
    expect(repos[0].fullName).toBe("project-1/linux");
  });

  it("returns repository info with branches and refs", async () => {
    installNativeStub({
      repository: {
        repoInfo: vi.fn(async () => ({
          traceId: "tr-info",
          json: JSON.stringify({
            repo: "linux",
            url: "https://git.tensorlake.ai/project-1/linux",
            branches: [{ name: "main", ref_name: "refs/heads/main", oid: "abc" }],
            refs: [{ name: "HEAD", oid: "abc" }],
          }),
        })),
      },
    });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const info = await client.info("linux");

    expect(info.traceId).toBe("tr-info");
    expect(info.branches[0].refName).toBe("refs/heads/main");
    expect(info.refs[0].name).toBe("HEAD");
  });

  it("gets git credentials", async () => {
    const gitCredential = vi.fn(async (repo: string | null) => {
      expect(repo).toBe("linux");
      return JSON.stringify({
        token: "tok",
        tokenType: "bearer",
        expiresAt: "2026-07-08T00:00:00Z",
        gitUsername: "t",
        repoPattern: "linux",
        scopes: ["git:read"],
      });
    });
    installNativeStub({ repository: { gitCredential } });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const credential = await client.credential("linux");

    expect(credential.gitUsername).toBe("t");
    expect(credential.tokenType).toBe("bearer");
    expect(credential.expiresAt).toBe("2026-07-08T00:00:00Z");
    expect(credential.scopes).toEqual(["git:read"]);
  });

  it("pushes a local worktree through the native core", async () => {
    const pushWorktree = vi.fn(
      async (
        repo: string,
        root: string,
        branch: string,
        message: string,
        expectOid: string | null,
      ) => {
        expect([repo, root, branch, message, expectOid]).toEqual([
          "linux",
          "/tmp/linux",
          "feature",
          "sync",
          "abc",
        ]);
        return {
          traceId: "tr-push",
          json: JSON.stringify({
            commit: "def",
            tree: "tree",
            ref_name: "refs/heads/feature",
            created: false,
            files: 2,
            bytes_total: 12,
            chunks_total: 1,
            chunks_uploaded: 1,
            bytes_uploaded: 12,
            file_blob_oids: [["README.md", "oid"]],
          }),
        };
      },
    );
    installNativeStub({ repository: { pushWorktree } });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const report = await client.pushWorktree("linux", {
      path: "/tmp/linux",
      branch: "feature",
      message: "sync",
      expectOid: "abc",
    });

    expect(report.traceId).toBe("tr-push");
    expect(report.refName).toBe("refs/heads/feature");
    expect(report.bytesUploaded).toBe(12);
  });

  it("merges branches through the native core", async () => {
    const mergeRepo = vi.fn(
      async (
        repo: string,
        ours: string,
        theirs: string,
        preflight: boolean,
        deep: boolean,
        materialize: boolean,
        message: string | null,
        base: string | null,
      ) => {
        expect([
          repo,
          ours,
          theirs,
          preflight,
          deep,
          materialize,
          message,
          base,
        ]).toEqual([
          "linux",
          "main",
          "feature",
          true,
          true,
          false,
          "merge feature",
          "base-oid",
        ]);
        return {
          traceId: "tr-merge",
          json: JSON.stringify({
            ours: "ours-oid",
            theirs: "theirs-oid",
            merge_base: "base-oid",
            clean: false,
            fast_forward: false,
            already_merged: false,
            changed_paths: 2,
            conflicts: [
              {
                path: "src/main.rs",
                kind: "content",
                potential: true,
                ours: { mode: 33188, oid: "ours-blob" },
                base: null,
                theirs: { mode: 33188, oid: "theirs-blob" },
              },
            ],
            stats: {
              trees_read: 3,
              entries_compared: 4,
              blobs_merged: 1,
              wall_ms: 2.5,
            },
            commit: null,
            fast_forwarded: false,
          }),
        };
      },
    );
    installNativeStub({ repository: { mergeRepo } });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const report = await client.merge("linux", "main", "feature", {
      preflight: true,
      deep: true,
      message: "merge feature",
      base: "base-oid",
    });

    expect(report.traceId).toBe("tr-merge");
    expect(report.mergeBase).toBe("base-oid");
    expect(report.changedPaths).toBe(2);
    expect(report.conflicts[0].potential).toBe(true);
    expect(report.conflicts[0].base).toBeUndefined();
    expect(report.stats.entriesCompared).toBe(4);
  });

  it("returns materialized merge conflict records", async () => {
    const commitConflicts = vi.fn(async (repo: string, commit: string) => {
      expect([repo, commit]).toEqual(["linux", "merge-oid"]);
      return {
        traceId: "tr-conflicts",
        json: JSON.stringify({
          version: 1,
          ours_commit: "ours-oid",
          theirs_commit: "theirs-oid",
          base_commit: null,
          paths: [
            {
              path: "src/main.rs",
              kind: "content",
              terms: [null, { mode: 33188, oid: "base-blob" }, null],
            },
          ],
          truncated_paths: 0,
        }),
      };
    });
    installNativeStub({ repository: { commitConflicts } });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });
    const record = await client.commitConflicts("linux", "merge-oid");

    expect(record?.traceId).toBe("tr-conflicts");
    expect(record?.oursCommit).toBe("ours-oid");
    expect(record?.baseCommit).toBeUndefined();
    expect(record?.paths[0].terms[0]).toBeNull();
    expect(record?.paths[0].terms[1]?.oid).toBe("base-blob");
  });

  it("returns null when a commit has no conflict record", async () => {
    installNativeStub({
      repository: {
        commitConflicts: vi.fn(async () => ({ traceId: "tr-none", json: "null" })),
      },
    });

    const client = new RepositoryClient({ apiKey: "k", projectId: "project-1" });

    await expect(client.commitConflicts("linux", "clean-oid")).resolves.toBeNull();
  });
});
