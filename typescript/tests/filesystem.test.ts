/**
 * Network-free unit tests for the filesystem SDK.
 *
 * The wire protocol (chunking, ingest, commit jobs, retries) lives in the
 * Rust cloud-sdk core and is tested there. These tests pin the TypeScript
 * wrapper: how native results map to models, how native errors translate
 * into the filesystem error hierarchy, and the idempotency-key discipline on
 * writes. The native client is injected through the `nativeClient` test seam.
 */

import { describe, expect, it } from "vitest";
import { FilesystemClient } from "../src/filesystem.js";
import {
  FileNotFoundInFilesystemError,
  FilesystemAPIError,
  FilesystemError,
  FilesystemNotFoundError,
  fileEntryFromWire,
  mountStatusFromRaw,
  trimSlashes,
} from "../src/filesystem-models.js";
import type {
  NativeRepositoryClient,
  TracedBytes,
  TracedJson,
} from "../src/native-sandbox.js";

const PROJECT = "proj_test";
const COMMIT = "c".repeat(40);

type NativeErrorSpec = { category: string; status: number | null; message: string };

function nativeError(spec: NativeErrorSpec): Error {
  return new Error(JSON.stringify(spec));
}

function traced(json: unknown): TracedJson {
  return { traceId: "trace-1", json: JSON.stringify(json) };
}

interface StubOptions {
  meta?: Record<string, unknown>;
  repos?: Array<Record<string, unknown>>;
  ref?: Record<string, unknown>;
  entries?: Array<Record<string, unknown>>;
  fileBytes?: Buffer;
  pushReport?: Record<string, unknown>;
  errors?: Record<string, NativeErrorSpec>;
  createBranch?: string;
}

/** Scripted stand-in for the napi NativeRepositoryClient filesystem surface. */
class StubNative {
  readonly calls: Array<{ method: string; args: unknown[] }> = [];
  private readonly options: StubOptions;

  constructor(options: StubOptions = {}) {
    this.options = options;
  }

  private record(method: string, args: unknown[]): void {
    this.calls.push({ method, args });
    const failure = this.options.errors?.[method];
    if (failure) throw nativeError(failure);
  }

  async createFilesystem(name: string): Promise<string> {
    this.record("createFilesystem", [name]);
    return JSON.stringify({
      trace_id: "trace-1",
      // Non-"main" simulates the binding adopting a pre-existing filesystem
      // on a lost-response retry.
      default_branch: this.options.createBranch ?? "main",
    });
  }

  async listFilesystems(): Promise<TracedJson> {
    this.record("listFilesystems", []);
    return traced({
      project: PROJECT,
      repos: this.options.repos ?? [],
      next_after: null,
    });
  }

  async filesystemMeta(name: string): Promise<TracedJson> {
    this.record("filesystemMeta", [name]);
    return traced(
      this.options.meta ?? {
        name: "my-fs",
        full_name: `${PROJECT}/my-fs`,
        default_branch: "main",
        status: "active",
        kind: "filesystem",
      },
    );
  }

  async deleteFilesystem(name: string): Promise<string> {
    this.record("deleteFilesystem", [name]);
    return "trace-2";
  }

  async filesystemRefStatus(name: string, refspec: string): Promise<TracedJson> {
    this.record("filesystemRefStatus", [name, refspec]);
    return traced(
      this.options.ref ?? {
        ref_name: "refs/heads/main",
        oid: COMMIT,
        resolved_commit: COMMIT,
        generation: 3,
      },
    );
  }

  async readFilesystemFile(
    name: string,
    path: string,
    version: string,
  ): Promise<TracedBytes> {
    this.record("readFilesystemFile", [name, path, version]);
    return { traceId: "trace-1", data: this.options.fileBytes ?? Buffer.alloc(0) };
  }

  async listFilesystemTree(
    name: string,
    dirPath: string,
    version: string,
  ): Promise<TracedJson> {
    this.record("listFilesystemTree", [name, dirPath, version]);
    return traced({ entries: this.options.entries ?? [] });
  }

  async pushFilesystemFiles(
    name: string,
    files: Array<{ path: string; content: Buffer }>,
    deletes: string[],
    message: string,
    branch: string,
    idempotencyKey?: string | null,
  ): Promise<TracedJson> {
    this.record("pushFilesystemFiles", [
      name,
      files,
      deletes,
      message,
      branch,
      idempotencyKey,
    ]);
    return traced(
      this.options.pushReport ?? {
        commit: COMMIT,
        tree: "t".repeat(40),
        ref_name: "refs/heads/main",
        created: true,
      },
    );
  }
}

function clientWith(stub: StubNative): FilesystemClient {
  return new FilesystemClient({
    apiKey: "test-key",
    apiUrl: "https://api.tensorlake.ai",
    organizationId: "org_test",
    projectId: PROJECT,
    nativeClient: stub as unknown as NativeRepositoryClient,
  });
}

describe("filesystem models", () => {
  it("maps tree entries to FileEntry", () => {
    const dir = fileEntryFromWire({ name: "d", oid: "x", mode: 0o40000 }, "docs");
    expect(dir.isDir).toBe(true);
    expect(dir.path).toBe("docs/d");
    const file = fileEntryFromWire({ name: "f", mode: 0o100644, size: 3 }, "");
    expect(file.isDir).toBe(false);
    expect(file.path).toBe("f");
  });

  it("trims slashes in linear time with Python strip('/') semantics", () => {
    expect(trimSlashes("/a/b/")).toBe("a/b");
    expect(trimSlashes("///a//b///")).toBe("a//b");
    expect(trimSlashes("////")).toBe("");
    expect(trimSlashes("")).toBe("");
    expect(trimSlashes("a")).toBe("a");
  });

  it("maps mount status with Python-parity semantics", () => {
    const status = mountStatusFromRaw(
      {
        path: "",
        mount_path: "/mnt/x",
        mounted: null,
        active: true,
        filesystem: "",
      },
      "/ignored",
    );
    // Empty path falls through to mount_path; a present-but-null "mounted"
    // means not mounted; empty filesystem becomes null.
    expect(status.path).toBe("/mnt/x");
    expect(status.mounted).toBe(false);
    expect(status.filesystem).toBeNull();

    const defaulted = mountStatusFromRaw({}, "/mnt/y");
    expect(defaulted.path).toBe("/mnt/y");
    expect(defaulted.mounted).toBe(true);
  });
});

describe("FilesystemClient", () => {
  it("runs the lifecycle against the native core", async () => {
    const stub = new StubNative({
      repos: [
        {
          name: "a",
          full_name: `${PROJECT}/a`,
          default_branch: "main",
          status: "active",
          kind: "filesystem",
        },
      ],
    });
    const client = clientWith(stub);
    const fs = await client.create("my-fs");
    expect(fs.name).toBe("my-fs");
    const infos = await client.list();
    expect(infos.map((i) => i.name)).toEqual(["a"]);
    expect(infos[0].fullName).toBe(`${PROJECT}/a`);
    await client.delete("my-fs");
    expect(stub.calls.map((c) => c.method)).toEqual([
      "createFilesystem",
      "listFilesystems",
      "deleteFilesystem",
    ]);
  });

  it("maps a 404 meta to FilesystemNotFoundError", async () => {
    const stub = new StubNative({
      errors: {
        filesystemMeta: { category: "remote_api", status: 404, message: "no repo" },
      },
    });
    await expect(clientWith(stub).get("nope")).rejects.toThrow(
      FilesystemNotFoundError,
    );
  });

  it("rejects a non-filesystem repo kind", async () => {
    const stub = new StubNative({
      meta: { name: "code", default_branch: "main", kind: "repository" },
    });
    await expect(clientWith(stub).get("code")).rejects.toThrow(
      FilesystemNotFoundError,
    );
  });

  it("maps push reports to snapshots and sets a stable idempotency key", async () => {
    const stub = new StubNative();
    const client = clientWith(stub);
    const fs = await client.create("my-fs");
    const snapshot = await fs.writeFiles(
      new Map<string, Uint8Array | string>([
        ["a.txt", "hello"],
        ["b.bin", new Uint8Array([0, 1])],
      ]),
      undefined,
      ["old.txt"],
    );
    expect(snapshot.commit).toBe(COMMIT);
    expect(snapshot.created).toBe(true);

    const push = stub.calls.at(-1)!;
    expect(push.method).toBe("pushFilesystemFiles");
    const [name, files, deletes, , branch, idempotencyKey] = push.args as [
      string,
      Array<{ path: string; content: Buffer }>,
      string[],
      string,
      string,
      string,
    ];
    expect(name).toBe("my-fs");
    expect(files.map((f) => f.path)).toEqual(["a.txt", "b.bin"]);
    expect(files[0].content.toString()).toBe("hello");
    expect([...files[1].content]).toEqual([0, 1]);
    expect(deletes).toEqual(["old.txt"]);
    expect(branch).toBe("main");
    // A fresh stable key per logical write (the Rust core reuses it across
    // its retries so a lost response cannot double-commit).
    expect(idempotencyKey).toMatch(/^[0-9a-f]{32}$/);
  });

  it("rejects an empty write", async () => {
    const client = clientWith(new StubNative());
    const fs = await client.create("my-fs");
    await expect(fs.writeFiles(new Map())).rejects.toThrow(FilesystemError);
  });

  it("reads files and maps missing files", async () => {
    const stub = new StubNative({ fileBytes: Buffer.from("content") });
    const client = clientWith(stub);
    const fs = await client.create("my-fs");
    expect(Buffer.from(await fs.readFile("docs/a.txt")).toString()).toBe(
      "content",
    );
    expect(stub.calls.at(-1)!.args).toEqual(["my-fs", "docs/a.txt", "main"]);

    const failing = new StubNative({
      errors: {
        readFilesystemFile: {
          category: "remote_api",
          status: 404,
          message: "not found",
        },
      },
    });
    const failingFs = await clientWith(failing).create("my-fs");
    await expect(failingFs.readFile("missing.txt")).rejects.toThrow(
      FileNotFoundInFilesystemError,
    );
    await expect(failingFs.readFile("//")).rejects.toThrow(FilesystemError);
  });

  it("lists files with paths and modes", async () => {
    const stub = new StubNative({
      entries: [
        { name: "sub", oid: "x", mode: 0o40000 },
        { name: "a.txt", oid: "y", mode: 0o100644, size: 3 },
      ],
    });
    const client = clientWith(stub);
    const fs = await client.create("my-fs");
    const entries = await fs.listFiles("docs");
    expect(entries.map((e) => e.path)).toEqual(["docs/sub", "docs/a.txt"]);
    expect(entries[0].isDir).toBe(true);
    expect(entries[1].size).toBe(3);
  });

  it("maps status, including an empty filesystem", async () => {
    const client = clientWith(new StubNative());
    const fs = await client.create("my-fs");
    const status = await fs.status();
    expect(status.headCommit).toBe(COMMIT);
    expect(status.generation).toBe(3);

    const empty = new StubNative({
      ref: { ref_name: "refs/heads/main", oid: null, resolved_commit: null, generation: 0 },
    });
    const emptyFs = await clientWith(empty).create("my-fs");
    expect((await emptyFs.status()).headCommit).toBeNull();
    await expect(emptyFs.snapshot("nothing yet")).rejects.toThrow(
      FilesystemError,
    );
  });

  it("swallows only 404 from ref-status", async () => {
    const notFound = new StubNative({
      errors: {
        filesystemRefStatus: { category: "remote_api", status: 404, message: "no ref" },
      },
    });
    const fs = await clientWith(notFound).create("my-fs");
    expect((await fs.status()).headCommit).toBeNull();

    const unavailable = new StubNative({
      errors: {
        filesystemRefStatus: {
          category: "remote_api",
          status: 503,
          message: "unavailable",
        },
      },
    });
    const failingFs = await clientWith(unavailable).create("my-fs");
    await expect(failingFs.status()).rejects.toThrow(FilesystemAPIError);
  });

  it("pins the current head as a snapshot", async () => {
    const fs = await clientWith(new StubNative()).create("my-fs");
    const snapshot = await fs.snapshot("pin");
    expect(snapshot.commit).toBe(COMMIT);
    expect(snapshot.created).toBe(false);
  });

  it("seeds the branch reported by the create binding", async () => {
    const stub = new StubNative({ createBranch: "trunk" });
    const fs = await clientWith(stub).create("my-fs");
    await fs.writeFile("a.txt", "x");
    // (name, files, deletes, message, branch, idempotencyKey)
    expect(stub.calls.at(-1)!.args[4]).toBe("trunk");
  });

  it("follows a non-main default branch for writes and reads", async () => {
    const stub = new StubNative({
      meta: {
        name: "my-fs",
        default_branch: "trunk",
        status: "active",
        kind: "filesystem",
      },
    });
    const client = clientWith(stub);
    const fs = await client.get("my-fs");
    await fs.writeFile("a.txt", "x");
    // (name, files, deletes, message, branch, idempotencyKey)
    expect(stub.calls.at(-1)!.args[4]).toBe("trunk");
    await fs.readFile("a.txt");
    // (name, path, version)
    expect(stub.calls.at(-1)!.args[2]).toBe("trunk");
    await fs.listFiles();
    expect(stub.calls.at(-1)!.args[2]).toBe("trunk");
    // An explicit version always wins over the default branch.
    await fs.readFile("a.txt", "c".repeat(40));
    expect(stub.calls.at(-1)!.args[2]).toBe("c".repeat(40));
  });

  it("readText raises on non-UTF-8 content instead of mangling it", async () => {
    const stub = new StubNative({ fileBytes: Buffer.from([0xff, 0xfe]) });
    const fs = await clientWith(stub).create("my-fs");
    await expect(fs.readText("a.bin")).rejects.toThrow();
  });

  it("rejects empty and non-string names with the right error types", async () => {
    const client = clientWith(new StubNative());
    await expect(client.create("")).rejects.toThrow(FilesystemError);
    await expect(client.create("")).rejects.toThrow(/must not be empty/);
    await expect(
      client.create(123 as unknown as string),
    ).rejects.toThrow(TypeError);
  });

  it("substitutes the default message for an explicit empty string", async () => {
    const stub = new StubNative();
    const fs = await clientWith(stub).create("my-fs");
    const snapshot = await fs.writeFile("a.txt", "x", "");
    const message = stub.calls.at(-1)!.args[3];
    expect(message).toBe("write 1 file(s) via SDK");
    expect(snapshot.message).toBe("write 1 file(s) via SDK");
  });

  it("maps connection-category errors to FilesystemError, not APIError", async () => {
    const stub = new StubNative({
      errors: {
        readFilesystemFile: {
          category: "connection",
          // The napi layer fabricates a gateway status for connect failures;
          // it must not surface as a server FilesystemAPIError.
          status: 503,
          message: "connect ECONNREFUSED",
        },
      },
    });
    const fs = await clientWith(stub).create("my-fs");
    const failure = await fs.readFile("a.txt").catch((e) => e);
    expect(failure).toBeInstanceOf(FilesystemError);
    expect(failure).not.toBeInstanceOf(FilesystemAPIError);
  });

  it("translates statusless native errors to FilesystemError", async () => {
    const stub = new StubNative({
      errors: {
        pushFilesystemFiles: {
          category: "internal",
          status: null,
          message: "commit job failed (conflict): boom",
        },
      },
    });
    const fs = await clientWith(stub).create("my-fs");
    const failure = await fs.writeFile("a.txt", "x").catch((e) => e);
    expect(failure).toBeInstanceOf(FilesystemError);
    expect(failure).not.toBeInstanceOf(FilesystemAPIError);
    expect(String(failure.message)).toContain("commit job failed");
  });
});
