import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { Sandbox } from "../src/sandbox.js";
import { SandboxClient } from "../src/client.js";
import { ProcessStatus, SandboxStatus } from "../src/models.js";
import { SandboxError } from "../src/errors.js";

vi.mock("undici", async (importOriginal) => {
  const actual = await importOriginal<typeof import("undici")>();
  return { ...actual, fetch: vi.fn() };
});

describe("Sandbox", () => {

  afterEach(() => {
    vi.mocked(undici.fetch).mockReset();
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    vi.mocked(undici.fetch).mockImplementation(handler as typeof undici.fetch);
  }

  function makeSandbox(id = "sbx-test"): Sandbox {
    return new Sandbox({
      sandboxId: id,
      proxyUrl: "http://localhost:9443",
    });
  }

  describe("constructor", () => {
    it("sets sandboxId", () => {
      const sbx = makeSandbox("sbx-abc");
      expect(sbx.sandboxId).toBe("sbx-abc");
      sbx.close();
    });
  });

  describe("run", () => {
    /** Build an SSE-formatted response body from an array of JSON events. */
    function sseResponse(events: unknown[]): Response {
      const body = events.map((e) => `data: ${JSON.stringify(e)}\n\n`).join("");
      return new Response(body, {
        status: 200,
        headers: { "Content-Type": "text/event-stream" },
      });
    }

    it("runs a command and returns result", async () => {
      mockFetch((url, init) => {
        if (url.includes("/api/v1/processes/run") && init?.method === "POST") {
          return sseResponse([
            { pid: 42, started_at: 1700000000 },
            { line: "hello", timestamp: 1700000000.1, stream: "stdout" },
            { exit_code: 0 },
          ]);
        }
        return new Response("", { status: 404 });
      });

      const sbx = makeSandbox();
      const result = await sbx.run("echo", { args: ["hello"] });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toBe("hello");
      expect(result.stderr).toBe("");
      sbx.close();
    });
  });

  describe("listProcesses", () => {
    it("returns an array with traceId", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            processes: [
              {
                pid: 42,
                status: "running",
                command: "bash",
                args: [],
                stdin_writable: false,
                started_at: 1700000000,
              },
            ],
          }),
          {
            status: 200,
            headers: { traceparent: "00-aabbccdd00112233aabbccdd00112233-cafebabe12345678-01" },
          },
        ),
      );

      const sbx = makeSandbox();
      const procs = await sbx.listProcesses();

      expect(Array.isArray(procs)).toBe(true);
      expect(procs).toHaveLength(1);
      expect(procs[0].pid).toBe(42);
      expect(typeof procs.traceId).toBe("string");
      expect(procs.traceId.length).toBeGreaterThan(0);
      sbx.close();
    });

    it("list comprehension pattern works on result", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            processes: [
              { pid: 1, status: "running", command: "bash", args: [], stdin_writable: false, started_at: 1700000000 },
              { pid: 2, status: "exited", command: "ls", args: [], stdin_writable: false, started_at: 1700000001 },
            ],
          }),
          { status: 200 },
        ),
      );

      const sbx = makeSandbox();
      const procs = await sbx.listProcesses();
      const pids = procs.map((p) => p.pid);

      expect(pids).toEqual([1, 2]);
      sbx.close();
    });
  });

  describe("startProcess", () => {
    it("sends correct payload", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.command).toBe("bash");
        expect(body.args).toEqual(["-c", "ls"]);
        expect(body.working_dir).toBe("/tmp");
        return new Response(
          JSON.stringify({
            pid: 1,
            status: "running",
            command: "bash",
            args: ["-c", "ls"],
            stdin_writable: false,
            started_at: 1700000000,
          }),
          { status: 200 },
        );
      });

      const sbx = makeSandbox();
      const proc = await sbx.startProcess("bash", {
        args: ["-c", "ls"],
        workingDir: "/tmp",
      });
      expect(proc.pid).toBe(1);
      expect(proc.status).toBe(ProcessStatus.RUNNING);
      sbx.close();
    });
  });

  describe("file operations", () => {
    it("reads a file", async () => {
      mockFetch((url) => {
        expect(url).toContain("/api/v1/files?path=%2Ftmp%2Ftest.txt");
        return new Response(new Uint8Array([72, 101, 108, 108, 111]), {
          status: 200,
        });
      });

      const sbx = makeSandbox();
      const data = await sbx.readFile("/tmp/test.txt");
      expect(new TextDecoder().decode(data)).toBe("Hello");
      sbx.close();
    });

    it("writes a file", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/api/v1/files?path=%2Ftmp%2Fout.txt");
        expect(init?.method).toBe("PUT");
        return new Response("", { status: 200 });
      });

      const sbx = makeSandbox();
      await sbx.writeFile(
        "/tmp/out.txt",
        new TextEncoder().encode("content"),
      );
      sbx.close();
    });

    it("lists a directory", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            path: "/tmp",
            entries: [
              { name: "file.txt", is_dir: false, size: 100, modified_at: 1700000000 },
              { name: "subdir", is_dir: true, size: null, modified_at: null },
            ],
          }),
          { status: 200 },
        ),
      );

      const sbx = makeSandbox();
      const listing = await sbx.listDirectory("/tmp");
      expect(listing.path).toBe("/tmp");
      expect(listing.entries).toHaveLength(2);
      expect(listing.entries[0].name).toBe("file.txt");
      expect(listing.entries[0].isDir).toBe(false);
      expect(listing.entries[1].name).toBe("subdir");
      expect(listing.entries[1].isDir).toBe(true);
      sbx.close();
    });
  });

  describe("health", () => {
    it("returns health status", async () => {
      mockFetch(() =>
        new Response(JSON.stringify({ healthy: true }), { status: 200 }),
      );

      const sbx = makeSandbox();
      const health = await sbx.health();
      expect(health.healthy).toBe(true);
      sbx.close();
    });
  });

  describe("info", () => {
    it("returns daemon info", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            version: "1.0.0",
            uptime_secs: 3600,
            running_processes: 2,
            total_processes: 10,
          }),
          { status: 200 },
        ),
      );

      const sbx = makeSandbox();
      const info = await sbx.info();
      expect(info.version).toBe("1.0.0");
      expect(info.uptimeSecs).toBe(3600);
      expect(info.runningProcesses).toBe(2);
      sbx.close();
    });
  });

  describe("name / status / update", () => {
    function sandboxInfoBody(overrides: Record<string, unknown> = {}) {
      return JSON.stringify({
        id: "sbx-1",
        namespace: "default",
        status: "running",
        resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
        secret_names: [],
        ...overrides,
      });
    }

    it("connect() populates name from server info", async () => {
      mockFetch(() =>
        new Response(sandboxInfoBody({ name: "my-sandbox" }), { status: 200 }),
      );

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBe("my-sandbox");
      sbx.close();
    });

    it("connect() leaves name null when server omits it", async () => {
      mockFetch(() => new Response(sandboxInfoBody(), { status: 200 }));

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBeNull();
      sbx.close();
    });

    it("status() fetches fresh status from the server every call", async () => {
      const responses = [
        sandboxInfoBody({ status: "running" }), // initial GET inside Sandbox.connect()
        sandboxInfoBody({ status: "running" }),
        sandboxInfoBody({ status: "suspended" }),
      ];
      mockFetch(() => new Response(responses.shift()!, { status: 200 }));

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(await sbx.status()).toBe(SandboxStatus.RUNNING);
      expect(await sbx.status()).toBe(SandboxStatus.SUSPENDED);
      sbx.close();
    });

    it("status() throws when no lifecycle client is wired", async () => {
      const sbx = makeSandbox();
      await expect(sbx.status()).rejects.toThrow(SandboxError);
      sbx.close();
    });

    it("update() PATCHes the sandbox and refreshes the local name", async () => {
      let patchBody: Record<string, unknown> | null = null;
      let patchUrl = "";
      mockFetch((url, init) => {
        if (init?.method === "PATCH") {
          patchUrl = url;
          patchBody = JSON.parse(init.body as string);
          return new Response(
            sandboxInfoBody({ name: "renamed", exposed_ports: [8080] }),
            { status: 200 },
          );
        }
        // Initial GET from Sandbox.connect()
        return new Response(sandboxInfoBody({ name: "old-name" }), {
          status: 200,
        });
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBe("old-name");

      const info = await sbx.update({ name: "renamed", exposedPorts: [8080] });

      expect(patchUrl).toContain("/sandboxes/sbx-1");
      expect(patchBody).not.toBeNull();
      expect(patchBody!.name).toBe("renamed");
      expect(patchBody!.exposed_ports).toEqual([8080]);
      expect(info.name).toBe("renamed");
      expect(sbx.name).toBe("renamed");
      sbx.close();
    });

    it("update() throws when no lifecycle client is wired", async () => {
      const sbx = makeSandbox();
      await expect(sbx.update({ name: "x" })).rejects.toThrow(SandboxError);
      sbx.close();
    });

    it("lifecycle calls stay pinned to canonical sandbox ID after renaming a name-connected handle", async () => {
      const calls: string[] = [];
      mockFetch((url, init) => {
        const method = init?.method ?? "GET";
        calls.push(`${method} ${url}`);

        if (method === "PATCH") {
          expect(url).toContain("/sandboxes/sbx-1");
          return new Response(
            sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
            { status: 200 },
          );
        }

        if (url.includes("/sandboxes/sbx-1")) {
          return new Response(
            sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle", status: "running" }),
            { status: 200 },
          );
        }

        if (url.includes("/sandboxes/my-original-name")) {
          return new Response(
            sandboxInfoBody({ id: "sbx-1", name: "my-original-name", status: "running" }),
            { status: 200 },
          );
        }

        return new Response("", { status: 404 });
      });

      const sbx = await Sandbox.connect({
        sandboxId: "my-original-name",
        apiUrl: "http://localhost:8900",
      });
      await sbx.update({ name: "renamed-by-handle" });
      expect(await sbx.status()).toBe(SandboxStatus.RUNNING);
      expect(sbx.name).toBe("renamed-by-handle");

      const patchCalls = calls.filter((line) => line.startsWith("PATCH "));
      expect(patchCalls).toHaveLength(1);
      expect(patchCalls[0]).toContain("/sandboxes/sbx-1");
      expect(calls.some((line) => line.includes("/sandboxes/my-original-name"))).toBe(true);
      expect(calls.some((line) => line.includes("/sandboxes/sbx-1"))).toBe(true);
      sbx.close();
    });

    it("checkpoint() uses canonical sandbox ID after renaming a SandboxClient.connect(name) handle", async () => {
      const calls: string[] = [];
      mockFetch((url, init) => {
        const method = init?.method ?? "GET";
        calls.push(`${method} ${url}`);

        if (method === "PATCH") {
          expect(url).toContain("/sandboxes/my-original-name");
          return new Response(
            sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
            { status: 200 },
          );
        }

        if (method === "POST") {
          expect(url).toContain("/sandboxes/sbx-1/snapshot");
          return new Response(
            JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
            { status: 200 },
          );
        }

        return new Response("", { status: 404 });
      });

      const client = new SandboxClient({ apiUrl: "http://localhost:8900" }, true);
      const sbx = client.connect("my-original-name");
      sbx._setOwner(client);

      await sbx.update({ name: "renamed-by-handle" });
      await sbx.checkpoint({ wait: false });

      expect(calls.some((line) => line.startsWith("POST ") && line.includes("/sandboxes/sbx-1/snapshot"))).toBe(true);
      sbx.close();
      client.close();
    });

    it("listSnapshots() filters by canonical sandbox ID after renaming a SandboxClient.connect(name) handle", async () => {
      mockFetch((url, init) => {
        const method = init?.method ?? "GET";

        if (method === "PATCH") {
          expect(url).toContain("/sandboxes/my-original-name");
          return new Response(
            sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
            { status: 200 },
          );
        }

        if (method === "GET" && url.includes("/snapshots")) {
          return new Response(
            JSON.stringify({
              snapshots: [
                { snapshot_id: "snap-1", sandbox_id: "sbx-1", status: "completed" },
                { snapshot_id: "snap-2", sandbox_id: "other-sbx", status: "completed" },
              ],
            }),
            { status: 200 },
          );
        }

        return new Response("", { status: 404 });
      });

      const client = new SandboxClient({ apiUrl: "http://localhost:8900" }, true);
      const sbx = client.connect("my-original-name");
      sbx._setOwner(client);

      await sbx.update({ name: "renamed-by-handle" });
      const snaps = await sbx.listSnapshots();

      expect(snaps).toHaveLength(1);
      expect(snaps[0].snapshotId).toBe("snap-1");
      sbx.close();
      client.close();
    });
  });

  describe("ptyWsUrl", () => {
    it("constructs correct WSS URL for https", () => {
      const sbx = new Sandbox({
        sandboxId: "sbx-1",
        proxyUrl: "https://sandbox.tensorlake.ai",
      });
      const url = sbx.ptyWsUrl("sess-1", "tok-1");
      expect(url).toBe(
        "wss://sbx-1.sandbox.tensorlake.ai/api/v1/pty/sess-1/ws?token=tok-1",
      );
      sbx.close();
    });

    it("constructs correct WS URL for http localhost", () => {
      const sbx = makeSandbox("sbx-1");
      const url = sbx.ptyWsUrl("sess-1", "tok-1");
      expect(url).toBe(
        "ws://localhost:9443/api/v1/pty/sess-1/ws?token=tok-1",
      );
      sbx.close();
    });
  });
});
