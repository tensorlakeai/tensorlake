import { afterEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { SandboxClient } from "../src/client.js";
import { ProcessStatus, SandboxStatus } from "../src/models.js";
import { SandboxError } from "../src/errors.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

// Every sandbox/lifecycle op is served by the Rust core; install a fake native
// binding (shared `installNativeStub`) to drive both the proxy (process/file/
// stream ops) and the management client (lifecycle ops).

describe("Sandbox", () => {
  afterEach(() => {
    clearNativeStub();
    vi.restoreAllMocks();
    delete process.env.TENSORLAKE_SDK_TIMINGS;
    delete process.env.TENSORLAKE_SDK_TIMING_PAYLOADS;
  });

  function makeSandbox(id = "sbx-test"): Sandbox {
    return new Sandbox({
      sandboxId: id,
      proxyUrl: "http://localhost:9443",
    });
  }

  describe("constructor", () => {
    it("sets sandboxId", () => {
      installNativeStub();
      const sbx = makeSandbox("sbx-abc");
      expect(sbx.sandboxId).toBe("sbx-abc");
      sbx.close();
    });

    it("uses explicit requestTimeout for proxy operations", async () => {
      const stub = installNativeStub({
        proxy: {
          health: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ healthy: true }),
          })),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-abc",
        proxyUrl: "http://localhost:9443",
        requestTimeout: 10,
      });
      await sbx.health();
      // Sandbox.connect wires the shared lifecycle native client, so the proxy
      // is minted via connectProxy(proxyUrl, sandboxId, routingHint,
      // requestTimeoutSec); the explicit timeout flows through as the 4th arg.
      expect(stub.client.connectProxy).toHaveBeenCalledWith(
        "http://localhost:9443",
        "sbx-abc",
        null,
        10,
      );
      sbx.close();
    });
  });

  describe("copy", () => {
    it("uses the lifecycle client", async () => {
      const stub = installNativeStub({
        client: {
          copySandbox: vi.fn(async (sandboxId: string, times: number) => {
            expect(sandboxId).toBe("sbx-abc");
            expect(times).toBe(3);
            return {
              traceId: "t",
              json: JSON.stringify({
                source_sandbox_id: "sbx-abc",
                sandboxes: [{ sandbox_id: "copy-1", status: "running" }],
              }),
            };
          }),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-abc",
        proxyUrl: "http://localhost:9443",
        apiUrl: "http://localhost:8900",
      });
      const response = await sbx.copy({ times: 3 });

      expect(response.sourceSandboxId).toBe("sbx-abc");
      expect(response.sandboxes[0].sandboxId).toBe("copy-1");
      expect(stub.client.copySandbox).toHaveBeenCalled();
      sbx.close();
    });
  });

  describe("run", () => {
    /** A buffered run_process event list (each event a JSON string). */
    function runEvents(events: unknown[]): { traceId: string; events: string[] } {
      return { traceId: "t", events: events.map((e) => JSON.stringify(e)) };
    }

    it("runs a command and returns result", async () => {
      installNativeStub({
        proxy: {
          runProcess: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.user).toBe("1000:1000");
            return runEvents([
              { pid: 42, started_at: 1700000000 },
              { line: "hello", timestamp: 1700000000.1, stream: "stdout" },
              { exit_code: 0 },
            ]);
          }),
        },
      });

      const sbx = makeSandbox();
      const result = await sbx.run("echo", { args: ["hello"], user: "1000:1000" });
      expect(result.exitCode).toBe(0);
      expect(result.stdout).toBe("hello");
      expect(result.stderr).toBe("");
      sbx.close();
    });

    it("emits SDK timings without payloads by default", async () => {
      process.env.TENSORLAKE_SDK_TIMINGS = "1";
      const errorSpy = vi.spyOn(console, "error").mockImplementation(() => {});
      installNativeStub({
        proxy: {
          runProcess: vi.fn(async () =>
            runEvents([{ pid: 42, started_at: 1700000000 }, { exit_code: 0 }]),
          ),
          readFile: vi.fn(async () => ({
            traceId: "t",
            data: Buffer.from("secret"),
          })),
        },
      });

      const sbx = makeSandbox();
      await sbx.run("echo");
      await sbx.readFile("/home/tl-user/secret.txt");

      // run() now emits only `start` and `complete` phases (the per-request
      // stream_headers/stream_complete phases were SSE artifacts of the old
      // undici path).
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("[tensorlake:sdk-timing] op=sandbox.run phase=start"),
      );
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("command_length=4"),
      );
      // The command payload must not leak when payloads are disabled.
      expect(errorSpy.mock.calls.some(([line]) => String(line).includes("command=echo"))).toBe(
        false,
      );
      expect(errorSpy).toHaveBeenCalledWith(
        expect.stringContaining("phase=complete"),
      );
      // The read file path/contents must never leak into timings.
      expect(errorSpy.mock.calls.some(([line]) => String(line).includes("secret.txt"))).toBe(
        false,
      );
      expect(errorSpy.mock.calls.some(([line]) => String(line).includes("secret"))).toBe(
        false,
      );
      sbx.close();
    });

    it("omits the process user by default", async () => {
      installNativeStub({
        proxy: {
          runProcess: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            // No user requested -> field omitted so the sandbox resolves the
            // image's configured user (image USER, falling back to root).
            expect(body.user).toBeUndefined();
            return runEvents([
              { pid: 42, started_at: 1700000000 },
              { exit_code: 0 },
            ]);
          }),
        },
      });

      const sbx = makeSandbox();
      await sbx.run("echo");
      sbx.close();
    });
  });

  describe("listProcesses", () => {
    it("returns an array with traceId", async () => {
      installNativeStub({
        proxy: {
          listProcesses: vi.fn(async () => ({
            traceId: "trace-list",
            json: JSON.stringify({
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
          })),
        },
      });

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
      installNativeStub({
        proxy: {
          listProcesses: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              processes: [
                { pid: 1, status: "running", command: "bash", args: [], stdin_writable: false, started_at: 1700000000 },
                { pid: 2, status: "exited", command: "ls", args: [], stdin_writable: false, started_at: 1700000001 },
              ],
            }),
          })),
        },
      });

      const sbx = makeSandbox();
      const procs = await sbx.listProcesses();
      const pids = procs.map((p) => p.pid);

      expect(pids).toEqual([1, 2]);
      sbx.close();
    });
  });

  describe("startProcess", () => {
    it("sends correct payload", async () => {
      installNativeStub({
        proxy: {
          startProcess: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.command).toBe("bash");
            expect(body.args).toEqual(["-c", "ls"]);
            expect(body.working_dir).toBe("/tmp");
            expect(body.user).toEqual({ uid: 1000, gid: 1000 });
            return {
              traceId: "t",
              json: JSON.stringify({
                pid: 1,
                status: "running",
                command: "bash",
                args: ["-c", "ls"],
                stdin_writable: false,
                started_at: 1700000000,
              }),
            };
          }),
        },
      });

      const sbx = makeSandbox();
      const proc = await sbx.startProcess("bash", {
        args: ["-c", "ls"],
        workingDir: "/tmp",
        user: { uid: 1000, gid: 1000 },
      });
      expect(proc.pid).toBe(1);
      expect(proc.status).toBe(ProcessStatus.RUNNING);
      sbx.close();
    });

    it("omits the process user by default", async () => {
      installNativeStub({
        proxy: {
          startProcess: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            // No user requested -> field omitted so the sandbox resolves the
            // image's configured user (image USER, falling back to root).
            expect(body.user).toBeUndefined();
            return {
              traceId: "t",
              json: JSON.stringify({
                pid: 1,
                status: "running",
                command: "bash",
                args: [],
                stdin_writable: false,
                started_at: 1700000000,
              }),
            };
          }),
        },
      });

      const sbx = makeSandbox();
      await sbx.startProcess("bash");
      sbx.close();
    });

    it("sends managed process options and parses managed metadata", async () => {
      installNativeStub({
        proxy: {
          startProcess: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.name).toBe("web");
            expect(body.restart).toEqual({
              policy: "always",
              max_restarts: 10,
              initial_backoff_ms: 250,
            });
            expect(body.health_check).toEqual({
              type: "http",
              port: 8000,
              path: "/health",
              interval_ms: 5000,
            });
            return {
              traceId: "t",
              json: JSON.stringify({
                handle: 7,
                pid: 1,
                status: "running",
                command: "bash",
                args: [],
                stdin_writable: false,
                started_at: 1700000000,
                managed: {
                  id: "managed-1",
                  name: "web",
                  status: "running",
                  restart_count: 0,
                  restart: {
                    policy: "always",
                    max_restarts: 10,
                    initial_backoff_ms: 250,
                    max_backoff_ms: 30000,
                  },
                  health_check: {
                    type: "http",
                    port: 8000,
                    path: "/health",
                    interval_ms: 5000,
                  },
                  health_status: "healthy",
                  consecutive_health_failures: 0,
                },
              }),
            };
          }),
        },
      });

      const sbx = makeSandbox();
      const proc = await sbx.startProcess("bash", {
        name: "web",
        restart: {
          policy: "always",
          maxRestarts: 10,
          initialBackoffMs: 250,
        },
        healthCheck: {
          type: "http",
          port: 8000,
          path: "/health",
          intervalMs: 5000,
        },
      });
      expect(proc.handle).toBe(7);
      expect(proc.managed?.name).toBe("web");
      expect(proc.managed?.restartCount).toBe(0);
      expect(proc.managed?.restart.initialBackoffMs).toBe(250);
      expect(proc.managed?.healthCheck?.intervalMs).toBe(5000);
      sbx.close();
    });

    it("rejects a numeric managed-process name client-side", async () => {
      installNativeStub();
      const sbx = makeSandbox();
      await expect(sbx.startProcess("bash", { name: "123" })).rejects.toThrow();
      sbx.close();
    });

    it("addresses a process by name or pid (segment is stringified)", async () => {
      const stub = installNativeStub();
      const sbx = makeSandbox();
      await sbx.killProcess("web");
      await sbx.killProcess(1234);
      expect(stub.proxy.killProcess).toHaveBeenNthCalledWith(1, "web");
      expect(stub.proxy.killProcess).toHaveBeenNthCalledWith(2, "1234");
      sbx.close();
    });
  });

  describe("restartProcess", () => {
    it("restarts the process by PID", async () => {
      const stub = installNativeStub({
        proxy: {
          restartProcess: vi.fn(async (process: string) => {
            expect(process).toBe("42");
            return {
              traceId: "t",
              json: JSON.stringify({
                handle: 8,
                pid: 43,
                status: "running",
                command: "bash",
                args: [],
                stdin_writable: false,
                started_at: 1700000001,
                managed: {
                  id: "managed-1",
                  name: "web",
                  status: "running",
                  restart_count: 1,
                  restart: {
                    policy: "always",
                    initial_backoff_ms: 500,
                    max_backoff_ms: 30000,
                  },
                  health_status: "healthy",
                  consecutive_health_failures: 0,
                },
              }),
            };
          }),
        },
      });

      const sbx = makeSandbox();
      const proc = await sbx.restartProcess(42);
      expect(proc.pid).toBe(43);
      expect(proc.managed?.restartCount).toBe(1);
      expect(stub.proxy.restartProcess).toHaveBeenCalledWith("42");
      sbx.close();
    });
  });

  describe("file operations", () => {
    it("reads a file", async () => {
      const stub = installNativeStub({
        proxy: {
          readFile: vi.fn(async (path: string) => {
            expect(path).toBe("/tmp/test.txt");
            return {
              traceId: "t",
              data: Buffer.from([72, 101, 108, 108, 111]),
            };
          }),
        },
      });

      const sbx = makeSandbox();
      const data = await sbx.readFile("/tmp/test.txt");
      expect(new TextDecoder().decode(data)).toBe("Hello");
      expect(stub.proxy.readFile).toHaveBeenCalledWith("/tmp/test.txt");
      sbx.close();
    });

    it("writes a file", async () => {
      const stub = installNativeStub();

      const sbx = makeSandbox();
      await sbx.writeFile(
        "/tmp/out.txt",
        new TextEncoder().encode("content"),
      );
      const [path, content] = stub.proxy.writeFile.mock.calls[0];
      expect(path).toBe("/tmp/out.txt");
      expect(Buffer.isBuffer(content)).toBe(true);
      expect((content as Buffer).toString()).toBe("content");
      sbx.close();
    });

    it("lists a directory", async () => {
      installNativeStub({
        proxy: {
          listDirectory: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              path: "/tmp",
              entries: [
                { name: "file.txt", is_dir: false, size: 100, modified_at: 1700000000 },
                { name: "subdir", is_dir: true, size: null, modified_at: null },
              ],
            }),
          })),
        },
      });

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
      installNativeStub({
        proxy: {
          health: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ healthy: true }),
          })),
        },
      });

      const sbx = makeSandbox();
      const health = await sbx.health();
      expect(health.healthy).toBe(true);
      sbx.close();
    });
  });

  describe("daemonInfo", () => {
    it("returns daemon info", async () => {
      installNativeStub({
        proxy: {
          info: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              version: "1.0.0",
              uptime_secs: 3600,
              running_processes: 2,
              total_processes: 10,
            }),
          })),
        },
      });

      const sbx = makeSandbox();
      const info = await sbx.daemonInfo();
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
        ...overrides,
      });
    }

    it("connect() is lazy — info() populates name on demand", async () => {
      const stub = installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: sandboxInfoBody({ name: "my-sandbox" }),
          })),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBeNull(); // lazy: no GET on connect
      expect(stub.client.getSandbox).not.toHaveBeenCalled();
      await sbx.info();
      expect(sbx.name).toBe("my-sandbox");
      sbx.close();
    });

    it("connect() leaves name null until info() is called", async () => {
      installNativeStub();
      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBeNull();
      sbx.close();
    });

    it("status() fetches fresh status from the server every call", async () => {
      const responses = [
        sandboxInfoBody({ status: "running" }),
        sandboxInfoBody({ status: "suspended" }),
      ];
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: responses.shift()!,
          })),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(await sbx.status()).toBe(SandboxStatus.RUNNING);
      expect(await sbx.status()).toBe(SandboxStatus.SUSPENDED);
      sbx.close();
    });

    it("status() throws when no lifecycle client is wired", async () => {
      installNativeStub();
      const sbx = makeSandbox();
      await expect(sbx.status()).rejects.toThrow(SandboxError);
      sbx.close();
    });

    it("update() updates the sandbox and refreshes the local name", async () => {
      const stub = installNativeStub({
        client: {
          updateSandbox: vi.fn(async (id: string, json: string) => {
            expect(id).toBe("sbx-1");
            const body = JSON.parse(json);
            expect(body.name).toBe("renamed");
            expect(body.exposed_ports).toEqual([8080]);
            return {
              traceId: "t",
              json: sandboxInfoBody({ name: "renamed", exposed_ports: [8080] }),
            };
          }),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-1",
        apiUrl: "http://localhost:8900",
      });
      expect(sbx.name).toBeNull(); // lazy connect: no initial GET

      const info = await sbx.update({ name: "renamed", exposedPorts: [8080] });

      expect(stub.client.updateSandbox).toHaveBeenCalledOnce();
      expect(info.name).toBe("renamed");
      expect(sbx.name).toBe("renamed");
      sbx.close();
    });

    it("update() throws when no lifecycle client is wired", async () => {
      installNativeStub();
      const sbx = makeSandbox();
      await expect(sbx.update({ name: "x" })).rejects.toThrow(SandboxError);
      sbx.close();
    });

    it("lifecycle calls stay pinned to canonical sandbox ID after the first mutating call resolves it", async () => {
      const updateSandbox = vi.fn(async (id: string) => {
        // First mutating call uses the name; response reveals the canonical UUID.
        expect(id).toBe("my-original-name");
        return {
          traceId: "t",
          json: sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
        };
      });
      const getSandbox = vi.fn(async (id: string) => {
        expect(id).toBe("sbx-1");
        return {
          traceId: "t",
          json: sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle", status: "running" }),
        };
      });
      installNativeStub({ client: { updateSandbox, getSandbox } });

      const sbx = await Sandbox.connect({
        sandboxId: "my-original-name",
        apiUrl: "http://localhost:8900",
      });
      await sbx.update({ name: "renamed-by-handle" });
      expect(await sbx.status()).toBe(SandboxStatus.RUNNING);
      expect(sbx.name).toBe("renamed-by-handle");

      expect(updateSandbox).toHaveBeenCalledOnce();
      expect(updateSandbox).toHaveBeenCalledWith("my-original-name", expect.any(String));
      // After update resolves the canonical ID, subsequent calls use the UUID.
      expect(getSandbox).toHaveBeenCalledWith("sbx-1");
      sbx.close();
    });

    it("checkpoint() uses canonical sandbox ID after renaming a SandboxClient.connect(name) handle", async () => {
      const updateSandbox = vi.fn(async (id: string) => {
        expect(id).toBe("my-original-name");
        return {
          traceId: "t",
          json: sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
        };
      });
      const createSnapshot = vi.fn(async (id: string) => {
        expect(id).toBe("sbx-1");
        return {
          traceId: "t",
          json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
        };
      });
      installNativeStub({ client: { updateSandbox, createSnapshot } });

      const client = new SandboxClient({ apiUrl: "http://localhost:8900" }, true);
      const sbx = client.connect("my-original-name");
      sbx._setOwner(client);

      await sbx.update({ name: "renamed-by-handle" });
      await sbx.checkpoint({ wait: false });

      expect(createSnapshot).toHaveBeenCalledWith("sbx-1", null);
      sbx.close();
      client.close();
    });

    it("listSnapshots() filters by canonical sandbox ID after renaming a SandboxClient.connect(name) handle", async () => {
      const updateSandbox = vi.fn(async (id: string) => {
        expect(id).toBe("my-original-name");
        return {
          traceId: "t",
          json: sandboxInfoBody({ id: "sbx-1", name: "renamed-by-handle" }),
        };
      });
      const listSnapshots = vi.fn(async () => ({
        traceId: "t",
        json: JSON.stringify({
          snapshots: [
            { snapshot_id: "snap-1", sandbox_id: "sbx-1", status: "completed" },
            { snapshot_id: "snap-2", sandbox_id: "other-sbx", status: "completed" },
          ],
        }),
      }));
      installNativeStub({ client: { updateSandbox, listSnapshots } });

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

  describe("file systems", () => {
    function fsSandboxInfoBody(overrides: Record<string, unknown> = {}) {
      return JSON.stringify({
        id: "sbx-abc",
        namespace: "default",
        status: "running",
        resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
        ...overrides,
      });
    }

    it("create() serializes fileSystems into the request body", async () => {
      let captured: Record<string, unknown> | undefined;
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            captured = JSON.parse(json);
            return { traceId: "t", json: fsSandboxInfoBody() };
          }),
        },
      });

      const client = new SandboxClient(
        { apiUrl: "http://localhost:8900" },
        /* _internal */ true,
      );
      await client.create({
        fileSystems: [
          { fileSystemId: "file_system_abc", mountPath: "/mnt/skills" },
        ],
      });
      expect(captured?.file_systems).toEqual([
        { file_system_id: "file_system_abc", mount_path: "/mnt/skills" },
      ]);
    });

    it("create() omits file_systems when none are provided", async () => {
      let captured: Record<string, unknown> | undefined;
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            captured = JSON.parse(json);
            return { traceId: "t", json: fsSandboxInfoBody() };
          }),
        },
      });

      const client = new SandboxClient(
        { apiUrl: "http://localhost:8900" },
        /* _internal */ true,
      );
      await client.create({});
      expect(captured && "file_systems" in captured).toBe(false);
    });

    it("attachFileSystem() calls the native client and returns updated mounts", async () => {
      const stub = installNativeStub({
        client: {
          attachFileSystem: vi.fn(
            async (sandboxId: string, fileSystemId: string, mountPath: string) => {
              expect(sandboxId).toBe("sbx-abc");
              expect(fileSystemId).toBe("file_system_abc");
              expect(mountPath).toBe("/mnt/skills");
              return {
                traceId: "t",
                json: fsSandboxInfoBody({
                  file_systems: [
                    { file_system_id: "file_system_abc", mount_path: "/mnt/skills" },
                  ],
                }),
              };
            },
          ),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-abc",
        apiUrl: "http://localhost:8900",
      });
      const info = await sbx.attachFileSystem(
        "file_system_abc",
        "/mnt/skills",
      );
      expect(info.fileSystems).toEqual([
        { fileSystemId: "file_system_abc", mountPath: "/mnt/skills" },
      ]);
      expect(stub.client.attachFileSystem).toHaveBeenCalledOnce();
      sbx.close();
    });

    it("detachFileSystem() calls the native client with the mount path", async () => {
      const stub = installNativeStub({
        client: {
          detachFileSystem: vi.fn(
            async (sandboxId: string, mountPath: string) => {
              expect(sandboxId).toBe("sbx-abc");
              expect(mountPath).toBe("/mnt/skills");
              return {
                traceId: "t",
                json: fsSandboxInfoBody({ file_systems: [] }),
              };
            },
          ),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-abc",
        apiUrl: "http://localhost:8900",
      });
      const info = await sbx.detachFileSystem("/mnt/skills");
      expect(info.fileSystems).toEqual([]);
      expect(stub.client.detachFileSystem).toHaveBeenCalledOnce();
      sbx.close();
    });

    it("listFileSystems() reads the sandbox's current mounts", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: fsSandboxInfoBody({
              file_systems: [
                { file_system_id: "file_system_abc", mount_path: "/mnt/skills" },
              ],
            }),
          })),
        },
      });

      const sbx = await Sandbox.connect({
        sandboxId: "sbx-abc",
        apiUrl: "http://localhost:8900",
      });
      const mounts = await sbx.listFileSystems();
      expect(mounts.map((m) => m.fileSystemId)).toEqual(["file_system_abc"]);
      expect(mounts[0].mountPath).toBe("/mnt/skills");
      sbx.close();
    });
  });

  describe("ptyWsUrl", () => {
    it("constructs correct WSS URL for https", () => {
      installNativeStub();
      const sbx = new Sandbox({
        sandboxId: "sbx-1",
        proxyUrl: "https://sandbox.tensorlake.ai",
      });
      const url = sbx.ptyWsUrl("sess-1", "tok-1");
      expect(url).toBe(
        "wss://sandbox.tensorlake.ai/api/v1/pty/sess-1/ws?token=tok-1",
      );
      sbx.close();
    });

    it("constructs correct WS URL for http localhost", () => {
      installNativeStub();
      const sbx = makeSandbox("sbx-1");
      const url = sbx.ptyWsUrl("sess-1", "tok-1");
      expect(url).toBe(
        "ws://localhost:9443/api/v1/pty/sess-1/ws?token=tok-1",
      );
      sbx.close();
    });
  });
});
