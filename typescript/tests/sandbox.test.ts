import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { Sandbox } from "../src/sandbox.js";
import { ProcessStatus } from "../src/models.js";

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
