import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { SandboxClient } from "../src/client.js";
import { SandboxStatus, SnapshotStatus } from "../src/models.js";
import { SandboxError, SandboxNotFoundError } from "../src/errors.js";

vi.mock("undici", async (importOriginal) => {
  const actual = await importOriginal<typeof import("undici")>();
  return { ...actual, fetch: vi.fn() };
});

describe("SandboxClient", () => {

  afterEach(() => {
    vi.mocked(undici.fetch).mockReset();
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    vi.mocked(undici.fetch).mockImplementation(handler as typeof undici.fetch);
  }

  describe("construction", () => {
    it("creates cloud client with defaults", () => {
      const client = SandboxClient.forCloud({ apiKey: "key" });
      expect(client).toBeInstanceOf(SandboxClient);
      client.close();
    });

    it("creates localhost client", () => {
      const client = SandboxClient.forLocalhost();
      expect(client).toBeInstanceOf(SandboxClient);
      client.close();
    });
  });

  describe("create", () => {
    it("creates a sandbox with defaults", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.resources).toEqual({
          cpus: 1.0,
          memory_mb: 1024,
        });
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create();
      expect(result.sandboxId).toBe("sbx-1");
      expect(result.status).toBe(SandboxStatus.PENDING);
      client.close();
    });

    it("creates a sandbox with custom options", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.image).toBe("python:3.12");
        expect(body.resources.cpus).toBe(2);
        expect(body.resources.memory_mb).toBe(1024);
        expect(body.resources.ephemeral_disk_mb).toBeUndefined();
        expect(body.resources.disk_mb).toBe(25 * 1024);
        expect(body.network).toEqual({
          allow_internet_access: false,
          allow_out: ["8.8.8.8"],
          deny_out: [],
        });
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-2", status: "pending" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create({
        image: "python:3.12",
        cpus: 2,
        memoryMb: 1024,
        diskMb: 25 * 1024,
        allowInternetAccess: false,
        allowOut: ["8.8.8.8"],
      });
      expect(result.sandboxId).toBe("sbx-2");
      client.close();
    });

    it("sends name in request body when provided", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.name).toBe("my-sandbox");
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-named", status: "pending" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create({ name: "my-sandbox" });
      expect(result.sandboxId).toBe("sbx-named");
      client.close();
    });

    it("omits name from request body when not provided", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.name).toBeUndefined();
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      await client.create();
      client.close();
    });

    it("returns readiness timeout responses without retrying", async () => {
      let attempts = 0;
      mockFetch(() => {
        attempts++;
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
          { status: 504 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create();
      expect(result.sandboxId).toBe("sbx-timeout");
      expect(result.status).toBe(SandboxStatus.TIMEOUT);
      expect(attempts).toBe(1);
      client.close();
    });

    it("does not retry rate-limited create responses", async () => {
      let attempts = 0;
      mockFetch(() => {
        attempts++;
        return new Response("rate limited", { status: 429 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.create()).rejects.toThrow("rate limited");
      expect(attempts).toBe(1);
      client.close();
    });
  });

  describe("get", () => {
    it("gets sandbox info with id mapped to sandboxId", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            created_at: 1700000000,
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-1");
      expect(info.sandboxId).toBe("sbx-1");
      expect(info.status).toBe(SandboxStatus.RUNNING);
      expect(info.createdAt).toBeInstanceOf(Date);
      client.close();
    });

    it("maps name field from response", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-named",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            name: "my-sandbox",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-named");
      expect(info.name).toBe("my-sandbox");
      client.close();
    });

    it("maps port access fields from response", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-ports",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            allow_unauthenticated_access: true,
            exposed_ports: [8080, 3000],
            ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
            sandbox_url: "https://sbx-ports.sandbox.tensorlake.ai",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-ports");
      expect(info.allowUnauthenticatedAccess).toBe(true);
      expect(info.exposedPorts).toEqual([8080, 3000]);
      expect(info.ingressEndpoint).toBe("https://sandbox.us-east-1.aws.tensorlake.ai");
      expect(info.sandboxUrl).toBe("https://sbx-ports.sandbox.tensorlake.ai");
      client.close();
    });

    it("returns undefined name when absent from response", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-1");
      expect(info.name).toBeUndefined();
      client.close();
    });
  });

  describe("list", () => {
    it("lists sandboxes", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            sandboxes: [
              {
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              },
            ],
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const list = await client.list();
      expect(list).toHaveLength(1);
      expect(list[0].sandboxId).toBe("sbx-1");
      client.close();
    });

    it("returns traceId on the array", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({ sandboxes: [] }),
          {
            status: 200,
            headers: { traceparent: "00-aabbccdd00112233aabbccdd00112233-cafebabe12345678-01" },
          },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const list = await client.list();
      expect(typeof list.traceId).toBe("string");
      expect(list.traceId.length).toBeGreaterThan(0);
      client.close();
    });
  });

  describe("update", () => {
    it("updates an unnamed sandbox with a new name", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1");
        expect(init?.method).toBe("PATCH");
        const body = JSON.parse(init?.body as string);
        expect(body.name).toBe("my-new-name");
        return new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            name: "my-new-name",
          }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.update("sbx-1", { name: "my-new-name" });
      expect(info.sandboxId).toBe("sbx-1");
      expect(info.name).toBe("my-new-name");
      client.close();
    });

    it("updates sandbox port access settings", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1");
        expect(init?.method).toBe("PATCH");
        const body = JSON.parse(init?.body as string);
        expect(body.allow_unauthenticated_access).toBe(true);
        expect(body.exposed_ports).toEqual([8080, 8081]);
        return new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            allow_unauthenticated_access: true,
            exposed_ports: [8080, 8081],
          }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.update("sbx-1", {
        allowUnauthenticatedAccess: true,
        exposedPorts: [8081, 8080],
      });
      expect(info.allowUnauthenticatedAccess).toBe(true);
      expect(info.exposedPorts).toEqual([8080, 8081]);
      client.close();
    });

    it("rejects empty sandbox updates", async () => {
      const client = SandboxClient.forLocalhost();
      await expect(client.update("sbx-1", {})).rejects.toThrow(
        "At least one sandbox update field must be provided.",
      );
      client.close();
    });
  });

  describe("port management", () => {
    it("reads current port access", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            allow_unauthenticated_access: false,
            exposed_ports: [8080],
            ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
            sandbox_url: "https://sbx-1.sandbox.tensorlake.ai",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const access = await client.getPortAccess("sbx-1");
      expect(access.allowUnauthenticatedAccess).toBe(false);
      expect(access.exposedPorts).toEqual([8080]);
      expect(access.ingressEndpoint).toBe("https://sandbox.us-east-1.aws.tensorlake.ai");
      expect(access.sandboxUrl).toBe("https://sbx-1.sandbox.tensorlake.ai");
      client.close();
    });

    it("exposes ports by merging with existing ports", async () => {
      vi.mocked(undici.fetch)
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: false,
              exposed_ports: [8080],
            }),
            { status: 200 },
          ),
        )
        .mockImplementationOnce((_url, init) => {
          const body = JSON.parse(init?.body as string);
          expect(body.allow_unauthenticated_access).toBe(true);
          expect(body.exposed_ports).toEqual([8080, 8081]);
          return Promise.resolve(
            new Response(
              JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                allow_unauthenticated_access: true,
                exposed_ports: [8080, 8081],
              }),
              { status: 200 },
            ),
          );
        });

      const client = SandboxClient.forLocalhost();
      const info = await client.exposePorts("sbx-1", [8081, 8080], {
        allowUnauthenticatedAccess: true,
      });
      expect(info.exposedPorts).toEqual([8080, 8081]);
      client.close();
    });

    it("unexposes ports and disables unauthenticated access when none remain", async () => {
      vi.mocked(undici.fetch)
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: true,
              exposed_ports: [8080],
            }),
            { status: 200 },
          ),
        )
        .mockImplementationOnce((_url, init) => {
          const body = JSON.parse(init?.body as string);
          expect(body.allow_unauthenticated_access).toBe(false);
          expect(body.exposed_ports).toEqual([]);
          return Promise.resolve(
            new Response(
              JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                allow_unauthenticated_access: false,
                exposed_ports: [],
              }),
              { status: 200 },
            ),
          );
        });

      const client = SandboxClient.forLocalhost();
      const info = await client.unexposePorts("sbx-1", [8080]);
      expect(info.exposedPorts).toEqual([]);
      expect(info.allowUnauthenticatedAccess).toBe(false);
      client.close();
    });

    it("rejects reserved management port 9501", async () => {
      const client = SandboxClient.forLocalhost();
      await expect(client.exposePorts("sbx-1", [9501])).rejects.toThrow(
        "port 9501 is reserved for sandbox management",
      );
      client.close();
    });
  });

  describe("delete", () => {
    it("deletes a sandbox", async () => {
      mockFetch(() => new Response("", { status: 200 }));
      const client = SandboxClient.forLocalhost();
      await client.delete("sbx-1");
      client.close();
    });
  });

  describe("suspend", () => {
    it("sends POST to suspend endpoint with wait=false", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1/suspend");
        expect(init?.method).toBe("POST");
        return new Response("", { status: 202 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.suspend("sbx-1", { wait: false })).resolves.toBeUndefined();
      client.close();
    });

    it("polls until Suspended when wait=true (default)", async () => {
      let callCount = 0;
      vi.mocked(undici.fetch).mockImplementation(((url: string, init?: RequestInit) => {
        callCount++;
        if (callCount === 1) {
          // First call: POST suspend
          expect(url).toContain("/sandboxes/sbx-1/suspend");
          return Promise.resolve(new Response("", { status: 202 }));
        }
        // Subsequent calls: GET sandbox status
        expect(url).toContain("/sandboxes/sbx-1");
        return Promise.resolve(
          new Response(
            JSON.stringify({ sandbox_id: "sbx-1", status: "suspended", namespace: "default", resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 } }),
            { status: 200 },
          ),
        );
      }) as typeof undici.fetch);

      const client = SandboxClient.forLocalhost();
      await expect(client.suspend("sbx-1")).resolves.toBeUndefined();
      expect(callCount).toBeGreaterThanOrEqual(2);
      client.close();
    });
  });

  describe("resume", () => {
    it("sends POST to resume endpoint with wait=false", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1/resume");
        expect(init?.method).toBe("POST");
        return new Response("", { status: 202 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.resume("sbx-1", { wait: false })).resolves.toBeUndefined();
      client.close();
    });

    it("polls until Running when wait=true (default)", async () => {
      let callCount = 0;
      vi.mocked(undici.fetch).mockImplementation(((url: string, init?: RequestInit) => {
        callCount++;
        if (callCount === 1) {
          // First call: POST resume
          expect(url).toContain("/sandboxes/sbx-1/resume");
          return Promise.resolve(new Response("", { status: 202 }));
        }
        // Subsequent calls: GET sandbox status
        expect(url).toContain("/sandboxes/sbx-1");
        return Promise.resolve(
          new Response(
            JSON.stringify({ sandbox_id: "sbx-1", status: "running", namespace: "default", resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 } }),
            { status: 200 },
          ),
        );
      }) as typeof undici.fetch);

      const client = SandboxClient.forLocalhost();
      await expect(client.resume("sbx-1")).resolves.toBeUndefined();
      expect(callCount).toBeGreaterThanOrEqual(2);
      client.close();
    });
  });

  describe("claim", () => {
    it("claims from pool", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({ sandbox_id: "sbx-3", status: "running" }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const result = await client.claim("pool-1");
      expect(result.sandboxId).toBe("sbx-3");
      client.close();
    });

    it("returns readiness timeout responses without retrying", async () => {
      let attempts = 0;
      mockFetch(() => {
        attempts++;
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
          { status: 504 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.claim("pool-1");
      expect(result.sandboxId).toBe("sbx-timeout");
      expect(result.status).toBe(SandboxStatus.TIMEOUT);
      expect(attempts).toBe(1);
      client.close();
    });

    it("does not retry rate-limited claim responses", async () => {
      let attempts = 0;
      mockFetch(() => {
        attempts++;
        return new Response("rate limited", { status: 429 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.claim("pool-1")).rejects.toThrow("rate limited");
      expect(attempts).toBe(1);
      client.close();
    });
  });

  describe("copy", () => {
    it("live-copies a sandbox and maps partial failures", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/v1/namespaces/default/sandboxes/sbx-1/copy?times=2");
        expect(init?.method).toBe("POST");
        const headers = init?.headers as Record<string, string>;
        expect(headers["X-Tensorlake-Request-Timeout-Ms"]).toBe("12000");
        return new Response(
          JSON.stringify({
            source_sandbox_id: "sbx-1",
            sandboxes: [
              { sandbox_id: "copy-1", status: "running" },
              { sandbox_id: "copy-2", status: "failed", reason: "no capacity" },
            ],
          }),
          { status: 422 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const response = await client.copy("sbx-1", {
        times: 2,
        requestTimeout: 12,
      });

      expect(response.sourceSandboxId).toBe("sbx-1");
      expect(response.sandboxes[0].sandboxId).toBe("copy-1");
      expect(response.sandboxes[0].status).toBe("running");
      expect(response.sandboxes[1].sandboxId).toBe("copy-2");
      expect(response.sandboxes[1].status).toBe("failed");
      expect(response.sandboxes[1].reason).toBe("no capacity");
      expect(response.traceId).toBeDefined();
      client.close();
    });

    it("rejects invalid times", async () => {
      const client = SandboxClient.forLocalhost();
      await expect(client.copy("sbx-1", { times: 0 })).rejects.toThrow(
        "times must be a positive integer",
      );
      client.close();
    });
  });

  describe("createAndConnect", () => {
    it("uses ingress endpoint from running create response", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            sandbox_id: "sbx-1",
            status: "running",
            routing_hint: "hint-1",
            ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forCloud({ apiKey: "key" });
      const sandbox = await client.createAndConnect();
      expect(sandbox.sandboxId).toBe("sbx-1");
      expect((sandbox as unknown as { baseUrl: string }).baseUrl).toBe(
        "https://sandbox.us-east-1.aws.tensorlake.ai",
      );
      sandbox.close();
      client.close();
    });

    it("uses per-call requestTimeout for the initial create request", async () => {
      mockFetch((_url, init) => {
        const headers = init?.headers as Record<string, string>;
        expect(headers["X-Tensorlake-Request-Timeout-Ms"]).toBe("10000");
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({ requestTimeout: 10 });
      expect(sandbox.sandboxId).toBe("sbx-1");
      client.close();
      sandbox.close();
    });

    it("uses startupTimeout as a compatibility alias for requestTimeout", async () => {
      mockFetch((_url, init) => {
        const headers = init?.headers as Record<string, string>;
        expect(headers["X-Tensorlake-Request-Timeout-Ms"]).toBe("12000");
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({ startupTimeout: 12 });
      expect(sandbox.sandboxId).toBe("sbx-1");
      client.close();
      sandbox.close();
    });

    it("prefers requestTimeout over startupTimeout", async () => {
      mockFetch((_url, init) => {
        const headers = init?.headers as Record<string, string>;
        expect(headers["X-Tensorlake-Request-Timeout-Ms"]).toBe("15000");
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({
        requestTimeout: 15,
        startupTimeout: 12,
      });
      expect(sandbox.sandboxId).toBe("sbx-1");
      client.close();
      sandbox.close();
    });

    it("deletes the sandbox returned by a readiness timeout response", async () => {
      const calls: string[] = [];
      mockFetch((url, init) => {
        calls.push(`${init?.method ?? "GET"} ${url}`);
        if (init?.method === "POST") {
          return new Response(
            JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
            { status: 504 },
          );
        }
        expect(init?.method).toBe("DELETE");
        expect(url).toContain("/sandboxes/sbx-timeout");
        return new Response("{}", { status: 200 });
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      await expect(
        client.createAndConnect({ requestTimeout: 10 }),
      ).rejects.toThrow("Sandbox sbx-timeout did not start within 10s");
      expect(calls).toHaveLength(2);
      client.close();
    });

    it("includes sandbox errorDetails in startup failures", async () => {
      vi.mocked(undici.fetch)
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
            { status: 200 },
          ),
        )
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "terminated",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              error_details: { message: "failed to pull image tensorlake/missing-image" },
            }),
            { status: 200 },
          ),
        );

      const client = SandboxClient.forLocalhost();
      await expect(
        client.createAndConnect({ image: "tensorlake/missing-image" }),
      ).rejects.toThrow(
        "Sandbox sbx-1 terminated during startup: failed to pull image tensorlake/missing-image",
      );
      client.close();
    });
  });

  describe("snapshots", () => {
    it("creates a snapshot", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const result = await client.snapshot("sbx-1");
      expect(result.snapshotId).toBe("snap-1");
      expect(result.status).toBe(SnapshotStatus.IN_PROGRESS);
      client.close();
    });

    it("omits request body when no snapshot options are provided", async () => {
      // Pins down backwards compatibility: when snapshotType is unset we
      // must not change the wire shape for existing callers.
      mockFetch((_url, init) => {
        expect(init?.method).toBe("POST");
        expect(init?.body).toBeUndefined();
        return new Response(
          JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      await client.snapshot("sbx-1");
      client.close();
    });

    it("sends snapshot_type in body when snapshotType is provided", async () => {
      // Regression: sandbox image builds MUST pass `filesystem` so that
      // restored sandboxes cold-boot (see PR #583 for the original
      // regression that broke `tl sbx new --image`).
      mockFetch((_url, init) => {
        expect(init?.method).toBe("POST");
        const body = JSON.parse(String(init?.body ?? "{}"));
        expect(body.snapshot_type).toBe("filesystem");
        return new Response(
          JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.snapshot("sbx-1", {
        snapshotType: "filesystem",
      });
      expect(result.snapshotId).toBe("snap-1");
      client.close();
    });

    it("gets snapshot info", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "snap-1",
            namespace: "default",
            sandbox_id: "sbx-1",
            base_image: "python:3.12",
            status: "completed",
            snapshot_type: "filesystem",
            created_at: 1700000000,
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.getSnapshot("snap-1");
      expect(info.snapshotId).toBe("snap-1");
      expect(info.baseImage).toBe("python:3.12");
      expect(info.status).toBe(SnapshotStatus.COMPLETED);
      expect(info.snapshotType).toBe("filesystem");
      client.close();
    });

    it("snapshotAndWait returns on local_ready by default", async () => {
      mockFetch((_url, init) => {
        if (init?.method === "POST") {
          return new Response(
            JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
            { status: 200 },
          );
        }
        return new Response(
          JSON.stringify({
            id: "snap-1",
            namespace: "default",
            sandbox_id: "sbx-1",
            base_image: "python:3.12",
            status: "local_ready",
          }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.snapshotAndWait("sbx-1");
      expect(info.status).toBe(SnapshotStatus.LOCAL_READY);
      expect(info.snapshotUri).toBeUndefined();
      client.close();
    });

    it("snapshotAndWait can wait for completed snapshots", async () => {
      let getCalls = 0;
      mockFetch((_url, init) => {
        if (init?.method === "POST") {
          return new Response(
            JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
            { status: 200 },
          );
        }
        getCalls += 1;
        return new Response(
          JSON.stringify({
            id: "snap-1",
            namespace: "default",
            sandbox_id: "sbx-1",
            base_image: "python:3.12",
            status: getCalls === 1 ? "local_ready" : "completed",
            snapshot_uri: "s3://snap-1.tar.zst",
          }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.snapshotAndWait("sbx-1", {
        pollInterval: 0,
        waitUntil: "completed",
      });
      expect(info.status).toBe(SnapshotStatus.COMPLETED);
      expect(getCalls).toBe(2);
      client.close();
    });

    it("listSnapshots returns traceId on the array", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({ snapshots: [] }),
          {
            status: 200,
            headers: { traceparent: "00-aabbccdd00112233aabbccdd00112233-cafebabe12345678-01" },
          },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const snaps = await client.listSnapshots();
      expect(Array.isArray(snaps)).toBe(true);
      expect(typeof snaps.traceId).toBe("string");
      expect(snaps.traceId.length).toBeGreaterThan(0);
      client.close();
    });
  });

  describe("pools", () => {
    it("creates a pool", async () => {
      mockFetch((_url, init) => {
        const body = JSON.parse(init?.body as string);
        expect(body.image).toBe("node:20");
        expect(body.max_containers).toBe(5);
        return new Response(
          JSON.stringify({ pool_id: "pool-1", namespace: "default" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.createPool({
        image: "node:20",
        maxContainers: 5,
      });
      expect(result.poolId).toBe("pool-1");
      client.close();
    });

    it("gets pool info with id mapped to poolId", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "pool-1",
            namespace: "default",
            image: "node:20",
            resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            timeout_secs: 0,
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.getPool("pool-1");
      expect(info.poolId).toBe("pool-1");
      expect(info.image).toBe("node:20");
      client.close();
    });

    it("listPools returns traceId on the array", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({ pools: [] }),
          {
            status: 200,
            headers: { traceparent: "00-aabbccdd00112233aabbccdd00112233-cafebabe12345678-01" },
          },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const pools = await client.listPools();
      expect(Array.isArray(pools)).toBe(true);
      expect(typeof pools.traceId).toBe("string");
      expect(pools.traceId.length).toBeGreaterThan(0);
      client.close();
    });
  });

  describe("URL paths", () => {
    it("uses namespaced paths for localhost", async () => {
      mockFetch((url) => {
        expect(url).toContain("/v1/namespaces/default/sandboxes");
        return new Response(JSON.stringify({ sandboxes: [] }), { status: 200 });
      });

      const client = SandboxClient.forLocalhost();
      await client.list();
      client.close();
    });

    it("uses flat paths for cloud", async () => {
      mockFetch((url) => {
        expect(url).toContain("/sandboxes");
        expect(url).not.toContain("namespaces");
        return new Response(JSON.stringify({ sandboxes: [] }), { status: 200 });
      });

      const client = SandboxClient.forCloud({ apiKey: "key" });
      await client.list();
      client.close();
    });
  });

  describe("connect", () => {
    it("returns a Sandbox instance", () => {
      const client = SandboxClient.forLocalhost();
      const sandbox = client.connect("sbx-1");
      expect(sandbox.sandboxId).toBe("sbx-1");
      sandbox.close();
      client.close();
    });

    it("resolves ingress endpoint before first proxy request", async () => {
      const calls: string[] = [];
      mockFetch((url, init) => {
        calls.push(url);
        if (url.includes("/sandboxes/stable-name")) {
          return new Response(
            JSON.stringify({
              sandbox_id: "sbx-1",
              status: "running",
              ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
              routing_hint: "hint-1",
            }),
            { status: 200 },
          );
        }
        if (url.includes("/api/v1/health")) {
          expect(url).toBe(
            "https://sandbox.us-east-1.aws.tensorlake.ai/api/v1/health",
          );
          expect(
            (init?.headers as Record<string, string>)["X-Tensorlake-Sandbox-Id"],
          ).toBe("sbx-1");
          expect(
            (init?.headers as Record<string, string>)["X-Tensorlake-Route-Hint"],
          ).toBe("hint-1");
          return new Response(JSON.stringify({ healthy: true }), { status: 200 });
        }
        return new Response("", { status: 404 });
      });

      const client = SandboxClient.forCloud({ apiKey: "key" });
      const sandbox = client.connect("stable-name");
      const health = await sandbox.health();

      expect(health.healthy).toBe(true);
      expect(calls[0]).toContain("/sandboxes/stable-name");
      expect(calls[1]).toBe(
        "https://sandbox.us-east-1.aws.tensorlake.ai/api/v1/health",
      );
      sandbox.close();
      client.close();
    });
  });

  describe("URL routing", () => {
    it("routes sandbox create to sandbox.tensorlake.ai for cloud", async () => {
      let capturedUrl = "";
      mockFetch((url) => {
        capturedUrl = url;
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          { status: 200 },
        );
      });

      const client = new SandboxClient({ apiUrl: "https://api.tensorlake.ai", apiKey: "key" }, true);
      await client.create();
      expect(capturedUrl).toContain("sandbox.tensorlake.ai");
      expect(capturedUrl).not.toContain("api.tensorlake.ai");
      client.close();
    });

    it("routes sandbox create to sandbox.tensorlake.dev when api.tensorlake.dev is set", async () => {
      let capturedUrl = "";
      mockFetch((url) => {
        capturedUrl = url;
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          { status: 200 },
        );
      });

      const client = new SandboxClient({ apiUrl: "https://api.tensorlake.dev", apiKey: "key" }, true);
      await client.create();
      expect(capturedUrl).toContain("sandbox.tensorlake.dev");
      expect(capturedUrl).not.toContain("api.tensorlake.dev");
      client.close();
    });

    it("routes sandbox create to localhost when local", async () => {
      let capturedUrl = "";
      mockFetch((url) => {
        capturedUrl = url;
        return new Response(
          JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      await client.create();
      expect(capturedUrl).toContain("localhost");
      client.close();
    });
  });
});
