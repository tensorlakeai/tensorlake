import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { SandboxStatus, SnapshotStatus } from "../src/models.js";
import { SandboxError, SandboxNotFoundError } from "../src/errors.js";

describe("SandboxClient", () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    globalThis.fetch = vi.fn(handler as typeof fetch);
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
          ephemeral_disk_mb: 1024,
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
        expect(body.secret_names).toEqual(["my-secret"]);
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
        secretNames: ["my-secret"],
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
            secret_names: [],
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
            secret_names: [],
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
            secret_names: [],
            allow_unauthenticated_access: true,
            exposed_ports: [8080, 3000],
            sandbox_url: "https://sbx-ports.sandbox.tensorlake.ai",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-ports");
      expect(info.allowUnauthenticatedAccess).toBe(true);
      expect(info.exposedPorts).toEqual([8080, 3000]);
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
            secret_names: [],
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
                secret_names: [],
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
  });

  describe("createAndConnect", () => {
    it("fails fast when sandbox suspends during startup", async () => {
      const fetchMock = vi
        .fn()
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
              status: "suspended",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              secret_names: [],
            }),
            { status: 200 },
          ),
        );
      globalThis.fetch = fetchMock as typeof fetch;

      const client = SandboxClient.forLocalhost();
      await expect(client.createAndConnect({ startupTimeout: 1 })).rejects.toThrow(
        "Sandbox sbx-1 became suspended during startup",
      );
      expect(fetchMock).toHaveBeenCalledTimes(2);
      client.close();
    });

    it("still fails when sandbox terminates during startup", async () => {
      const fetchMock = vi
        .fn()
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
              secret_names: [],
            }),
            { status: 200 },
          ),
        );
      globalThis.fetch = fetchMock as typeof fetch;

      const client = SandboxClient.forLocalhost();
      await expect(client.createAndConnect({ startupTimeout: 1 })).rejects.toThrow(
        "Sandbox sbx-1 became terminated during startup",
      );
      expect(fetchMock).toHaveBeenCalledTimes(2);
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
            secret_names: [],
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
            secret_names: [],
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
            secret_names: [],
            allow_unauthenticated_access: false,
            exposed_ports: [8080],
            sandbox_url: "https://sbx-1.sandbox.tensorlake.ai",
          }),
          { status: 200 },
        ),
      );

      const client = SandboxClient.forLocalhost();
      const access = await client.getPortAccess("sbx-1");
      expect(access.allowUnauthenticatedAccess).toBe(false);
      expect(access.exposedPorts).toEqual([8080]);
      expect(access.sandboxUrl).toBe("https://sbx-1.sandbox.tensorlake.ai");
      client.close();
    });

    it("exposes ports by merging with existing ports", async () => {
      const fetchMock = vi
        .fn()
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              secret_names: [],
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
                secret_names: [],
                allow_unauthenticated_access: true,
                exposed_ports: [8080, 8081],
              }),
              { status: 200 },
            ),
          );
        });
      globalThis.fetch = fetchMock as typeof fetch;

      const client = SandboxClient.forLocalhost();
      const info = await client.exposePorts("sbx-1", [8081, 8080], {
        allowUnauthenticatedAccess: true,
      });
      expect(info.exposedPorts).toEqual([8080, 8081]);
      client.close();
    });

    it("unexposes ports and disables unauthenticated access when none remain", async () => {
      const fetchMock = vi
        .fn()
        .mockResolvedValueOnce(
          new Response(
            JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              secret_names: [],
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
                secret_names: [],
                allow_unauthenticated_access: false,
                exposed_ports: [],
              }),
              { status: 200 },
            ),
          );
        });
      globalThis.fetch = fetchMock as typeof fetch;

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
      await expect(client.delete("sbx-1")).resolves.toBeUndefined();
      client.close();
    });
  });

  describe("suspend", () => {
    it("suspends a sandbox", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1/suspend");
        expect(init?.method).toBe("POST");
        return new Response("", { status: 202 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.suspend("sbx-1")).resolves.toBeUndefined();
      client.close();
    });
  });

  describe("resume", () => {
    it("resumes a sandbox", async () => {
      mockFetch((url, init) => {
        expect(url).toContain("/sandboxes/sbx-1/resume");
        expect(init?.method).toBe("POST");
        return new Response("", { status: 202 });
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.resume("sbx-1")).resolves.toBeUndefined();
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
      // Pins down backwards compatibility: when contentMode is unset we
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

    it("sends snapshot_content_mode in body when contentMode is provided", async () => {
      // Regression: sandbox image builds MUST pass `filesystem_only` so
      // that restored sandboxes cold-boot (see PR #583 for the original
      // regression that broke `tl sbx new --image`).
      mockFetch((_url, init) => {
        expect(init?.method).toBe("POST");
        const body = JSON.parse(String(init?.body ?? "{}"));
        expect(body.snapshot_content_mode).toBe("filesystem_only");
        return new Response(
          JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          { status: 200 },
        );
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.snapshot("sbx-1", {
        contentMode: "filesystem_only",
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
            secret_names: [],
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
  });
});
