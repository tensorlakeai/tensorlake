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
          memory_mb: 512,
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
  });

  describe("get", () => {
    it("gets sandbox info with id mapped to sandboxId", async () => {
      mockFetch(() =>
        new Response(
          JSON.stringify({
            id: "sbx-1",
            namespace: "default",
            status: "running",
            resources: { cpus: 1, memory_mb: 512, ephemeral_disk_mb: 1024 },
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
                resources: { cpus: 1, memory_mb: 512, ephemeral_disk_mb: 1024 },
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

  describe("delete", () => {
    it("deletes a sandbox", async () => {
      mockFetch(() => new Response("", { status: 200 }));
      const client = SandboxClient.forLocalhost();
      await expect(client.delete("sbx-1")).resolves.toBeUndefined();
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
            resources: { cpus: 1, memory_mb: 512, ephemeral_disk_mb: 1024 },
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
