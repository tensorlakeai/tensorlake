import { afterEach, describe, expect, it, vi } from "vitest";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { SandboxClient } from "../src/client.js";
import { SandboxStatus, SnapshotStatus } from "../src/models.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

/** Build the native error a non-2xx HTTP response now surfaces from Rust. */
function nativeError(status: number, message: string): Error {
  return new Error(
    JSON.stringify({ category: "remote_api", status, message }),
  );
}

describe("SandboxClient", () => {
  let tempDirs: string[] = [];

  afterEach(() => {
    clearNativeStub();
    vi.restoreAllMocks();
    const dirs = tempDirs;
    tempDirs = [];
    return Promise.all(dirs.map((dir) => rm(dir, { recursive: true, force: true })));
  });

  async function tempFile(contents: string | Uint8Array): Promise<string> {
    const dir = await mkdtemp(join(tmpdir(), "tensorlake-sdk-test-"));
    tempDirs.push(dir);
    const path = join(dir, "cloud-init.yaml");
    await writeFile(path, contents);
    return path;
  }

  describe("construction", () => {
    it("creates cloud client with defaults", () => {
      installNativeStub();
      const client = SandboxClient.forCloud({ apiKey: "key" });
      expect(client).toBeInstanceOf(SandboxClient);
      client.close();
    });

    it("creates localhost client", () => {
      installNativeStub();
      const client = SandboxClient.forLocalhost();
      expect(client).toBeInstanceOf(SandboxClient);
      client.close();
    });
  });

  describe("create", () => {
    it("creates a sandbox with defaults", async () => {
      const stub = installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.resources).toEqual({
              cpus: 1.0,
              memory_mb: 1024,
            });
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create();
      expect(result.sandboxId).toBe("sbx-1");
      expect(result.status).toBe(SandboxStatus.PENDING);
      expect(stub.client.createSandbox).toHaveBeenCalledOnce();
      client.close();
    });

    it("creates a sandbox with GPU resources", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.resources.gpus).toEqual([{ count: 1, model: "A10" }]);
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-gpu", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create({ gpus: 1, gpuModel: "A10" });
      expect(result.sandboxId).toBe("sbx-gpu");
      client.close();
    });

    it("rejects partial GPU resources", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(),
        },
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.create({ gpus: 1 })).rejects.toThrow(
        "gpus and gpuModel",
      );
      client.close();
    });

    it("rejects non-A10 GPU resources", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(),
        },
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.create({ gpus: 1, gpuModel: "H100" })).rejects.toThrow(
        "only A10",
      );
      client.close();
    });

    it("creates a sandbox with custom options", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
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
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-2", status: "pending" }),
            };
          }),
        },
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
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.name).toBe("my-sandbox");
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-named", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.create({ name: "my-sandbox" });
      expect(result.sandboxId).toBe("sbx-named");
      client.close();
    });

    it("omits name from request body when not provided", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.name).toBeUndefined();
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.create();
      client.close();
    });

    it("returns readiness timeout responses without retrying", async () => {
      const createSandbox = vi.fn(async () => ({
        traceId: "t",
        json: JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
      }));
      installNativeStub({ client: { createSandbox } });

      const client = SandboxClient.forLocalhost();
      const result = await client.create();
      expect(result.sandboxId).toBe("sbx-timeout");
      expect(result.status).toBe(SandboxStatus.TIMEOUT);
      expect(createSandbox).toHaveBeenCalledOnce();
      client.close();
    });

    it("does not retry rate-limited create responses", async () => {
      const createSandbox = vi.fn(async () => {
        throw nativeError(429, "rate limited");
      });
      installNativeStub({ client: { createSandbox } });

      const client = SandboxClient.forLocalhost();
      await expect(client.create()).rejects.toThrow("rate limited");
      expect(createSandbox).toHaveBeenCalledOnce();
      client.close();
    });

    it("reads cloudInit file path and sends cloud_init_base64", async () => {
      const path = await tempFile("#cloud-config\n");
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.cloud_init_base64).toBe(
              Buffer.from("#cloud-config\n").toString("base64"),
            );
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-cloud-init", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.create({ cloudInit: path });
      client.close();
    });

    it("encodes cloudInit URL as include user-data", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.cloud_init_base64).toBe(
              Buffer.from("#include\nhttps://example.com/cloud-init.yaml\n").toString(
                "base64",
              ),
            );
            return {
              traceId: "t",
              json: JSON.stringify({ sandbox_id: "sbx-cloud-init-url", status: "pending" }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.create({ cloudInit: "https://example.com/cloud-init.yaml" });
      client.close();
    });

    it("rejects invalid cloudInit URLs before sending a request", async () => {
      const createSandbox = vi.fn(async () => {
        throw new Error("request should not be sent");
      });
      installNativeStub({ client: { createSandbox } });

      const client = SandboxClient.forLocalhost();
      await expect(
        client.create({ cloudInit: "ftp://example.com/cloud-init.yaml" }),
      ).rejects.toThrow("HTTP(S) URL");
      expect(createSandbox).not.toHaveBeenCalled();
      client.close();
    });

    it("sends cloudInit file with snapshotId", async () => {
      const path = await tempFile("#cloud-config\n");
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.snapshot_id).toBe("snap-1");
            expect(body.cloud_init_base64).toBe(
              Buffer.from("#cloud-config\n").toString("base64"),
            );
            return {
              traceId: "t",
              json: JSON.stringify({
                sandbox_id: "sbx-cloud-init-snapshot",
                status: "pending",
              }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.create({ snapshotId: "snap-1", cloudInit: path });
      client.close();
    });

    it("sends cloudInit URL with snapshotId", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.snapshot_id).toBe("snap-1");
            expect(body.cloud_init_base64).toBe(
              Buffer.from("#include\nhttps://example.com/cloud-init.yaml\n").toString(
                "base64",
              ),
            );
            return {
              traceId: "t",
              json: JSON.stringify({
                sandbox_id: "sbx-cloud-init-url-snapshot",
                status: "pending",
              }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.create({
        snapshotId: "snap-1",
        cloudInit: "https://example.com/cloud-init.yaml",
      });
      client.close();
    });
  });

  describe("get", () => {
    it("gets sandbox info with id mapped to sandboxId", async () => {
      const stub = installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              created_at: 1700000000,
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-1");
      expect(info.sandboxId).toBe("sbx-1");
      expect(info.status).toBe(SandboxStatus.RUNNING);
      expect(info.createdAt).toBeInstanceOf(Date);
      expect(stub.client.getSandbox).toHaveBeenCalledWith("sbx-1");
      client.close();
    });

    it("maps name field from response", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-named",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              name: "my-sandbox",
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-named");
      expect(info.name).toBe("my-sandbox");
      client.close();
    });

    it("maps port access fields from response", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-ports",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: true,
              exposed_ports: [8080, 3000],
              ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
              sandbox_url: "https://sbx-ports.sandbox.tensorlake.ai",
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-ports");
      expect(info.allowUnauthenticatedAccess).toBe(true);
      expect(info.exposedPorts).toEqual([8080, 3000]);
      expect(info.ingressEndpoint).toBe("https://sandbox.us-east-1.aws.tensorlake.ai");
      expect(info.sandboxUrl).toBe("https://sbx-ports.sandbox.tensorlake.ai");
      client.close();
    });

    it("returns undefined name when absent from response", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.get("sbx-1");
      expect(info.name).toBeUndefined();
      client.close();
    });
  });

  describe("list", () => {
    it("lists sandboxes", async () => {
      installNativeStub({
        client: {
          listSandboxes: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              sandboxes: [
                {
                  id: "sbx-1",
                  namespace: "default",
                  status: "running",
                  resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                },
              ],
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const list = await client.list();
      expect(list).toHaveLength(1);
      expect(list[0].sandboxId).toBe("sbx-1");
      client.close();
    });

    it("returns traceId on the array", async () => {
      installNativeStub({
        client: {
          listSandboxes: vi.fn(async () => ({
            traceId: "trace-list",
            json: JSON.stringify({ sandboxes: [] }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const list = await client.list();
      expect(typeof list.traceId).toBe("string");
      expect(list.traceId.length).toBeGreaterThan(0);
      client.close();
    });
  });

  describe("update", () => {
    it("updates an unnamed sandbox with a new name", async () => {
      const stub = installNativeStub({
        client: {
          updateSandbox: vi.fn(async (id: string, json: string) => {
            expect(id).toBe("sbx-1");
            const body = JSON.parse(json);
            expect(body.name).toBe("my-new-name");
            return {
              traceId: "t",
              json: JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                name: "my-new-name",
              }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.update("sbx-1", { name: "my-new-name" });
      expect(info.sandboxId).toBe("sbx-1");
      expect(info.name).toBe("my-new-name");
      expect(stub.client.updateSandbox).toHaveBeenCalledOnce();
      client.close();
    });

    it("updates sandbox port access settings", async () => {
      installNativeStub({
        client: {
          updateSandbox: vi.fn(async (id: string, json: string) => {
            expect(id).toBe("sbx-1");
            const body = JSON.parse(json);
            expect(body.allow_unauthenticated_access).toBe(true);
            expect(body.exposed_ports).toEqual([8080, 8081]);
            return {
              traceId: "t",
              json: JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                allow_unauthenticated_access: true,
                exposed_ports: [8080, 8081],
              }),
            };
          }),
        },
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
      installNativeStub();
      const client = SandboxClient.forLocalhost();
      await expect(client.update("sbx-1", {})).rejects.toThrow(
        "At least one sandbox update field must be provided.",
      );
      client.close();
    });
  });

  describe("port management", () => {
    it("reads current port access", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: false,
              exposed_ports: [8080],
              ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
              sandbox_url: "https://sbx-1.sandbox.tensorlake.ai",
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const access = await client.getPortAccess("sbx-1");
      expect(access.allowUnauthenticatedAccess).toBe(false);
      expect(access.exposedPorts).toEqual([8080]);
      expect(access.ingressEndpoint).toBe("https://sandbox.us-east-1.aws.tensorlake.ai");
      expect(access.sandboxUrl).toBe("https://sbx-1.sandbox.tensorlake.ai");
      client.close();
    });

    it("exposes ports by merging with existing ports", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: false,
              exposed_ports: [8080],
            }),
          })),
          updateSandbox: vi.fn(async (_id: string, json: string) => {
            const body = JSON.parse(json);
            expect(body.allow_unauthenticated_access).toBe(true);
            expect(body.exposed_ports).toEqual([8080, 8081]);
            return {
              traceId: "t",
              json: JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                allow_unauthenticated_access: true,
                exposed_ports: [8080, 8081],
              }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.exposePorts("sbx-1", [8081, 8080], {
        allowUnauthenticatedAccess: true,
      });
      expect(info.exposedPorts).toEqual([8080, 8081]);
      client.close();
    });

    it("unexposes ports and disables unauthenticated access when none remain", async () => {
      installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "running",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              allow_unauthenticated_access: true,
              exposed_ports: [8080],
            }),
          })),
          updateSandbox: vi.fn(async (_id: string, json: string) => {
            const body = JSON.parse(json);
            expect(body.allow_unauthenticated_access).toBe(false);
            expect(body.exposed_ports).toEqual([]);
            return {
              traceId: "t",
              json: JSON.stringify({
                id: "sbx-1",
                namespace: "default",
                status: "running",
                resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
                allow_unauthenticated_access: false,
                exposed_ports: [],
              }),
            };
          }),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.unexposePorts("sbx-1", [8080]);
      expect(info.exposedPorts).toEqual([]);
      expect(info.allowUnauthenticatedAccess).toBe(false);
      client.close();
    });

    it("rejects reserved management port 9501", async () => {
      installNativeStub();
      const client = SandboxClient.forLocalhost();
      await expect(client.exposePorts("sbx-1", [9501])).rejects.toThrow(
        "port 9501 is reserved for sandbox management",
      );
      client.close();
    });
  });

  describe("delete", () => {
    it("deletes a sandbox", async () => {
      const stub = installNativeStub();
      const client = SandboxClient.forLocalhost();
      await client.delete("sbx-1");
      expect(stub.client.deleteSandbox).toHaveBeenCalledWith("sbx-1");
      client.close();
    });
  });

  describe("suspend", () => {
    it("sends a suspend request with wait=false", async () => {
      const stub = installNativeStub();

      const client = SandboxClient.forLocalhost();
      await expect(client.suspend("sbx-1", { wait: false })).resolves.toBeUndefined();
      expect(stub.client.suspendSandbox).toHaveBeenCalledWith("sbx-1");
      client.close();
    });

    it("polls until Suspended when wait=true (default)", async () => {
      const stub = installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              status: "suspended",
              namespace: "default",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.suspend("sbx-1")).resolves.toBeUndefined();
      expect(stub.client.suspendSandbox).toHaveBeenCalledWith("sbx-1");
      expect(stub.client.getSandbox).toHaveBeenCalled();
      client.close();
    });
  });

  describe("resume", () => {
    it("sends a resume request with wait=false", async () => {
      const stub = installNativeStub();

      const client = SandboxClient.forLocalhost();
      await expect(client.resume("sbx-1", { wait: false })).resolves.toBeUndefined();
      expect(stub.client.resumeSandbox).toHaveBeenCalledWith("sbx-1");
      client.close();
    });

    it("polls until Running when wait=true (default)", async () => {
      const stub = installNativeStub({
        client: {
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              status: "running",
              namespace: "default",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      await expect(client.resume("sbx-1")).resolves.toBeUndefined();
      expect(stub.client.resumeSandbox).toHaveBeenCalledWith("sbx-1");
      expect(stub.client.getSandbox).toHaveBeenCalled();
      client.close();
    });
  });

  describe("claim", () => {
    it("claims from pool", async () => {
      const stub = installNativeStub({
        client: {
          claimSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-3", status: "running" }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.claim("pool-1");
      expect(result.sandboxId).toBe("sbx-3");
      expect(stub.client.claimSandbox).toHaveBeenCalledWith("pool-1");
      client.close();
    });

    it("returns readiness timeout responses without retrying", async () => {
      const claimSandbox = vi.fn(async () => ({
        traceId: "t",
        json: JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
      }));
      installNativeStub({ client: { claimSandbox } });

      const client = SandboxClient.forLocalhost();
      const result = await client.claim("pool-1");
      expect(result.sandboxId).toBe("sbx-timeout");
      expect(result.status).toBe(SandboxStatus.TIMEOUT);
      expect(claimSandbox).toHaveBeenCalledOnce();
      client.close();
    });

    it("does not retry rate-limited claim responses", async () => {
      const claimSandbox = vi.fn(async () => {
        throw nativeError(429, "rate limited");
      });
      installNativeStub({ client: { claimSandbox } });

      const client = SandboxClient.forLocalhost();
      await expect(client.claim("pool-1")).rejects.toThrow("rate limited");
      expect(claimSandbox).toHaveBeenCalledOnce();
      client.close();
    });
  });

  describe("copy", () => {
    it("live-copies a sandbox and maps partial failures", async () => {
      const copySandbox = vi.fn(async (sandboxId: string, times: number) => {
        expect(sandboxId).toBe("sbx-1");
        expect(times).toBe(2);
        return {
          traceId: "trace-copy",
          json: JSON.stringify({
            source_sandbox_id: "sbx-1",
            sandboxes: [
              { sandbox_id: "copy-1", status: "running" },
              { sandbox_id: "copy-2", status: "failed", reason: "no capacity" },
            ],
          }),
        };
      });
      // copy() clones the client when a requestTimeout is set; the cloned client
      // builds a fresh native client, so wire the same fn onto every client.
      installNativeStub({ client: { copySandbox } });

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
      installNativeStub();
      const client = SandboxClient.forLocalhost();
      await expect(client.copy("sbx-1", { times: 0 })).rejects.toThrow(
        "times must be a positive integer",
      );
      client.close();
    });
  });

  describe("createAndConnect", () => {
    it("uses ingress endpoint from running create response", async () => {
      const stub = installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              sandbox_id: "sbx-1",
              status: "running",
              routing_hint: "hint-1",
              ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
            }),
          })),
        },
      });

      const client = SandboxClient.forCloud({ apiKey: "key" });
      const sandbox = await client.createAndConnect();
      expect(sandbox.sandboxId).toBe("sbx-1");
      // The proxy is minted from the shared native client via connectProxy with
      // the ingress endpoint resolved from the create response.
      expect(stub.client.connectProxy).toHaveBeenCalledWith(
        "https://sandbox.us-east-1.aws.tensorlake.ai",
        "sbx-1",
        "hint-1",
        expect.anything(),
      );
      sandbox.close();
      client.close();
    });

    it("uses per-call requestTimeout for the initial create request", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({ requestTimeout: 10 });
      expect(sandbox.sandboxId).toBe("sbx-1");
      // The per-call requestTimeout flows to the cloned native client's ctor
      // (7th arg, in seconds).
      expect(client).toBeDefined();
      client.close();
      sandbox.close();
    });

    it("uses startupTimeout as a compatibility alias for requestTimeout", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({ startupTimeout: 12 });
      expect(sandbox.sandboxId).toBe("sbx-1");
      client.close();
      sandbox.close();
    });

    it("prefers requestTimeout over startupTimeout", async () => {
      const stub = installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-1", status: "running" }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      const sandbox = await client.createAndConnect({
        requestTimeout: 15,
        startupTimeout: 12,
      });
      expect(sandbox.sandboxId).toBe("sbx-1");
      // The cloned native client used for the create request is built with the
      // 15s timeout (7th ctor arg).
      expect(stub.clientCtorArgs[6]).toBe(15);
      client.close();
      sandbox.close();
    });

    it("deletes the sandbox returned by a readiness timeout response", async () => {
      const deleteSandbox = vi.fn(async () => "t");
      installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-timeout", status: "timeout" }),
          })),
          deleteSandbox,
        },
      });

      const client = SandboxClient.forLocalhost({ requestTimeout: 300 });
      await expect(
        client.createAndConnect({ requestTimeout: 10 }),
      ).rejects.toThrow("Sandbox sbx-timeout did not start within 10s");
      expect(deleteSandbox).toHaveBeenCalledWith("sbx-timeout");
      client.close();
    });

    it("includes sandbox errorDetails in startup failures", async () => {
      installNativeStub({
        client: {
          createSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandbox_id: "sbx-1", status: "pending" }),
          })),
          getSandbox: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "sbx-1",
              namespace: "default",
              status: "terminated",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              error_details: { message: "failed to pull image tensorlake/missing-image" },
            }),
          })),
        },
      });

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
      const stub = installNativeStub({
        client: {
          createSnapshot: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const result = await client.snapshot("sbx-1");
      expect(result.snapshotId).toBe("snap-1");
      expect(result.status).toBe(SnapshotStatus.IN_PROGRESS);
      expect(stub.client.createSnapshot).toHaveBeenCalledWith("sbx-1", null);
      client.close();
    });

    it("passes a null snapshot type when no options are provided", async () => {
      // Pins down backwards compatibility: when snapshotType is unset we pass
      // an explicit null so the native client omits it from the wire request.
      const createSnapshot = vi.fn(async () => ({
        traceId: "t",
        json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
      }));
      installNativeStub({ client: { createSnapshot } });

      const client = SandboxClient.forLocalhost();
      await client.snapshot("sbx-1");
      expect(createSnapshot).toHaveBeenCalledWith("sbx-1", null);
      client.close();
    });

    it("sends snapshot_type when snapshotType is provided", async () => {
      // Regression: sandbox image builds MUST pass `filesystem` so that
      // restored sandboxes cold-boot (see PR #583 for the original
      // regression that broke `tl sbx new --image`).
      const createSnapshot = vi.fn(async () => ({
        traceId: "t",
        json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
      }));
      installNativeStub({ client: { createSnapshot } });

      const client = SandboxClient.forLocalhost();
      const result = await client.snapshot("sbx-1", {
        snapshotType: "filesystem",
      });
      expect(result.snapshotId).toBe("snap-1");
      expect(createSnapshot).toHaveBeenCalledWith("sbx-1", "filesystem");
      client.close();
    });

    it("gets snapshot info", async () => {
      installNativeStub({
        client: {
          getSnapshot: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "snap-1",
              namespace: "default",
              sandbox_id: "sbx-1",
              base_image: "python:3.12",
              status: "completed",
              snapshot_type: "filesystem",
              created_at: 1700000000,
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.getSnapshot("snap-1");
      expect(info.snapshotId).toBe("snap-1");
      expect(info.baseImage).toBe("python:3.12");
      expect(info.status).toBe(SnapshotStatus.COMPLETED);
      expect(info.snapshotType).toBe("filesystem");
      client.close();
    });

    it("snapshotAndWait returns on local_ready by default", async () => {
      installNativeStub({
        client: {
          createSnapshot: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          })),
          getSnapshot: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "snap-1",
              namespace: "default",
              sandbox_id: "sbx-1",
              base_image: "python:3.12",
              status: "local_ready",
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.snapshotAndWait("sbx-1");
      expect(info.status).toBe(SnapshotStatus.LOCAL_READY);
      expect(info.snapshotUri).toBeUndefined();
      client.close();
    });

    it("snapshotAndWait can wait for completed snapshots", async () => {
      let getCalls = 0;
      installNativeStub({
        client: {
          createSnapshot: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ snapshot_id: "snap-1", status: "in_progress" }),
          })),
          getSnapshot: vi.fn(async () => {
            getCalls += 1;
            return {
              traceId: "t",
              json: JSON.stringify({
                id: "snap-1",
                namespace: "default",
                sandbox_id: "sbx-1",
                base_image: "python:3.12",
                status: getCalls === 1 ? "local_ready" : "completed",
                snapshot_uri: "s3://snap-1.tar.zst",
              }),
            };
          }),
        },
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
      installNativeStub({
        client: {
          listSnapshots: vi.fn(async () => ({
            traceId: "trace-snaps",
            json: JSON.stringify({ snapshots: [] }),
          })),
        },
      });

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
      installNativeStub({
        client: {
          createPool: vi.fn(async (json: string) => {
            const body = JSON.parse(json);
            expect(body.image).toBe("node:20");
            expect(body.max_containers).toBe(5);
            return {
              traceId: "t",
              json: JSON.stringify({ pool_id: "pool-1", namespace: "default" }),
            };
          }),
        },
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
      const stub = installNativeStub({
        client: {
          getPool: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({
              id: "pool-1",
              namespace: "default",
              image: "node:20",
              resources: { cpus: 1, memory_mb: 1024, ephemeral_disk_mb: 1024 },
              timeout_secs: 0,
            }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const info = await client.getPool("pool-1");
      expect(info.poolId).toBe("pool-1");
      expect(info.image).toBe("node:20");
      expect(stub.client.getPool).toHaveBeenCalledWith("pool-1");
      client.close();
    });

    it("listPools returns traceId on the array", async () => {
      installNativeStub({
        client: {
          listPools: vi.fn(async () => ({
            traceId: "trace-pools",
            json: JSON.stringify({ pools: [] }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      const pools = await client.listPools();
      expect(Array.isArray(pools)).toBe(true);
      expect(typeof pools.traceId).toBe("string");
      expect(pools.traceId.length).toBeGreaterThan(0);
      client.close();
    });
  });

  describe("native client construction", () => {
    it("builds the native client with the localhost api url and namespace", async () => {
      const stub = installNativeStub({
        client: {
          listSandboxes: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandboxes: [] }),
          })),
        },
      });

      const client = SandboxClient.forLocalhost();
      await client.list();
      // ctor args: (apiUrl, apiKey, orgId, projectId, namespace, userAgent, timeoutSec)
      expect(stub.clientCtorArgs[0]).toBe("http://localhost:8900");
      expect(stub.clientCtorArgs[4]).toBe("default");
      client.close();
    });

    it("builds the native client with the cloud api url", async () => {
      const stub = installNativeStub({
        client: {
          listSandboxes: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ sandboxes: [] }),
          })),
        },
      });

      const client = SandboxClient.forCloud({ apiKey: "key" });
      await client.list();
      expect(stub.clientCtorArgs[0]).toBe("https://api.tensorlake.ai");
      expect(stub.clientCtorArgs[1]).toBe("key");
      client.close();
    });
  });

  describe("connect", () => {
    it("returns a Sandbox instance", () => {
      installNativeStub();
      const client = SandboxClient.forLocalhost();
      const sandbox = client.connect("sbx-1");
      expect(sandbox.sandboxId).toBe("sbx-1");
      sandbox.close();
      client.close();
    });

    it("resolves ingress endpoint before first proxy request", async () => {
      const stub = installNativeStub({
        client: {
          getSandbox: vi.fn(async (id: string) => {
            expect(id).toBe("stable-name");
            return {
              traceId: "t",
              json: JSON.stringify({
                sandbox_id: "sbx-1",
                status: "running",
                ingress_endpoint: "https://sandbox.us-east-1.aws.tensorlake.ai",
                routing_hint: "hint-1",
              }),
            };
          }),
        },
        proxy: {
          health: vi.fn(async () => ({
            traceId: "t",
            json: JSON.stringify({ healthy: true }),
          })),
        },
      });

      const client = SandboxClient.forCloud({ apiKey: "key" });
      const sandbox = client.connect("stable-name");
      const health = await sandbox.health();

      expect(health.healthy).toBe(true);
      // The lazy resolver resolves via getSandbox(name) before the first proxy op.
      expect(stub.client.getSandbox).toHaveBeenCalledWith("stable-name");
      // After resolution the proxy is reconnected with the resolved ingress
      // endpoint and canonical id.
      expect(stub.client.connectProxy).toHaveBeenLastCalledWith(
        "https://sandbox.us-east-1.aws.tensorlake.ai",
        "sbx-1",
        "hint-1",
        null,
      );
      expect(stub.proxy.health).toHaveBeenCalledOnce();
      sandbox.close();
      client.close();
    });
  });
});
