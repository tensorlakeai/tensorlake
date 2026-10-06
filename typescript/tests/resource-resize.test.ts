import { afterEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { SandboxClient } from "../src/client.js";
import { SandboxError, SandboxResizeError, RemoteAPIError } from "../src/errors.js";
import { ResizeStatus, ResizeErrorReason, type UpdateSandboxOptions } from "../src/models.js";
import { installNativeStub, clearNativeStub } from "./native-stub.js";

const info = {
  id: "sb-1", namespace: "default", status: "running",
  resources: { cpus: 1, memory_mb: 1024, disk_mb: 1024 },
  resource_resize: { generation: 7, status: "pending", requested: { cpus: 2, memory_mb: 2048, disk_mb: 1024 }, error_message: null },
};
const response = () => ({ traceId: "resize-trace", json: JSON.stringify(info) });
afterEach(() => { clearNativeStub(); vi.restoreAllMocks(); });

describe("resource resize", () => {
  it.each([
    [{ cpus: 2 }, { cpus: 2 }],
    [{ memoryMb: 1001 }, { memory_mb: 1001 }],
    [{ diskMb: 2048 }, { disk_mb: 2048 }],
  ])("uses create's resource names and preserves partial targets %j", async (options, resources) => {
    const stub = installNativeStub({ client: { updateSandbox: vi.fn(async () => response()) } });
    const client = SandboxClient.forLocalhost();
    const result = await client.update("named", options);
    expect(stub.client.updateSandbox).toHaveBeenCalledWith("named", JSON.stringify({ resources }), true, 300, 1);
    expect(result.resourceResize?.generation).toBe(7);
    expect(result.resourceResize?.status).toBe(ResizeStatus.PENDING);
    expect(result.resources.memoryMb).toBe(1024);
    client.close();
  });

  it.each([
    { cpus: 1.5 }, { cpus: NaN }, { cpus: Infinity }, { cpus: 0 },
    { memoryMb: -1 }, { memoryMb: 1.5 }, { diskMb: 0 }, { diskMb: 2.5 },
    { memoryMb: true }, { diskMb: "2048" }, { cpus: null },
  ])("rejects malformed inputs before native serialization: %j", async (options) => {
    const stub = installNativeStub();
    const client = SandboxClient.forLocalhost();
    await expect(client.update("sb-1", options as UpdateSandboxOptions)).rejects.toBeInstanceOf(SandboxError);
    expect(stub.client.updateSandbox).not.toHaveBeenCalled();
    client.close();
  });

  it.each([{ network: null }, { name: "rename" }, { exposedPorts: [] }, { allowUnauthenticatedAccess: false }])("rejects mixed resource/settings updates %j", async (settings) => {
    const stub = installNativeStub();
    const client = SandboxClient.forLocalhost();
    await expect(client.update("sb-1", { cpus: 2, ...settings })).rejects.toThrow("cannot be combined");
    expect(stub.client.updateSandbox).not.toHaveBeenCalled();
    client.close();
  });

  it("forwards admission-only options and waits for an exact generation", async () => {
    const stub = installNativeStub({ client: {
      updateSandbox: vi.fn(async () => response()), waitForResourceResize: vi.fn(async () => response()),
    } });
    const client = SandboxClient.forLocalhost();
    await client.update("sb-1", { cpus: 2, wait: false, timeout: 12, pollInterval: 0.1 });
    expect(stub.client.updateSandbox).toHaveBeenCalledWith("sb-1", '{"resources":{"cpus":2}}', false, 12, 0.1);
    await client.waitForResourceResize("sb-1", 7, { timeout: 20, pollInterval: 0.2 });
    expect(stub.client.waitForResourceResize).toHaveBeenCalledWith("sb-1", 7, 20, 0.2);
    client.close();
  });

  it.each([ResizeErrorReason.FAILED, ResizeErrorReason.TIMEOUT, ResizeErrorReason.SUPERSEDED])("preserves %s generation, driver details and confirmed allocation", async (reason) => {
    installNativeStub({ client: { updateSandbox: vi.fn(async () => {
      throw new Error(JSON.stringify({ category: "resize", status: null, message: JSON.stringify({
        sandbox_id: "sb-1", generation: 7, reason, message: "ConfigurationError: below immutable boot memory", info,
      }) }));
    }) } });
    const client = SandboxClient.forLocalhost();
    const error = await client.update("sb-1", { memoryMb: 512 }).catch((error: unknown) => error);
    expect(error).toBeInstanceOf(SandboxResizeError);
    expect(error).toMatchObject({ reason, generation: 7, sandboxId: "sb-1", info: { resources: { memoryMb: 1024 } } });
    expect((error as Error).message).toContain("below immutable boot memory");
    expect((error as SandboxResizeError).confirmedResources?.memoryMb).toBe(1024);
    expect((error as Error).message).toContain("1 CPUs, 1024 MiB memory, 1024 MiB disk");
    client.close();
  });

  it("forwards resize and generation waits on sandbox objects", async () => {
    const stub = installNativeStub({ client: {
      updateSandbox: vi.fn(async () => response()),
      waitForResourceResize: vi.fn(async () => response()),
    } });
    const sandbox = await Sandbox.connect({ sandboxId: "sb-1", apiUrl: "http://localhost:8900" });
    await sandbox.update({ cpus: 2, memoryMb: 2048, diskMb: 4096, wait: false });
    expect(stub.client.updateSandbox).toHaveBeenCalledWith("sb-1", JSON.stringify({ resources: { cpus: 2, memory_mb: 2048, disk_mb: 4096 } }), false, 300, 1);
    await sandbox.waitForResourceResize(7, { timeout: 12 });
    expect(stub.client.waitForResourceResize).toHaveBeenCalledWith("sb-1", 7, 12, 1);
    sandbox.close();
  });

  it("supports typed resize errors without an observation", () => {
    const error = new SandboxResizeError({ sandboxId: "sb-1", generation: 7, reason: ResizeErrorReason.TIMEOUT, message: "wait timed out" });
    expect(error.confirmedResources).toBeUndefined();
    expect(error.message).toContain("last confirmed allocation: unavailable");
  });

  it("preserves policy rejection details", async () => {
    installNativeStub({ client: { updateSandbox: vi.fn(async () => { throw new Error(JSON.stringify({ category: "remote_api", status: 422, message: "host-specific CPU ceiling exceeded" })); }) } });
    const client = SandboxClient.forLocalhost();
    await expect(client.update("sb-1", { cpus: 32 })).rejects.toBeInstanceOf(RemoteAPIError);
    client.close();
  });
});
