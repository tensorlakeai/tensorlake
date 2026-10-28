import { afterEach, describe, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { Sandbox } from "../src/sandbox.js";
import { RemoteAPIError } from "../src/errors.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

const info = (autoResume: boolean) => ({ traceId: "t", json: JSON.stringify({
  id: "sbx-1", name: "agent", namespace: "default", status: "suspended",
  resources: { cpus: 1, memory_mb: 512, disk_mb: 1024 }, auto_resume: autoResume,
}) });
const created = { traceId: "t", json: '{"sandbox_id":"sbx-1","status":"running"}' };

afterEach(clearNativeStub);

describe("sandbox auto-resume", () => {
  it.each([undefined, false, true])("preserves create %s through blocking and pending paths", async (autoResume) => {
    const stub = installNativeStub({ client: { createSandbox: vi.fn(async () => created) } });
    const client = SandboxClient.forLocalhost();
    for (const wait of [true, false]) {
      await client.create({ name: "agent", autoResume, wait });
      const method = wait ? stub.client.createSandbox : stub.client.createSandboxNoWait;
      const body = JSON.parse(method.mock.calls.at(-1)![0]);
      expect(body.auto_resume).toBe(autoResume);
      expect(Object.hasOwn(body, "auto_resume")).toBe(autoResume !== undefined);
    }
    client.close();
  });

  it("forwards through connected creation and handle update without waking", async () => {
    const stub = installNativeStub({ client: {
      createSandbox: vi.fn(async () => created),
      updateSandbox: vi.fn(async () => info(false)),
      getSandbox: vi.fn(async () => info(false)),
    } });
    const sandbox = await Sandbox.create({ name: "agent", autoResume: false, proxyUrl: "http://localhost:9443" });
    expect(JSON.parse(stub.client.createSandboxAndWait.mock.calls[0][0]).auto_resume).toBe(false);
    const updated = await sandbox.update({ autoResume: false });
    expect(updated.autoResume).toBe(false);
    expect(updated.status).toBe("suspended");
    expect(JSON.parse(stub.client.updateSandbox.mock.calls[0][1])).toEqual({ auto_resume: false });
    expect((await sandbox.info()).autoResume).toBe(false);
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    sandbox.close();
  });

  it("omits unset patch, releases explicitly, and rejects resize combinations", async () => {
    const stub = installNativeStub({ client: { updateSandbox: vi.fn(async () => info(true)) } });
    const client = SandboxClient.forLocalhost();
    await client.update("agent", { exposedPorts: [8080] });
    expect(JSON.parse(stub.client.updateSandbox.mock.calls[0][1])).not.toHaveProperty("auto_resume");
    expect((await client.update("agent", { autoResume: true })).autoResume).toBe(true);
    expect(JSON.parse(stub.client.updateSandbox.mock.calls[1][1])).toEqual({ auto_resume: true });
    await expect(client.update("agent", { autoResume: false, cpus: 2 })).rejects.toThrow("cannot be combined");
    expect(stub.client.updateSandbox).toHaveBeenCalledTimes(2);
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    client.close();
  });

  it("exposes the coded proxy 409 without retrying or resuming", async () => {
    const body = JSON.stringify({ error: "Automatic resume disabled", code: "SANDBOX_AUTO_RESUME_DISABLED" });
    const stub = installNativeStub({ proxy: { readFile: vi.fn(async () => {
      throw new Error(JSON.stringify({ category: "remote_api", status: 409, message: body }));
    }) } });
    const sandbox = await Sandbox.connect({ sandboxId: "agent", proxyUrl: "http://localhost:9443" });
    const error = await sandbox.readFile("/tmp/test").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RemoteAPIError);
    expect(error).toMatchObject({ statusCode: 409, code: "SANDBOX_AUTO_RESUME_DISABLED", responseMessage: body });
    expect(stub.proxy.readFile).toHaveBeenCalledOnce();
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    sandbox.close();
  });
});
