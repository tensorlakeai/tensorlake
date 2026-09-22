import { afterEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

const info = (status = "running", sandboxUrl: string | null = "https://sandbox.test") => ({
  traceId: "trace-ready",
  json: JSON.stringify({ sandbox_id: "sandbox-id", name: "session-name", status, sandbox_url: sandboxUrl }),
});
const failure = (status: number, message = "unavailable") => new Error(JSON.stringify({ category: "remote_api", status, message }));

// Use the real TypeScript facade and native error translation. No user command
// should be sent while establishing readiness.
describe("connect readiness", () => {
  afterEach(() => { clearNativeStub(); vi.restoreAllMocks(); });

  it("resumes by name without requiring a pre-existing proxy URL", async () => {
    const get = vi.fn().mockResolvedValueOnce(info("suspended", null)).mockResolvedValue(info());
    const stub = installNativeStub({ client: { getSandbox: get } });
    const sandbox = await Sandbox.connect({ sandboxId: "session-name", requestTimeout: 2 });
    expect(stub.client.resumeSandbox).toHaveBeenCalledOnce();
    expect(stub.client.resumeSandbox).toHaveBeenCalledWith("sandbox-id");
    expect(stub.proxy.health).toHaveBeenCalledOnce();
    expect(stub.proxy.runProcess).not.toHaveBeenCalled();
    expect(sandbox.sandboxId).toBe("sandbox-id");
    expect(stub.client.connectProxy).toHaveBeenLastCalledWith("https://sandbox.test", "sandbox-id", null, 2);
    sandbox.close();
  });

  it("retries only the read-only health probe and uses fresh routing", async () => {
    const stub = installNativeStub({ client: { getSandbox: vi.fn().mockResolvedValue(info()) } });
    stub.proxy.health.mockRejectedValueOnce(failure(502));
    const sandbox = await Sandbox.connect({ sandboxId: "sandbox-id" });
    expect(stub.proxy.health).toHaveBeenCalledTimes(2);
    expect(stub.proxy.runProcess).not.toHaveBeenCalled();
    expect(stub.client.connectProxy).toHaveBeenLastCalledWith("https://sandbox.test", "sandbox-id", null, null);
    sandbox.close();
  });

  it("waits through suspending and pending states", async () => {
    const get = vi.fn().mockResolvedValueOnce(info("suspending", null))
      .mockResolvedValueOnce(info("suspended", null)).mockResolvedValueOnce(info("pending", null))
      .mockResolvedValue(info());
    const stub = installNativeStub({ client: { getSandbox: get } });
    const sandbox = await Sandbox.connect({ sandboxId: "sandbox-id" });
    expect(stub.client.resumeSandbox).toHaveBeenCalledOnce();
    expect(stub.proxy.health).toHaveBeenCalledOnce();
    sandbox.close();
  });

  it("waits for a concurrent resume winner", async () => {
    const stub = installNativeStub({ client: {
      getSandbox: vi.fn().mockResolvedValueOnce(info("suspended", null))
        .mockResolvedValueOnce(info("pending", null)).mockResolvedValue(info()),
      resumeSandbox: vi.fn().mockRejectedValue(failure(400, "already resuming")),
    } });
    const sandbox = await Sandbox.connect({ sandboxId: "sandbox-id" });
    expect(stub.client.resumeSandbox).toHaveBeenCalledOnce();
    sandbox.close();
  });

  it("preserves quota failures while still suspended", async () => {
    const stub = installNativeStub({ client: {
      getSandbox: vi.fn().mockResolvedValue(info("suspended", null)),
      resumeSandbox: vi.fn().mockRejectedValue(failure(400, "quota exceeded")),
    } });
    await expect(Sandbox.connect({ sandboxId: "sandbox-id" })).rejects.toThrow(/quota exceeded/);
    expect(stub.proxy.health).not.toHaveBeenCalled();
  });

  it.each([401, 403, 404, 409, 500])("does not retry unrelated health error %s", async (status) => {
    const stub = installNativeStub({ client: { getSandbox: vi.fn().mockResolvedValue(info()) } });
    stub.proxy.health.mockRejectedValue(failure(status));
    await expect(Sandbox.connect({ sandboxId: "sandbox-id" })).rejects.toThrow();
    expect(stub.proxy.health).toHaveBeenCalledOnce();
  });

  it("fails for terminated sandboxes without resuming", async () => {
    const stub = installNativeStub({ client: { getSandbox: vi.fn().mockResolvedValue(info("terminated", null)) } });
    await expect(Sandbox.connect({ sandboxId: "sandbox-id" })).rejects.toThrow(/terminated/);
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    expect(stub.proxy.health).not.toHaveBeenCalled();
  });

  it("leaves passive attachment passive", async () => {
    const stub = installNativeStub();
    const sandbox = await Sandbox.connect({ sandboxId: "sandbox-id", resume: false });
    expect(stub.client.getSandbox).not.toHaveBeenCalled();
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    expect(stub.proxy.health).not.toHaveBeenCalled();
    sandbox.close();
  });

  it("bounds waiting for readiness", async () => {
    const stub = installNativeStub({ client: { getSandbox: vi.fn().mockResolvedValue(info("pending", null)) } });
    await expect(Sandbox.connect({ sandboxId: "sandbox-id", requestTimeout: 0.01 })).rejects.toThrow(/within 0.01s/);
    expect(stub.proxy.runProcess).not.toHaveBeenCalled();
  });
});
