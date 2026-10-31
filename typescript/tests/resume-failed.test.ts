import { afterEach, describe, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { Sandbox } from "../src/sandbox.js";
import { SandboxResumeError, RemoteAPIError } from "../src/errors.js";
import { SandboxStatus } from "../src/models.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

const diagnosis = { message: "restore failed", debug_data: { attempt_id: 17 } };
const record = (status = "resume_failed", details: unknown = diagnosis) => ({
  id: "sbx-1", namespace: "default", status, snapshot_id: "snap-1", error_details: details,
  resources: { cpus: 1, memory_mb: 512, disk_mb: 1024 }, sandbox_url: "http://localhost:9443",
});
const traced = (value: unknown) => ({ traceId: "t", json: JSON.stringify(value) });
afterEach(clearNativeStub);

describe("recoverable resume", () => {
  it("preserves get/list diagnosis and retries only when explicitly requested (docs flow)", async () => {
    let status = "resume_failed";
    let details: unknown = diagnosis;
    const stub = installNativeStub({ client: {
      getSandbox: vi.fn(async () => traced(record(status, details))),
      listSandboxes: vi.fn(async () => traced({ sandboxes: [record()] })),
      resumeSandboxAndWait: vi.fn(async () => {
        throw new Error(JSON.stringify({ category: "resume_failed", message: JSON.stringify({
          sandbox_id: "sbx-1", snapshot_id: "snap-1", error_details: diagnosis,
        }) }));
      }),
    } });
    const client = SandboxClient.forLocalhost();
    const info = await client.get("sbx-1");
    expect(info.status).toBe(SandboxStatus.RESUME_FAILED);
    expect(info.snapshotId).toBe("snap-1");
    expect(info.errorDetails).toEqual(diagnosis);
    expect((await client.list())[0].errorDetails).toEqual(diagnosis);
    if (info.status === SandboxStatus.RESUME_FAILED) {
      await expect(client.resume("sbx-1", { timeout: 120 })).rejects.toMatchObject({
        name: "SandboxResumeError", sandboxId: "sbx-1", status: "resume_failed",
        snapshotId: "snap-1", errorDetails: diagnosis,
      });
    }
    status = "running";
    details = null;
    stub.client.resumeSandboxAndWait.mockResolvedValue(true);
    await client.resume("sbx-1", { timeout: 120 });
    expect((await client.get("sbx-1")).errorDetails).toBeUndefined();
    expect((await client.get("sbx-1")).status).toBe(SandboxStatus.RUNNING);
    expect(stub.client.resumeSandboxAndWait).toHaveBeenCalledTimes(2);
    expect(stub.client.deleteSandbox).not.toHaveBeenCalled();
    client.close();
  });

  it("stops polling accepted resumes promptly, even without diagnosis", async () => {
    const stub = installNativeStub({ client: {
      resumeSandboxAndWait: vi.fn(async () => false),
      getSandbox: vi.fn(async () => traced(record("resume_failed", null))),
    } });
    const client = SandboxClient.forLocalhost();
    await expect(client.resume("sbx-1", { timeout: 120 })).rejects.toBeInstanceOf(SandboxResumeError);
    expect(stub.client.getSandbox).toHaveBeenCalledOnce();
    expect(stub.client.resumeSandboxAndWait).toHaveBeenCalledOnce();
    expect(stub.client.deleteSandbox).not.toHaveBeenCalled();
    client.close();
  });

  it("ready and lazy connect fail without resume or delete, even with cancelOnTimeout", async () => {
    const stub = installNativeStub({ client: {
      getSandbox: vi.fn(async () => traced(record())),
      waitForSandbox: vi.fn(async () => traced(record())),
    } });
    const client = SandboxClient.forLocalhost();
    const pending = await client.create({ wait: false });
    await expect(pending.ready({ timeout: 0, cancelOnTimeout: true })).rejects.toBeInstanceOf(SandboxResumeError);
    const sandbox = client.connect("sbx-1");
    await expect(sandbox.readFile("/tmp/test")).rejects.toBeInstanceOf(SandboxResumeError);
    await expect(Sandbox.getOrCreate("sbx-1")).rejects.toBeInstanceOf(SandboxResumeError);
    expect(stub.client.resumeSandbox).not.toHaveBeenCalled();
    expect(stub.client.resumeSandboxAndWait).not.toHaveBeenCalled();
    expect(stub.client.deleteSandbox).not.toHaveBeenCalled();
    sandbox.close();
    client.close();
  });

  it("does not call pending/uncertain execution a recoverable failure", async () => {
    const stub = installNativeStub({ client: {
      resumeSandboxAndWait: vi.fn(async () => { throw new Error(JSON.stringify({ category: "remote_api", status: 409, message: "Resident resume failed" })); }),
      getSandbox: vi.fn(async () => traced(record("pending"))),
    } });
    const client = SandboxClient.forLocalhost();
    await expect(client.resume("sbx-1")).rejects.toBeInstanceOf(RemoteAPIError);
    expect(stub.client.resumeSandboxAndWait).toHaveBeenCalledOnce();
    client.close();
  });
});
