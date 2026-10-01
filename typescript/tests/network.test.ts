import { afterEach, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { Sandbox } from "../src/sandbox.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

afterEach(() => { clearNativeStub(); vi.restoreAllMocks(); });

it("retains canonical identity for network reads after sandbox termination", async () => {
  installNativeStub();
  const sandbox = new Sandbox({ sandboxId: "friendly-name", proxyUrl: "http://localhost:9443" });
  const client = SandboxClient.forLocalhost();
  sandbox._setLifecycleClient(client);
  sandbox._setLifecycleIdentifier("retained-sandbox-id");
  const info = vi.spyOn(client, "get").mockRejectedValue(new Error("sandbox no longer exists"));
  const status = vi.spyOn(client, "networkStatus").mockResolvedValue({ traceId: "trace", state: "stale", last_observed_at_ms: 1, allocation_id: "old", coverage: [], limitations: [] });
  try {
    expect((await sandbox.networkStatus()).state).toBe("stale");
    expect(status).toHaveBeenCalledWith("retained-sandbox-id");
    expect(info).not.toHaveBeenCalled();
  } finally { sandbox.close(); client.close(); }
});

it("reads retained network metadata with its cursor and never connects to the guest", async () => {
  const response = { events: [], from_ms: 1, to_ms: 2, next_cursor: "next" };
  const networkEvents = vi.fn(async () => ({ traceId: "trace-network", json: JSON.stringify(response) }));
  const networkStatus = vi.fn(async () => ({ traceId: "trace-status", json: JSON.stringify({ state: "unknown", last_observed_at_ms: null, allocation_id: null, coverage: [], limitations: ["No HTTP visibility"] }) }));
  const stub = installNativeStub({ client: { networkEvents, networkStatus } });
  const client = SandboxClient.forLocalhost();
  try {
    const events = await client.networkEvents("removed-sandbox", { fromMs: 1, toMs: 2, limit: 10, cursor: "opaque&cursor" });
    expect(events).toEqual({ ...response, traceId: "trace-network" });
    expect(events.traceId).toBe("trace-network");
    expect(networkEvents).toHaveBeenCalledWith("removed-sandbox", JSON.stringify({ from_ms: 1, to_ms: 2, limit: 10, cursor: "opaque&cursor" }));
    const health = await client.networkStatus("removed-sandbox");
    expect(health.state).toBe("unknown");
    expect(health.last_observed_at_ms).toBeNull();
    expect(stub.proxyCtorArgs).toEqual([]);
  } finally { client.close(); }
});


it("sends the per-sandbox opt-in on create and defaults it off", async () => {
  const createSandbox = vi.fn(async (_request: string) => ({ traceId: "trace", json: JSON.stringify({ sandbox_id: "sandbox-a", status: "running" }) }));
  installNativeStub({ client: { createSandbox } });
  const client = SandboxClient.forLocalhost();
  try {
    await client.create({ networkObservability: true });
    expect(JSON.parse(createSandbox.mock.calls[0][0] as string).network_observability).toBe(true);
    await client.create();
    expect(JSON.parse(createSandbox.mock.calls[1][0] as string).network_observability ?? false).toBe(false);
  } finally { client.close(); }
});

it("sends pool claim opt-in without requiring mounts", async () => {
  const claimSandbox = vi.fn(async (_pool: string, _request?: string) => ({ traceId: "trace", json: JSON.stringify({ sandbox_id: "sandbox-a", status: "running" }) }));
  installNativeStub({ client: { claimSandbox } });
  const client = SandboxClient.forLocalhost();
  try {
    await client.claim("pool-a", { networkObservability: true });
    expect(claimSandbox.mock.calls[0][0]).toBe("pool-a");
    expect(JSON.parse(claimSandbox.mock.calls[0][1]!).network_observability).toBe(true);
  } finally { client.close(); }
});
