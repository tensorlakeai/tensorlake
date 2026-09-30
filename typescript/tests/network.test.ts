import { afterEach, expect, it, vi } from "vitest";
import { SandboxClient } from "../src/client.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

afterEach(clearNativeStub);

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
