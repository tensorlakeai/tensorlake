import { afterEach, describe, expect, it } from "vitest";
import {
  __setNativeSandboxBindingForTest,
  type NativeSandboxBinding,
} from "../src/native-sandbox.js";
import { UsageClient } from "../src/usage.js";

type Call = { method: string; last: string | null };

/** Stand in for the napi NativeUsageClient; the Rust client owns the wire. */
function installUsageStub(responses: { summary?: unknown; sandboxes?: unknown; error?: Error }) {
  const calls: Call[] = [];
  let constructed: unknown[] = [];
  __setNativeSandboxBindingForTest({
    NativeUsageClient: class {
      constructor(...args: unknown[]) {
        constructed = args;
      }
      async summary(last: string | null) {
        calls.push({ method: "summary", last });
        if (responses.error) throw responses.error;
        return { traceId: "trace", json: JSON.stringify(responses.summary) };
      }
      async sandboxes(last: string | null) {
        calls.push({ method: "sandboxes", last });
        if (responses.error) throw responses.error;
        return { traceId: "trace", json: JSON.stringify(responses.sandboxes) };
      }
    },
  } as unknown as NativeSandboxBinding);
  return { calls, constructed: () => constructed };
}

const EXPORT = {
  windowStart: "2026-10-01T00:00:00.000Z",
  windowEnd: "2026-10-28T12:00:00.000Z",
  attributedUsageCharge: { currency: "usd", amountMinor: "1000", basis: "invoice" },
  includedUsage: null,
  attributedUsageValue: null,
  sandboxes: [{ sandboxId: "sb-1" }, { sandboxId: "sb-2" }],
};

describe("UsageClient", () => {
  afterEach(() => {
    __setNativeSandboxBindingForTest(undefined);
  });

  it("reads sandboxes through the native client", async () => {
    const stub = installUsageStub({ sandboxes: EXPORT });

    const client = new UsageClient({
      apiUrl: "http://localhost:8900",
      apiKey: "key",
      organizationId: "org-1",
    });
    const usage = await client.sandboxes({ last: "30d" });

    expect(stub.constructed().slice(0, 3)).toEqual(["http://localhost:8900", "key", "org-1"]);
    expect(stub.calls).toEqual([{ method: "sandboxes", last: "30d" }]);
    expect(usage.sandboxes.map((s) => s.sandboxId)).toEqual(["sb-1", "sb-2"]);
    expect(usage.attributedUsageCharge?.basis).toBe("invoice");
  });

  it("reads the current period summary without a lookback", async () => {
    const stub = installUsageStub({
      summary: { computeUnits: "42.5", runningSandboxes: 3, includedUsage: null },
    });

    const client = new UsageClient({ apiKey: "key", organizationId: "org-1" });
    const summary = await client.summary();

    expect(stub.calls).toEqual([{ method: "summary", last: null }]);
    expect(summary.computeUnits).toBe("42.5");
    expect(summary.includedUsage).toBeNull();
  });

  it("surfaces native errors", async () => {
    installUsageStub({
      error: new Error(
        JSON.stringify({
          category: "sdk_usage",
          message: "incomplete sandbox usage export: missing end record",
        }),
      ),
    });

    const client = new UsageClient({ apiKey: "key", organizationId: "org-1" });
    await expect(client.sandboxes()).rejects.toThrow(/missing end record/);
  });

  it("requires an API key", () => {
    installUsageStub({});
    expect(() => new UsageClient({ apiKey: "", organizationId: "org-1" })).toThrow(
      /TENSORLAKE_API_KEY/,
    );
  });

  it("requires an organization", () => {
    installUsageStub({});
    const previous = process.env.TENSORLAKE_ORGANIZATION_ID;
    delete process.env.TENSORLAKE_ORGANIZATION_ID;
    try {
      expect(() => new UsageClient({ apiKey: "key" })).toThrow(/organization ID/);
    } finally {
      if (previous !== undefined) process.env.TENSORLAKE_ORGANIZATION_ID = previous;
    }
  });
});
