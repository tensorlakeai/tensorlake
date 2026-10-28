import { afterEach, describe, expect, it } from "vitest";
import { clearCloudStub, installCloudStub } from "./cloud-stub.js";
import { UsageClient } from "../src/usage.js";

const WINDOW_START = "2026-10-01T00:00:00.000Z";
const WINDOW_END = "2026-10-28T12:00:00.000Z";

function sandbox(sandboxId: string) {
  const resource = (unit: string) => ({
    quantity: "1.5",
    unit,
    attributedAmountMinor: "100",
    usageValueAmountMinor: null,
  });
  return {
    sandboxId,
    project: { projectId: "proj-1", projectName: "Default" },
    attributedAmountMinor: "500",
    usageValueAmountMinor: null,
    resources: {
      cpu: resource("vCPU-hour"),
      memory: resource("GiB-hour"),
      sandboxStorage: resource("GiB-hour"),
      snapshotStorage: resource("GiB-hour"),
      networkEgress: resource("GiB"),
    },
  };
}

function exportBody(sandboxIds: string[], end = true): string {
  const records: unknown[] = [
    {
      type: "window",
      windowStart: WINDOW_START,
      windowEnd: WINDOW_END,
      attributedUsageCharge: { currency: "usd", amountMinor: "1000", basis: "invoice" },
      includedUsage: null,
      attributedUsageValue: null,
    },
    ...sandboxIds.map((id) => ({ type: "sandbox", ...sandbox(id) })),
  ];
  if (end) records.push({ type: "end", count: sandboxIds.length });
  return records.map((record) => `${JSON.stringify(record)}\n`).join("");
}

describe("UsageClient", () => {
  afterEach(() => {
    clearCloudStub();
  });

  it("reads every sandbox from one export request", async () => {
    const urls: URL[] = [];
    installCloudStub((url) => {
      urls.push(new URL(url));
      return new Response(exportBody(["sb-1", "sb-2"]), { status: 200 });
    });

    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    const usage = await client.sandboxes();
    client.close();

    expect(usage.sandboxes.map((s) => s.sandboxId)).toEqual(["sb-1", "sb-2"]);
    expect(usage.windowStart).toBe(WINDOW_START);
    expect(usage.attributedUsageCharge?.basis).toBe("invoice");
    expect(urls).toHaveLength(1);
    expect(urls[0].pathname).toBe("/platform/v1/organizations/org-1/sandbox-usage/sandboxes/export");
    expect(urls[0].search).toBe("");
  });

  it("rejects a truncated export", async () => {
    installCloudStub(() => new Response(exportBody(["sb-1"], false), { status: 200 }));

    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    await expect(client.sandboxes({ last: "30d" })).rejects.toThrow(/missing end record/);
    client.close();
  });

  it("reads a lookback summary as an exact range", async () => {
    let requested: URL | undefined;
    installCloudStub((url) => {
      requested = new URL(url);
      return new Response(
        JSON.stringify({
          windowStart: WINDOW_START,
          windowEnd: WINDOW_END,
          computeUnits: "42.5",
          usageCharge: null,
          includedUsage: null,
          usageValue: null,
          runningSandboxes: 3,
          projects: [],
        }),
        { status: 200 },
      );
    });

    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    const summary = await client.summary({ last: "7d" });
    client.close();

    expect(requested?.pathname).toBe("/platform/v1/organizations/org-1/sandbox-usage/breakdown");
    expect(requested?.searchParams.get("granularity")).toBe("day");
    expect(requested?.searchParams.has("fromDate")).toBe(true);
    expect(requested?.searchParams.has("toDate")).toBe(true);
    expect(summary.computeUnits).toBe("42.5");
    expect(summary.runningSandboxes).toBe(3);
  });

  it("rejects an unsupported lookback before any request", async () => {
    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    await expect(client.sandboxes({ last: "2d" as never })).rejects.toThrow(/1d, 7d, 30d/);
    client.close();
  });

  it("requires an organization", () => {
    const previous = process.env.TENSORLAKE_ORGANIZATION_ID;
    delete process.env.TENSORLAKE_ORGANIZATION_ID;
    try {
      expect(() => new UsageClient({ apiUrl: "http://localhost:8900" })).toThrow(/organization ID/);
    } finally {
      if (previous !== undefined) process.env.TENSORLAKE_ORGANIZATION_ID = previous;
    }
  });
});
