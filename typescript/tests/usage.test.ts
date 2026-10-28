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

function page(sandboxId: string, nextCursor: number | null) {
  return {
    windowStart: WINDOW_START,
    windowEnd: WINDOW_END,
    attributedUsageCharge: null,
    includedUsage: null,
    attributedUsageValue: null,
    projects: [],
    sandboxes: [sandbox(sandboxId)],
    total: 2,
    nextCursor,
  };
}

describe("UsageClient", () => {
  afterEach(() => {
    clearCloudStub();
  });

  it("pages sandboxes through the first page's window", async () => {
    const urls: URL[] = [];
    installCloudStub((url) => {
      const parsed = new URL(url);
      urls.push(parsed);
      const body = parsed.searchParams.has("cursor") ? page("sb-2", null) : page("sb-1", 1);
      return new Response(JSON.stringify(body), { status: 200 });
    });

    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    const ids: string[] = [];
    for await (const usage of client.sandboxes()) ids.push(usage.sandboxId);
    client.close();

    expect(ids).toEqual(["sb-1", "sb-2"]);
    expect(urls[0].pathname).toBe("/platform/v1/organizations/org-1/sandbox-usage/sandboxes");
    expect(urls[0].searchParams.has("fromDate")).toBe(false);
    expect(urls[1].searchParams.get("fromDate")).toBe(WINDOW_START);
    expect(urls[1].searchParams.get("toDate")).toBe(WINDOW_END);
    expect(urls[1].searchParams.get("cursor")).toBe("1");
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

  it("rejects an unsupported lookback before any request", () => {
    const client = new UsageClient({ apiUrl: "http://localhost:8900", organizationId: "org-1" });
    expect(() => client.sandboxes({ last: "2d" as never })).toThrow(/1d, 7d, 30d/);
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
