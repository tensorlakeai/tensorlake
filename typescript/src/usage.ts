import * as defaults from "./defaults.js";
import { NativeCloudTransport } from "./native-cloud.js";

/** How far back to read instead of the current billing period. */
export type UsageLookback = "1d" | "7d" | "30d";

const LOOKBACK_DAYS: Record<UsageLookback, number> = { "1d": 1, "7d": 7, "30d": 30 };
const DAY_MS = 24 * 60 * 60 * 1000;

export interface UsageClientOptions {
  /** Tensorlake API key. Defaults to `TENSORLAKE_API_KEY`. */
  apiKey?: string;
  /** Organization to read. Defaults to `TENSORLAKE_ORGANIZATION_ID`. */
  organizationId?: string;
  /** Cloud API URL. Defaults to `TENSORLAKE_API_URL`. */
  apiUrl?: string;
}

export interface UsageReadOptions {
  /** Read the last `1d`, `7d` or `30d` instead of the current billing period. */
  last?: UsageLookback;
}

// Money amounts are minor currency units (for example cents), and quantities
// and compute units are decimal strings, so no precision is lost.

/** Metered usage, excluding subscription fees and tax. */
export interface UsageCharge {
  currency: string;
  amountMinor: string;
  resources: {
    cpuAmountMinor: string;
    memoryAmountMinor: string;
    sandboxStorageAmountMinor: string;
    snapshotStorageAmountMinor: string;
  };
}

/** Plan allowance consumption, valued at the overage rate. */
export interface IncludedUsage {
  currency: string;
  includedAmountMinor: string;
  periodUsedAmountMinor: string;
  rangeUsageAmountMinor: string;
  periodStart: string;
  periodEnd: string;
}

/** Organization usage totals for a window. */
export interface UsageSummary {
  windowStart: string;
  windowEnd: string;
  computeUnits: string;
  /** `null` when the organization has no metered price. */
  usageCharge: UsageCharge | null;
  includedUsage: IncludedUsage | null;
  /** Usage valued at the overage rate before the plan allowance. */
  usageValue: UsageCharge | null;
  runningSandboxes: number;
  projects: Array<{ projectId: string; projectName: string; computeUnits: string }>;
}

export interface ResourceUsage {
  quantity: string;
  /** `vCPU-hour`, `GiB-hour` or `GiB`. */
  unit: string;
  attributedAmountMinor: string | null;
  usageValueAmountMinor: string | null;
}

/** Usage of one sandbox and its share of the organization's usage charge. */
export interface SandboxUsage {
  sandboxId: string;
  /** `null` for usage whose project no longer exists. */
  project: { projectId: string; projectName: string } | null;
  attributedAmountMinor: string | null;
  usageValueAmountMinor: string | null;
  resources: {
    cpu: ResourceUsage;
    memory: ResourceUsage;
    sandboxStorage: ResourceUsage;
    snapshotStorage: ResourceUsage;
    networkEgress: ResourceUsage;
  };
}

/** A share of the organization's pooled usage charge. */
export interface AttributedUsageCharge {
  currency: string;
  amountMinor: string;
  /** `invoice` for a finalized invoice, `current_price_estimate` otherwise. */
  basis: string;
}

/** Every sandbox of a window with its usage and attributed charge. */
export interface SandboxUsageExport {
  windowStart: string;
  windowEnd: string;
  /**
   * The organization's usage charge; the sandboxes' attributed amounts add up
   * to it exactly. `null` when the organization has no metered price.
   */
  attributedUsageCharge: AttributedUsageCharge | null;
  includedUsage: IncludedUsage | null;
  attributedUsageValue: { currency: string; amountMinor: string } | null;
  sandboxes: SandboxUsage[];
}

type ExportRecord =
  | ({ type: "window" } & Omit<SandboxUsageExport, "sandboxes">)
  | ({ type: "sandbox" } & SandboxUsage)
  | { type: "end"; count: number };

/**
 * Read an organization's sandbox usage and cost.
 *
 * Every read covers the current billing period, or the last `1d`, `7d` or
 * `30d` when `last` is set.
 */
export class UsageClient {
  private readonly transport: NativeCloudTransport;
  private readonly organizationId: string;

  constructor(options?: UsageClientOptions) {
    const organizationId =
      options?.organizationId ?? process.env.TENSORLAKE_ORGANIZATION_ID;
    if (!organizationId) {
      throw new Error(
        "An organization ID is required. Pass organizationId or set TENSORLAKE_ORGANIZATION_ID.",
      );
    }
    this.organizationId = organizationId;
    this.transport = new NativeCloudTransport({
      baseUrl: options?.apiUrl ?? defaults.API_URL,
      apiKey: options?.apiKey ?? defaults.API_KEY,
      maxRetries: defaults.MAX_RETRIES,
      retryBackoffMs: defaults.RETRY_BACKOFF_MS,
      timeoutMs: defaults.DEFAULT_HTTP_TIMEOUT_MS,
      userAgent: `tensorlake-typescript-sdk/${defaults.SDK_VERSION}`,
    });
  }

  close(): void {
    this.transport.close();
  }

  /** Organization usage totals for the window. */
  async summary(options?: UsageReadOptions): Promise<UsageSummary> {
    const query = new URLSearchParams({ granularity: "day", ...rangeQuery(options?.last) });
    return this.transport.requestJson<UsageSummary>(
      "GET",
      `${this.basePath()}/breakdown?${query}`,
    );
  }

  /**
   * Usage and attributed cost of every sandbox that ran in the window.
   *
   * The server computes the attribution once and returns every sandbox in one
   * response.
   */
  async sandboxes(options?: UsageReadOptions): Promise<SandboxUsageExport> {
    const query = new URLSearchParams(rangeQuery(options?.last)).toString();
    const suffix = query ? `?${query}` : "";
    const response = await this.transport.requestResponse(
      "GET",
      `${this.basePath()}/sandboxes/export${suffix}`,
      { headers: { Accept: "application/x-ndjson" } },
    );
    return parseExport(await response.text());
  }

  private basePath(): string {
    return `/platform/v1/organizations/${encodeURIComponent(this.organizationId)}/sandbox-usage`;
  }
}

/**
 * A lookback sends an exact range ending now; the current period sends none,
 * so the server resolves the billing period itself.
 */
function rangeQuery(last: UsageLookback | undefined): Record<string, string> {
  if (last === undefined) return {};
  const days = LOOKBACK_DAYS[last];
  if (days === undefined) {
    throw new Error(`last must be one of 1d, 7d, 30d; got ${JSON.stringify(last)}`);
  }
  const now = Date.now();
  return {
    fromDate: new Date(now - days * DAY_MS).toISOString(),
    toDate: new Date(now).toISOString(),
  };
}

/**
 * Collect an export body, rejecting one that lacks its window header or its
 * end record, or whose count disagrees: a stream cut short after the response
 * started looks otherwise like a smaller, valid export.
 */
export function parseExport(body: string): SandboxUsageExport {
  const records = body
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .map((line) => JSON.parse(line) as ExportRecord);
  const [header, ...rest] = records;
  if (header?.type !== "window") {
    throw new Error("incomplete sandbox usage export: missing window header");
  }
  const { type: _header, ...window } = header;
  const sandboxes: SandboxUsage[] = [];
  for (const record of rest) {
    if (record.type === "sandbox") {
      const { type: _sandbox, ...sandbox } = record;
      sandboxes.push(sandbox);
    } else if (record.type === "end") {
      if (record.count !== sandboxes.length) {
        throw new Error(
          `incomplete sandbox usage export: expected ${record.count} sandboxes, received ${sandboxes.length}`,
        );
      }
      return { ...window, sandboxes };
    } else {
      throw new Error("incomplete sandbox usage export: repeated window header");
    }
  }
  throw new Error("incomplete sandbox usage export: missing end record");
}
