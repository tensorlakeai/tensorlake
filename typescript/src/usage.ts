import * as defaults from "./defaults.js";
import { SandboxError } from "./errors.js";
import { callNative, loadNativeSandboxBinding, type NativeUsageClient } from "./native-sandbox.js";
import { releaseNativeHandle } from "./native-worker-client.js";

/** How far back to read instead of the current billing period. */
export type UsageLookback = "1d" | "7d" | "30d";

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
  /**
   * `null` when the organization has no metered price, and on plans with
   * included usage (Pro, Enterprise) when the window is not inside the current
   * billing period. Use `usageValue` then.
   */
  usageCharge: UsageCharge | null;
  includedUsage: IncludedUsage | null;
  /**
   * Usage valued at the plan's overage rate, before included usage. Set for
   * every window on plans with included usage, `null` otherwise.
   */
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
  basis: "invoice" | "current_price_estimate" | "unknown";
}

/** Every sandbox of a window with its usage and attributed charge. */
export interface SandboxUsageExport {
  windowStart: string;
  windowEnd: string;
  /**
   * The organization's usage charge; the sandboxes' attributed amounts add up
   * to it exactly. `null` when the organization has no metered price, and on
   * plans with included usage when the window is not inside the current
   * billing period.
   */
  attributedUsageCharge: AttributedUsageCharge | null;
  includedUsage: IncludedUsage | null;
  /**
   * The organization's usage at the plan's overage rate; the sandboxes' usage
   * values add up to it exactly. Set for every window on plans with included
   * usage.
   */
  attributedUsageValue: { currency: string; amountMinor: string } | null;
  sandboxes: SandboxUsage[];
}

/**
 * Read an organization's sandbox usage and cost.
 *
 * Every read covers the current billing period, or the last `1d`, `7d` or
 * `30d` when `last` is set. Requests, windows and response parsing are done
 * by the Rust SDK, the same as for Python and the CLI.
 */
export class UsageClient {
  private readonly native: NativeUsageClient;

  constructor(options?: UsageClientOptions) {
    const apiKey = options?.apiKey ?? defaults.API_KEY;
    if (!apiKey) {
      throw new SandboxError("Missing TENSORLAKE_API_KEY credentials.");
    }
    const organizationId =
      options?.organizationId ?? process.env.TENSORLAKE_ORGANIZATION_ID;
    if (!organizationId) {
      throw new SandboxError(
        "An organization ID is required. Pass organizationId or set TENSORLAKE_ORGANIZATION_ID.",
      );
    }
    const binding = loadNativeSandboxBinding();
    if (typeof binding.NativeUsageClient !== "function") {
      throw new SandboxError(
        "native binding does not export the usage client; rebuild with 'npm run build:native'",
      );
    }
    this.native = new binding.NativeUsageClient(
      options?.apiUrl ?? defaults.API_URL,
      apiKey,
      organizationId,
      `tensorlake-typescript-sdk/${defaults.SDK_VERSION}`,
      defaults.DEFAULT_HTTP_TIMEOUT_MS / 1000,
    );
  }

  close(): void {
    releaseNativeHandle(this.native);
  }

  /** Organization usage totals for the window. */
  async summary(options?: UsageReadOptions): Promise<UsageSummary> {
    const { json } = await callNative(() => this.native.summary(options?.last ?? null));
    return JSON.parse(json) as UsageSummary;
  }

  /**
   * Usage and attributed cost of every sandbox that ran in the window.
   *
   * The server computes the attribution once and returns every sandbox in one
   * response.
   */
  async sandboxes(options?: UsageReadOptions): Promise<SandboxUsageExport> {
    const { json } = await callNative(() => this.native.sandboxes(options?.last ?? null));
    return JSON.parse(json) as SandboxUsageExport;
  }
}
