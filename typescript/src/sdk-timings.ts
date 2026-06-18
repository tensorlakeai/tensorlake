const SDK_TIMINGS_ENV_VAR = "TENSORLAKE_SDK_TIMINGS";
const SDK_TIMING_PAYLOADS_ENV_VAR = "TENSORLAKE_SDK_TIMING_PAYLOADS";

export type SdkTimingFields = Record<string, string | number | boolean | null | undefined>;

export function sdkTimingsEnabled(): boolean {
  const value = process.env[SDK_TIMINGS_ENV_VAR];
  return value === "1" || value === "true" || value === "yes";
}

export function sdkTimingPayloadsEnabled(): boolean {
  const value = process.env[SDK_TIMING_PAYLOADS_ENV_VAR];
  return value === "1" || value === "true" || value === "yes";
}

export function nowMs(): number {
  return performance.now();
}

export function logSdkTiming(
  op: string,
  phase: string,
  startedAt: number,
  fields: SdkTimingFields = {},
): void {
  if (!sdkTimingsEnabled()) return;
  logSdkTimingEvent(op, phase, {
    ...fields,
    duration_ms: Math.round((nowMs() - startedAt) * 100) / 100,
  });
}

export function logSdkTimingEvent(
  op: string,
  phase: string,
  fields: SdkTimingFields = {},
): void {
  if (!sdkTimingsEnabled()) return;

  const parts = [
    "[tensorlake:sdk-timing]",
    `op=${op}`,
    `phase=${phase}`,
  ];

  for (const [key, value] of Object.entries(fields)) {
    if (value == null) continue;
    parts.push(`${key}=${formatSdkTimingValue(value)}`);
  }

  console.error(parts.join(" "));
}

function formatSdkTimingValue(value: string | number | boolean): string {
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (/^[A-Za-z0-9_./:@-]+$/.test(value)) {
    return value;
  }
  return JSON.stringify(value);
}
