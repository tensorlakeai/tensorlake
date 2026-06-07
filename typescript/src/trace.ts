const TRACE_ENV_VAR = "TENSORLAKE_TRACE";
const TRACE_PAYLOADS_ENV_VAR = "TENSORLAKE_TRACE_PAYLOADS";

export type TraceFields = Record<string, string | number | boolean | null | undefined>;

export function traceEnabled(): boolean {
  const value = process.env[TRACE_ENV_VAR];
  return value === "1" || value === "true" || value === "yes";
}

export function tracePayloadsEnabled(): boolean {
  const value = process.env[TRACE_PAYLOADS_ENV_VAR];
  return value === "1" || value === "true" || value === "yes";
}

export function nowMs(): number {
  return performance.now();
}

export function traceTiming(
  op: string,
  phase: string,
  startedAt: number,
  fields: TraceFields = {},
): void {
  if (!traceEnabled()) return;
  traceEvent(op, phase, {
    ...fields,
    duration_ms: Math.round((nowMs() - startedAt) * 100) / 100,
  });
}

export function traceEvent(
  op: string,
  phase: string,
  fields: TraceFields = {},
): void {
  if (!traceEnabled()) return;

  const parts = [
    "[tensorlake:trace]",
    `op=${op}`,
    `phase=${phase}`,
  ];

  for (const [key, value] of Object.entries(fields)) {
    if (value == null) continue;
    parts.push(`${key}=${formatTraceValue(value)}`);
  }

  console.error(parts.join(" "));
}

function formatTraceValue(value: string | number | boolean): string {
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (/^[A-Za-z0-9_./:@-]+$/.test(value)) {
    return value;
  }
  return JSON.stringify(value);
}
