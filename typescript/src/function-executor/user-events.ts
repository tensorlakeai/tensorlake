import { randomUUID } from "node:crypto";

export function printCloudEvent(
  data: Record<string, unknown>,
  options: { type?: string; source?: string; message?: string } = {},
): void {
  process.stdout.write(`${JSON.stringify({
    specversion: "1.0",
    id: randomUUID(),
    timestamp: new Date().toISOString(),
    type: options.type ?? "ai.tensorlake.event",
    source: options.source ?? "/tensorlake/function_executor/events",
    data,
    ...(options.message == null ? {} : { message: options.message }),
  })}\n`);
}
