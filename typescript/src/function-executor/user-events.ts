import { randomUUID } from "node:crypto";
import { writeStructuredOutput } from "./safe-output.js";

export function printCloudEvent(
  data: Record<string, unknown>,
  options: { type?: string; source?: string; message?: string } = {},
): void {
  writeStructuredOutput("stdout", () => ({
    specversion: "1.0",
    id: randomUUID(),
    timestamp: new Date().toISOString(),
    type: options.type ?? "ai.tensorlake.event",
    source: options.source ?? "/tensorlake/function_executor/events",
    data,
    ...(options.message == null ? {} : { message: options.message }),
  }));
}
