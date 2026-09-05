import { loadNative } from "../native-binding.js";
import { configureNativeWorker } from "../native-worker-client.js";
import type { NativeFunctionAgentBinding, NativeFunctionAgentOptions } from "./protocol.js";
import { FunctionAgentRunner } from "./runner.js";

function parseArgs(args: readonly string[]): NativeFunctionAgentOptions {
  const values = new Map<string, string>();
  for (let index = 0; index < args.length; index += 1) {
    const key = args[index];
    const value = args[index + 1];
    if (!key?.startsWith("--") || value == null || value.startsWith("--")) {
      throw new Error(`Expected --name value argument, got '${key ?? ""}'`);
    }
    values.set(key.slice(2), value);
    index += 1;
  }
  const functionServiceUrl = values.get("function-service-url");
  const registrationToken = values.get("registration-token");
  if (functionServiceUrl == null || registrationToken == null) {
    throw new Error("--function-service-url and --registration-token are required");
  }
  return {
    functionServiceUrl,
    registrationToken,
    ...(values.has("agent-id") ? { agentId: values.get("agent-id") } : {}),
    ...(values.has("incarnation") ? { incarnation: values.get("incarnation") } : {}),
    ...(values.has("secret-service-workload-url")
      ? { secretServiceWorkloadUrl: values.get("secret-service-workload-url") }
      : {}),
    ...(values.has("credential-request-timeout-ms")
      ? { credentialRequestTimeoutMs: Number(values.get("credential-request-timeout-ms")) }
      : {}),
  };
}

export async function main(args: readonly string[] = process.argv.slice(2)): Promise<void> {
  configureNativeWorker(() => new URL("../native-worker.cjs", import.meta.url));
  const binding = loadNative<NativeFunctionAgentBinding>();
  const core = new binding.FunctionAgentCore(parseArgs(args));
  return new FunctionAgentRunner(core).run();
}
