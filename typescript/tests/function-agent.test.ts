import { createHash } from "node:crypto";
import { zipSync } from "fflate";
import { describe, expect, it } from "vitest";
import { registerApplication, registerFunction } from "../src/applications/function.js";
import type { NativeFunctionAgentCore } from "../src/function-agent/protocol.js";
import { FunctionAgentRunner } from "../src/function-agent/runner.js";

class FakeCore implements NativeFunctionAgentCore {
  readonly outputs: Array<Record<string, unknown>> = [];
  private readonly inputs: string[];
  private waiting?: { resolve(value: string): void; reject(error: unknown): void };

  constructor(
    inputs: Array<Record<string, unknown>>,
    private readonly onOutput?: (
      output: Record<string, unknown>,
      core: FakeCore,
    ) => void,
  ) {
    this.inputs = inputs.map((input) => JSON.stringify(input));
  }

  nextInput(): Promise<string> {
    const input = this.inputs.shift();
    if (input != null) return Promise.resolve(input);
    return new Promise<string>((resolve, reject) => {
      this.waiting = { resolve, reject };
    });
  }

  async submitOutput(outputJson: string): Promise<void> {
    const output = JSON.parse(outputJson) as Record<string, unknown>;
    this.outputs.push(output);
    this.onOutput?.(output, this);
    if (output.type === "success") this.stop();
  }

  push(input: Record<string, unknown>): void {
    const encoded = JSON.stringify(input);
    const waiting = this.waiting;
    if (waiting == null) {
      this.inputs.push(encoded);
      return;
    }
    this.waiting = undefined;
    waiting.resolve(encoded);
  }

  stop(): void {
    this.waiting?.reject(new Error("fake core stopped"));
    this.waiting = undefined;
  }
}

describe("embedded TypeScript function agent", () => {
  it("loads an application assignment and returns its value through the native core", async () => {
    const module = `
const definition = {
  name: "echo",
  handler: async (value) => ({ echoed: value }),
  parameters: [{
    name: "value",
    schema: { jsonSchema: {} },
    required: true,
    hasDefault: false,
  }],
  returns: { jsonSchema: {} },
  options: { timeout: 300 },
};
export function __tensorlakeGetFunction(name) {
  if (name !== "echo") throw new Error("unknown function " + name);
  return definition;
}
`;
    const archive = zipSync({
      ".tensorlake_code_manifest.json": new TextEncoder().encode(JSON.stringify({
        format_version: 2,
        runtime: "typescript",
        minimum_node_major: 18,
        module: "runtime.mjs",
        functions: { echo: { name: "echo" } },
      })),
      "runtime.mjs": new TextEncoder().encode(module),
    });
    const core = new FakeCore([{
      type: "assignment",
      assignment: {
        attempt_id: "attempt-1",
        fence_token: 7,
        function_run_id: "run-1",
        request_id: "request-1",
        namespace: "ns",
        application: "app",
        application_version: "v1",
        function: "echo",
        timeout_ms: 10_000,
        initialization_timeout_ms: 10_000,
        inputs: [{
          data_base64: Buffer.from(JSON.stringify("hello")).toString("base64"),
          content_type: "application/json",
        }],
        request_headers: [],
        call_metadata_base64: "",
        application_code_base64: Buffer.from(archive).toString("base64"),
        application_code_sha256: createHash("sha256").update(archive).digest("hex"),
      },
    }]);

    await expect(new FunctionAgentRunner(core).run()).rejects.toThrow("fake core stopped");
    expect(core.outputs[0]).toEqual({ type: "initialized" });
    expect(core.outputs[1]).toMatchObject({
      type: "success",
      attempt_id: "attempt-1",
      result: {
        type: "value",
        content_type: "application/json; charset=UTF-8",
      },
    });
    const result = core.outputs[1].result as { output_base64: string };
    expect(JSON.parse(Buffer.from(result.output_base64, "base64").toString("utf8")))
      .toEqual({ echoed: "hello" });
  });

  it("routes an SDK child call through the Rust-core contract", async () => {
    const child = registerFunction(
      "embedded_agent_child",
      async (value: number) => value * 2,
    );
    const parent = registerApplication(
      "embedded_agent_parent",
      async (value: number) => child(value),
    );
    const definitions = { [parent.definition.name]: parent.definition };
    (globalThis as typeof globalThis & {
      __tensorlakeAgentTestDefinitions?: typeof definitions;
    }).__tensorlakeAgentTestDefinitions = definitions;
    const module = `
export function __tensorlakeGetFunction(name) {
  const definition = globalThis.__tensorlakeAgentTestDefinitions?.[name];
  if (definition == null) throw new Error("unknown function " + name);
  return definition;
}
`;
    const archive = zipSync({
      ".tensorlake_code_manifest.json": new TextEncoder().encode(JSON.stringify({
        format_version: 2,
        runtime: "typescript",
        minimum_node_major: 18,
        module: "runtime.mjs",
        functions: { embedded_agent_parent: { name: "embedded_agent_parent" } },
      })),
      "runtime.mjs": new TextEncoder().encode(module),
    });
    const assignment = {
      attempt_id: "attempt-child",
      fence_token: 11,
      function_run_id: "run-child",
      request_id: "request-child",
      namespace: "ns",
      application: "app",
      application_version: "v1",
      function: "embedded_agent_parent",
      timeout_ms: 10_000,
      initialization_timeout_ms: 10_000,
      inputs: [{
        data_base64: Buffer.from(JSON.stringify(21)).toString("base64"),
        content_type: "application/json",
      }],
      request_headers: [],
      call_metadata_base64: "",
      application_code_base64: Buffer.from(archive).toString("base64"),
      application_code_sha256: createHash("sha256").update(archive).digest("hex"),
    };
    let functionCallId: string | undefined;
    const core = new FakeCore(
      [{ type: "assignment", assignment }],
      (output, fake) => {
        if (output.type === "call_batch") {
          const calls = output.calls as Array<{ function_call_id: string }>;
          functionCallId = calls[0]?.function_call_id;
        }
        if (output.type === "watch" && functionCallId != null) {
          fake.push({
            type: "call_result",
            attempt_id: assignment.attempt_id,
            function_call_id: functionCallId,
            outcome: "success",
            output_base64: Buffer.from(JSON.stringify(42)).toString("base64"),
            metadata_base64: "",
            content_type: "application/json; charset=UTF-8",
          });
        }
      },
    );

    await expect(new FunctionAgentRunner(core).run()).rejects.toThrow("fake core stopped");
    expect(core.outputs.map((output) => output.type)).toEqual([
      "initialized",
      "call_batch",
      "watch",
      "success",
    ]);
    expect(functionCallId).toMatch(/^[a-f0-9]{64}$/);
    const success = core.outputs.at(-1)?.result as { output_base64: string };
    expect(JSON.parse(Buffer.from(success.output_base64, "base64").toString("utf8")))
      .toBe(42);
  });
});
