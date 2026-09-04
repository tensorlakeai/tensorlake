import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { compileFunction, constants as vmConstants } from "node:vm";
import { unzipSync } from "fflate";
import type { RequestContextValue } from "../applications/context.js";
import {
  runWithRequestContext,
  serializeRequestStateValue,
  validateCounterMetric,
  validateProgressUpdate,
  validateRequestStateKey,
  validateTimerMetric,
} from "../applications/context.js";
import {
  FunctionError,
  RequestError,
  SDKUsageError,
  TimeoutError,
  isRequestError,
} from "../applications/errors.js";
import { File } from "../applications/file.js";
import {
  executeHandlerResult,
  installPromiseInstrumentation,
  isTailCall,
  runWithFunctionRuntime,
  type FunctionFuture,
  type FunctionRuntime,
  type RegisteredDefinition,
  type TailCall,
} from "../applications/function.js";
import { Headers } from "../applications/headers.js";
import { HttpBody } from "../applications/http-body.js";
import { deserializeJSON, serializeValue } from "../applications/serialization.js";
import type {
  AgentInput,
  AgentInputValue,
  Assignment,
  CallResult,
  NativeFunctionAgentCore,
  RequestStateResult,
} from "./protocol.js";

installPromiseInstrumentation();

type Message = Record<string, unknown>;

const RESERVED_ENVIRONMENT_TARGETS = new Set([
  "PYTHONFAULTHANDLER",
  "LD_PRELOAD",
  "LD_LIBRARY_PATH",
  "DYLD_INSERT_LIBRARIES",
  "PYTHONHOME",
  "PYTHONPATH",
  "NODE_OPTIONS",
  "NODE_PATH",
]);

interface CodeManifest {
  format_version?: number;
  runtime?: string;
  minimum_node_major?: number;
  module?: string;
  functions?: Record<string, { name?: string }>;
}

interface RuntimeModule {
  __tensorlakeGetFunction?(name: string): RegisteredDefinition;
}

interface LoadedApplication {
  root: string;
  getFunction(name: string): RegisteredDefinition;
}

interface Deferred<T> {
  promise: Promise<T>;
  resolve(value: T): void;
  reject(error: unknown): void;
}

const nativeImport = compileFunction(
  "return import(specifier)",
  ["specifier"],
  { importModuleDynamically: vmConstants.USE_MAIN_CONTEXT_DEFAULT_LOADER },
) as (specifier: string) => Promise<unknown>;

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolveValue, rejectValue) => {
    resolve = resolveValue;
    reject = rejectValue;
  });
  return { promise, resolve, reject };
}

function durableHash(values: readonly string[]): string {
  const hash = createHash("sha256");
  for (const value of values) {
    const bytes = Buffer.from(value, "utf8");
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(bytes.byteLength));
    hash.update(length);
    hash.update(bytes);
    hash.update("|");
  }
  return hash.digest("hex");
}

function safeArchivePath(root: string, archivePath: string): string {
  const output = path.resolve(root, archivePath);
  if (output !== root && !output.startsWith(`${root}${path.sep}`)) {
    throw new Error(`Application archive contains unsafe path '${archivePath}'`);
  }
  return output;
}

function decodeBase64(value: string): Uint8Array {
  return new Uint8Array(Buffer.from(value, "base64"));
}

function encodeBase64(value: Uint8Array): string {
  return Buffer.from(value).toString("base64");
}

function deserializeValue(value: AgentInputValue): unknown {
  const data = decodeBase64(value.data_base64);
  if (value.content_type.toLowerCase().includes("json")) return deserializeJSON(data);
  return new File(data, value.content_type || "application/octet-stream");
}

function serializeArgument(value: unknown): Message {
  const serialized = serializeValue(value);
  return {
    source: "data",
    data_base64: encodeBase64(serialized.data),
    metadata_base64: "",
    content_type: serialized.contentType,
  };
}

function consumeResolvedEnvironment(assignment: Assignment, install: boolean): void {
  const rawEnvironment = assignment.resolved_environment ?? [];
  const targets = new Set<string>();
  for (const item of rawEnvironment) {
    if (
      item == null
      || typeof item.target !== "string"
      || !/^[A-Za-z_][A-Za-z0-9_]{0,255}$/.test(item.target)
      || RESERVED_ENVIRONMENT_TARGETS.has(item.target)
      || item.target.startsWith("TENSORLAKE_")
      || targets.has(item.target)
      || typeof item.value !== "string"
      || item.value.includes("\0")
    ) {
      throw new Error("Resolved environment entry is invalid");
    }
    targets.add(item.target);
  }
  if (install) {
    for (const item of rawEnvironment) process.env[item.target] = item.value;
  }
  for (const item of rawEnvironment) item.value = "";
  assignment.resolved_environment = [];
}

function failureFromResult(result: Exclude<CallResult, { outcome: "success" }>): Error {
  if (result.outcome === "timed_out") return new TimeoutError();
  if (result.reason === "request_error") return new RequestError("Function call failed");
  return new FunctionError(`Function call failed: ${result.reason}`);
}

export class FunctionAgentRunner {
  private readonly attempts = new Map<string, AttemptRuntime>();
  private readonly applications = new Map<string, Promise<LoadedApplication>>();
  private initializationSent = false;
  private processEnvironmentInitialized = false;

  constructor(private readonly core: NativeFunctionAgentCore) {}

  async run(): Promise<void> {
    for (;;) {
      const input = JSON.parse(await this.core.nextInput()) as AgentInput;
      switch (input.type) {
        case "assignment":
          void this.startAssignment(input.assignment);
          break;
        case "call_result":
          this.attempts.get(input.attempt_id)?.deliverCallResult(
            input.function_call_id,
            input,
          );
          break;
        case "request_state_result":
          this.attempts.get(input.result.attempt_id)?.deliverStateResult(input.result);
          break;
        case "cancel":
          this.attempts.get(input.attempt_id)?.cancel();
          break;
        case "shutdown":
          return;
      }
    }
  }

  private async startAssignment(assignment: Assignment): Promise<void> {
    try {
      const installEnvironment = !this.processEnvironmentInitialized;
      consumeResolvedEnvironment(assignment, installEnvironment);
      if (installEnvironment) this.processEnvironmentInitialized = true;
      const application = await this.loadApplication(assignment);
      if (!this.initializationSent) {
        await this.submit({ type: "initialized" });
        this.initializationSent = true;
      }
      const definition = application.getFunction(assignment.function);
      const attempt = new AttemptRuntime(assignment, definition, this.submit.bind(this));
      this.attempts.set(assignment.attempt_id, attempt);
      await attempt.run();
    } catch (error) {
      // Initialization failures must be terminal user-code failures. Marking
      // the runner initialized before reporting the attempt avoids turning a
      // bad bundle into an unbounded infrastructure retry loop.
      if (!this.initializationSent) {
        await this.submit({ type: "initialized" });
        this.initializationSent = true;
      }
      await this.submit({
        type: "failure",
        attempt_id: assignment.attempt_id,
        reason: "function_error",
        message: error instanceof Error ? error.stack ?? error.message : String(error),
      });
    } finally {
      this.attempts.delete(assignment.attempt_id);
    }
  }

  private loadApplication(assignment: Assignment): Promise<LoadedApplication> {
    let application = this.applications.get(assignment.application_code_sha256);
    if (application == null) {
      application = unpackApplication(assignment);
      this.applications.set(assignment.application_code_sha256, application);
    }
    return application;
  }

  private submit(output: Message): Promise<void> {
    return this.core.submitOutput(JSON.stringify(output));
  }
}

class AttemptRuntime implements FunctionRuntime {
  private readonly controller = new AbortController();
  private readonly callResults = new Map<string, Deferred<CallResult>>();
  private readonly stateResults = new Map<string, Deferred<RequestStateResult>>();
  private previousDurableId: string;
  private nextStateSequence = 1;

  constructor(
    private readonly assignment: Assignment,
    private readonly definition: RegisteredDefinition,
    private readonly submit: (output: Message) => Promise<void>,
  ) {
    this.previousDurableId = assignment.function_run_id;
  }

  async run(): Promise<void> {
    try {
      const args = await deserializeAssignmentArguments(this.definition, this.assignment.inputs);
      const output = await runWithFunctionRuntime(this, () =>
        runWithRequestContext(this.requestContext(), () =>
          executeHandlerResult(this.definition, args),
        ),
      );
      if (isTailCall(output)) {
        const callId = await this.createTailCall(output);
        await this.submit({
          type: "success",
          attempt_id: this.assignment.attempt_id,
          result: { type: "call_graph", output_function_call_id: callId },
        });
        return;
      }
      const serialized = serializeValue(output);
      await this.submit({
        type: "success",
        attempt_id: this.assignment.attempt_id,
        result: {
          type: "value",
          output_base64: encodeBase64(serialized.data),
          metadata_base64: "",
          content_type: serialized.contentType,
        },
      });
    } catch (error) {
      if (this.controller.signal.aborted) return;
      await this.submit({
        type: "failure",
        attempt_id: this.assignment.attempt_id,
        reason: isRequestError(error) ? "request_error" : "function_error",
        message: error instanceof Error ? error.stack ?? error.message : String(error),
      });
    }
  }

  cancel(): void {
    if (!this.controller.signal.aborted) {
      this.controller.abort(new FunctionError("Function execution was cancelled"));
    }
    for (const waiter of this.callResults.values()) waiter.reject(this.controller.signal.reason);
    for (const waiter of this.stateResults.values()) waiter.reject(this.controller.signal.reason);
    this.callResults.clear();
    this.stateResults.clear();
  }

  deliverCallResult(functionCallId: string, result: CallResult): void {
    const waiter = this.callResults.get(functionCallId);
    if (waiter == null) return;
    this.callResults.delete(functionCallId);
    waiter.resolve(result);
  }

  deliverStateResult(result: RequestStateResult): void {
    const waiter = this.stateResults.get(result.operation_id);
    if (waiter == null) return;
    this.stateResults.delete(result.operation_id);
    waiter.resolve(result);
  }

  invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T> {
    return this.runChild<T>(definition, args, 0);
  }

  async runFuture<T>(future: FunctionFuture<T>): Promise<T> {
    if (future.delaySeconds > 0) {
      await this.submit({ type: "suspend", attempt_id: this.assignment.attempt_id });
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, future.delaySeconds * 1_000);
        this.controller.signal.addEventListener("abort", () => {
          clearTimeout(timer);
          reject(this.controller.signal.reason);
        }, { once: true });
      });
      await this.submit({ type: "resume", attempt_id: this.assignment.attempt_id });
    }
    return this.runChild<T>(future.definition, future.args, 0);
  }

  async reduce<T>(
    definition: RegisteredDefinition,
    items: readonly unknown[],
    initial: unknown,
    hasInitial: boolean,
  ): Promise<T> {
    let values = items;
    let accumulator = initial;
    if (!hasInitial) {
      if (items.length === 0) throw new SDKUsageError("reduce of empty iterable with no initial value");
      [accumulator, ...values] = items;
    }
    if (values.length === 0) return accumulator as T;
    const calls: Message[] = [];
    let previous: string | undefined;
    for (const item of values) {
      const id = this.nextDurableId(definition, "ReduceStep");
      calls.push({
        function_call_id: id,
        function_name: definition.name,
        inputs: [
          previous == null
            ? serializeArgument(accumulator)
            : { source: "function_run_output", function_call_id: previous },
          serializeArgument(item),
        ],
        call_metadata_base64: this.callMetadata(id, definition.name, "reduce"),
      });
      previous = id;
    }
    await this.submit({ type: "call_batch", attempt_id: this.assignment.attempt_id, calls });
    return this.watch<T>(previous as string);
  }

  private async runChild<T>(
    definition: RegisteredDefinition,
    args: readonly unknown[],
    _delaySeconds: number,
  ): Promise<T> {
    const id = this.nextDurableId(definition);
    await this.submit({
      type: "call_batch",
      attempt_id: this.assignment.attempt_id,
      calls: [{
        function_call_id: id,
        function_name: definition.name,
        inputs: args.map(serializeArgument),
        call_metadata_base64: this.callMetadata(id, definition.name, "call"),
      }],
    });
    return this.watch<T>(id);
  }

  private async watch<T>(functionCallId: string): Promise<T> {
    const result = deferred<CallResult>();
    this.callResults.set(functionCallId, result);
    await this.submit({
      type: "watch",
      attempt_id: this.assignment.attempt_id,
      function_call_id: functionCallId,
    });
    const resolved = await result.promise;
    if (resolved.outcome !== "success") throw failureFromResult(resolved);
    return deserializeValue({
      data_base64: resolved.output_base64,
      metadata_base64: resolved.metadata_base64,
      content_type: resolved.content_type,
    }) as T;
  }

  private async createTailCall(tailCall: TailCall<unknown>): Promise<string> {
    const id = this.nextDurableId(tailCall.definition);
    await this.submit({
      type: "call_batch",
      attempt_id: this.assignment.attempt_id,
      calls: [{
        function_call_id: id,
        function_name: tailCall.definition.name,
        inputs: tailCall.args.map(serializeArgument),
        call_metadata_base64: this.callMetadata(id, tailCall.definition.name, "tail_call"),
      }],
    });
    return id;
  }

  private nextDurableId(definition: RegisteredDefinition, operation = "FunctionCall"): string {
    const id = durableHash([
      this.assignment.function_run_id,
      this.previousDurableId,
      operation,
      definition.name,
    ]);
    this.previousDurableId = id;
    return id;
  }

  private callMetadata(id: string, functionName: string, operation: string): string {
    return Buffer.from(JSON.stringify({
      format: "tensorlake.typescript.function-call.v1",
      id,
      functionName,
      operation,
    })).toString("base64");
  }

  private requestContext(): RequestContextValue {
    return {
      requestId: this.assignment.request_id,
      headers: new Headers(
        (this.assignment.request_headers ?? []).map(({ name, value }) => [name, value] as const),
      ),
      signal: this.controller.signal,
      state: {
        get: async <T>(key: string, defaultValue?: T) => {
          validateRequestStateKey(key);
          const result = await this.requestState({ operation: "get", key });
          if (result.result !== "get" || result.value_base64 == null) return defaultValue;
          return deserializeJSON(decodeBase64(result.value_base64)) as T;
        },
        set: async (key: string, value: unknown) => {
          validateRequestStateKey(key);
          const encoded = encodeBase64(serializeRequestStateValue(value));
          await this.requestState({ operation: "set", key, value_base64: encoded });
        },
      },
      metrics: {
        counter: async (name, value = 1) => validateCounterMetric(name, value),
        timer: async (name, value) => validateTimerMetric(name, value),
      },
      progress: {
        update: async (current, total, options) => {
          validateProgressUpdate(current, total, options);
          await this.submit({
            type: "progress",
            attempt_id: this.assignment.attempt_id,
            message: JSON.stringify({ current, total, ...options }),
          });
        },
      },
    };
  }

  private async requestState(operation: Message): Promise<RequestStateResult> {
    const operationId = `${this.assignment.attempt_id}:state:${this.nextStateSequence}`;
    this.nextStateSequence += 1;
    const result = deferred<RequestStateResult>();
    this.stateResults.set(operationId, result);
    await this.submit({
      type: "request_state",
      attempt_id: this.assignment.attempt_id,
      operation_id: operationId,
      operation,
    });
    return result.promise;
  }
}

async function unpackApplication(assignment: Assignment): Promise<LoadedApplication> {
  const archive = decodeBase64(assignment.application_code_base64);
  const actualHash = createHash("sha256").update(archive).digest("hex");
  if (actualHash !== assignment.application_code_sha256) {
    throw new Error(
      `Application code hash mismatch: expected ${assignment.application_code_sha256}, got ${actualHash}`,
    );
  }
  const root = await mkdtemp(path.join(os.tmpdir(), "tensorlake-typescript-app-"));
  const files = unzipSync(archive);
  for (const [name, content] of Object.entries(files)) {
    const destination = safeArchivePath(root, name);
    if (name.endsWith("/")) {
      await mkdir(destination, { recursive: true });
      continue;
    }
    await mkdir(path.dirname(destination), { recursive: true });
    await writeFile(destination, content, { mode: 0o600 });
  }
  const manifest = JSON.parse(
    await readFile(path.join(root, ".tensorlake_code_manifest.json"), "utf8"),
  ) as CodeManifest;
  if (manifest.format_version !== 2 || manifest.runtime !== "typescript") {
    throw new Error("Application code is not a Tensorlake TypeScript bundle");
  }
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  if ((manifest.minimum_node_major ?? 24) > nodeMajor) {
    throw new Error(`Application requires Node ${manifest.minimum_node_major} or newer; runner has ${nodeMajor}`);
  }
  if (manifest.module == null) throw new Error("Application manifest does not name a runtime module");
  const modulePath = safeArchivePath(root, manifest.module);
  const runtime = await nativeImport(pathToFileURL(modulePath).href) as RuntimeModule;
  if (typeof runtime.__tensorlakeGetFunction !== "function") {
    throw new Error("Application runtime does not export __tensorlakeGetFunction");
  }
  return {
    root,
    getFunction(name: string): RegisteredDefinition {
      if (manifest.functions?.[name] == null) {
        throw new Error(`Function '${name}' is not present in the application bundle`);
      }
      const definition = runtime.__tensorlakeGetFunction?.(name);
      if (definition?.name !== name) {
        throw new Error(`Application runtime resolved '${name}' as '${String(definition?.name)}'`);
      }
      return definition;
    },
  };
}

async function deserializeAssignmentArguments(
  definition: RegisteredDefinition,
  inputs: readonly AgentInputValue[],
): Promise<unknown[]> {
  if (inputs.length === 0) return [];
  if (inputs.some((input) => input.source_function_call_id != null) || inputs.length > 1) {
    return inputs.map(deserializeValue);
  }
  let data = decodeBase64(inputs[0].data_base64);
  let contentType = inputs[0].content_type;
  if (contentType.toLowerCase().startsWith("message/http")) {
    const marker = Buffer.from("\r\n\r\n");
    const bytes = Buffer.from(data);
    const split = bytes.indexOf(marker);
    if (split < 0) throw new Error("Invalid message/http application input");
    const lines = bytes.subarray(0, split).toString("utf8").split("\r\n");
    lines.shift();
    for (const line of lines) {
      const colon = line.indexOf(":");
      if (colon > 0 && line.slice(0, colon).trim().toLowerCase() === "content-type") {
        contentType = line.slice(colon + 1).trim();
      }
    }
    data = new Uint8Array(bytes.subarray(split + marker.byteLength));
  }
  if (definition.parameters.length === 0) return [];
  const first = definition.parameters[0];
  if (definition.parameters.length === 1 && first?.schema._httpBody) {
    return [new HttpBody(data, contentType)];
  }
  if (data.byteLength === 0 && !first?.schema._file) return [];
  if (first?.schema._file) return [new File(data, contentType || "application/octet-stream")];
  if (!contentType.toLowerCase().includes("json")) {
    throw new Error(`Expected application/json input, got ${contentType}`);
  }
  return [deserializeJSON(data)];
}
