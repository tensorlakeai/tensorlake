import { createHash, randomUUID } from "node:crypto";
import type { RegisteredDefinition, FunctionRuntime, FunctionFuture, TailCall } from "../applications/function.js";
import { executeHandlerResult, runWithFunctionRuntime } from "../applications/function.js";
import { File } from "../applications/file.js";
import { FunctionError, ReplayMismatchError, RequestError, isRequestError } from "../applications/errors.js";
import type { RequestContextValue } from "../applications/context.js";
import { runWithRequestContext } from "../applications/context.js";
import { deserializeJSON, serializeValue } from "../applications/serialization.js";
import { deferred, type Deferred } from "./async-queue.js";
import {
  deserializeValueFromProtocol,
  downloadBlob as downloadBlobData,
  downloadSerializedObject as downloadSerializedObjectData,
  joinPrepared,
  prepareSerializedObject,
  prepareTextObject,
  uploadBlob as uploadBlobData,
  type BlobValue,
  type PreparedSerializedObject,
  type SerializedObjectInsideBlobValue,
} from "./blob.js";
import { printCloudEvent } from "./user-events.js";

type Message = Record<string, any>;
type StreamCall = { write(value: Message): boolean; end(): void; on(event: string, listener: () => void): void };
type LogLevel = "debug" | "info" | "warn" | "error";

const OK = 0;
const NOT_FOUND = 5;
const REPLAY_STRICT = new Set(["REPLAY_MODE_STRICT", 1]);
const OUTCOME_SUCCESS = new Set(["ALLOCATION_OUTCOME_CODE_SUCCESS", 1]);
const WATCHER_TIMED_OUT = new Set(["FUNCTION_CALL_WATCHER_STATUS_TIMEDOUT", 2]);

function statusError(status: Message | undefined, fallback: string): Error | undefined {
  if (status == null || (status.code ?? OK) === OK) return undefined;
  return new FunctionError(status.message || fallback);
}

function durableHash(values: string[]): string {
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

function protocolTimestamp(date: Date): { seconds: number; nanos: number } {
  const milliseconds = date.getTime();
  return { seconds: Math.floor(milliseconds / 1000), nanos: (milliseconds % 1000) * 1_000_000 };
}

function parseHTTPMessage(data: Uint8Array): { headers: Record<string, string>; body: Uint8Array } {
  const marker = Buffer.from("\r\n\r\n");
  const bytes = Buffer.from(data);
  const split = bytes.indexOf(marker);
  if (split < 0) throw new Error("Invalid message/http application input");
  const lines = bytes.subarray(0, split).toString("utf8").split("\r\n");
  lines.shift();
  const headers: Record<string, string> = {};
  for (const line of lines) {
    const colon = line.indexOf(":");
    if (colon > 0) headers[line.slice(0, colon).trim().toLowerCase()] = line.slice(colon + 1).trim();
  }
  return { headers, body: new Uint8Array(bytes.subarray(split + marker.byteLength)) };
}

async function deserializeApplicationArguments(
  definition: RegisteredDefinition,
  payload: { data: Uint8Array; contentType?: string; encoding?: string | number },
): Promise<{ args: unknown[]; headers: Record<string, string> }> {
  let data = payload.data;
  let contentType = payload.contentType ?? "application/json";
  let headers: Record<string, string> = {};
  if (contentType === "message/http") {
    const parsed = parseHTTPMessage(data);
    data = parsed.body;
    headers = parsed.headers;
    contentType = headers["content-type"] ?? "application/json";
  }
  if (definition.parameters.length === 0) return { args: [], headers };
  if (contentType.toLowerCase().startsWith("multipart/form-data")) {
    const form = await new Response(Uint8Array.from(data).buffer, {
      headers: { "content-type": contentType },
    }).formData();
    const args: unknown[] = [];
    for (const [index, parameter] of definition.parameters.entries()) {
      const part = form.get(parameter.name) ?? form.get(String(index));
      if (part == null) {
        args.push(undefined);
      } else if (typeof part === "string") {
        try {
          args.push(JSON.parse(part));
        } catch {
          args.push(part);
        }
      } else {
        args.push(new File(await part.arrayBuffer(), part.type || "application/octet-stream"));
      }
    }
    return { args, headers };
  }
  const firstParameter = definition.parameters[0];
  if (firstParameter?.schema._file) return { args: [new File(data, contentType)], headers };
  if (!contentType.toLowerCase().includes("json")) {
    throw new Error(`Expected application/json input for '${firstParameter?.name ?? "argument"}', got ${contentType}`);
  }
  return { args: [deserializeJSON(data)], headers };
}

class ReplayHistory {
  readonly ordered: Message[] = [];
  readonly results = new Map<string, Message[]>();
  private cursor = 0;
  private mismatch?: ReplayMismatchError;

  constructor(entries: Message[]) {
    for (const entry of entries) {
      if (entry.functionCallWatcherResult != null) {
        const id = String(entry.functionCallWatcherResult.functionCallId ?? "");
        const values = this.results.get(id) ?? [];
        values.push(entry.functionCallWatcherResult);
        this.results.set(id, values);
      } else if (entry.functionCallCreated != null || entry.functionCallWatcherCreated != null) {
        this.ordered.push(entry);
      }
    }
  }

  takeOrdered(kind: "call" | "watcher", id: string): Message | undefined {
    if (this.cursor >= this.ordered.length) return undefined;
    const entry = this.ordered[this.cursor++];
    const event = kind === "call" ? entry.functionCallCreated : entry.functionCallWatcherCreated;
    if (event == null || event.functionCallId !== id) {
      this.mismatch = new ReplayMismatchError(`Expected replay ${kind} event for ${id}`);
      throw this.mismatch;
    }
    return event;
  }

  takeResult(id: string): Message | undefined {
    return this.results.get(id)?.shift();
  }

  assertNoMismatch(): void {
    if (this.mismatch != null) throw this.mismatch;
  }

  assertConsumed(): void {
    this.assertNoMismatch();
    if (this.cursor !== this.ordered.length) {
      throw new ReplayMismatchError("Function completed before its durable event history was consumed");
    }
  }
}

interface EventWaiter {
  predicate(event: Message): boolean;
  result: Deferred<Message>;
}

class AllocationProtocolError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AllocationProtocolError";
  }
}

export class AllocationRunner implements FunctionRuntime {
  readonly allocation: Message;
  private readonly functionRef: Message;
  private readonly definition: RegisteredDefinition;
  private readonly applicationDefinition: RegisteredDefinition;
  private readonly stateStreams = new Set<StreamCall>();
  private readonly eventReadStreams = new Set<StreamCall>();
  private readonly outputBlobRequests = new Map<string, Deferred<Message>>();
  private readonly stateOperationRequests = new Map<string, Deferred<Message>>();
  private readonly executionBatches: Message[][] = [];
  private readonly executionBatchWaiters: Array<(events: Message[]) => void> = [];
  private readonly liveBacklog: Message[] = [];
  private readonly liveWaiters: EventWaiter[] = [];
  private readonly controller = new AbortController();
  private currentRead?: { request: Message; response: Deferred<Message> };
  private replay?: ReplayHistory;
  private protocolError?: AllocationProtocolError;
  private lastEventClock = 0;
  private previousDurableId: string;
  private livePump?: Promise<void>;
  private started = false;
  private terminalBatchQueued = false;
  private finished = false;
  private state: Message = { outputBlobRequests: [], requestStateOperations: [] };
  private readonly startedAt = Date.now();

  constructor(
    allocation: Message,
    functionRef: Message,
    definition: RegisteredDefinition,
    applicationDefinition: RegisteredDefinition = definition,
  ) {
    this.allocation = allocation;
    this.functionRef = functionRef;
    this.definition = definition;
    this.applicationDefinition = applicationDefinition;
    this.previousDurableId = String(allocation.functionCallId);
    this.updateStateHash();
    this.log("debug", "allocation runner created", {
      replay_mode: allocation.replayMode,
      input_count: allocation.inputs?.args?.length ?? 0,
      input_blob_count: allocation.inputs?.argBlobs?.length ?? 0,
    });
  }

  start(): void {
    if (this.started) {
      this.log("warn", "duplicate allocation runner start ignored");
      return;
    }
    this.started = true;
    this.log("info", "allocation runner scheduled");
    void this.run().catch((error) => {
      this.log("error", "allocation runner promise rejected", {}, error);
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      });
      this.finish();
    });
  }

  async invoke<T>(definition: RegisteredDefinition, args: readonly unknown[]): Promise<T> {
    this.log("debug", "durable function invocation requested", {
      target_function: definition.name,
      argument_count: args.length,
    });
    return this.runChild<T>(definition, args, 0);
  }

  async runFuture<T>(future: FunctionFuture<T>): Promise<T> {
    this.log("debug", "durable future execution requested", {
      target_function: future.definition.name,
      argument_count: future.args.length,
      delay_seconds: future.delaySeconds,
    });
    return this.runChild<T>(future.definition, future.args, future.delaySeconds);
  }

  private nextDurableId(definition: RegisteredDefinition): string {
    const id = durableHash([
      String(this.allocation.functionCallId),
      this.previousDurableId,
      "FunctionCall",
      definition.name,
    ]);
    this.previousDurableId = id;
    return id;
  }

  private async runChild<T>(
    definition: RegisteredDefinition,
    args: readonly unknown[],
    delaySeconds: number,
  ): Promise<T> {
    const id = this.nextDurableId(definition);
    const startedAt = Date.now();
    const maxRetries = definition.options.retries?.maxRetries
      ?? this.applicationDefinition.application?.retries.maxRetries
      ?? 0;
    this.log("info", "durable function call starting", {
      durable_id: id,
      target_function: definition.name,
      argument_count: args.length,
      delay_seconds: delaySeconds,
      max_retries: maxRetries,
    });
    await this.createChildCall(id, definition, args, delaySeconds);
    await this.createWatcher(id);
    this.log("debug", "waiting for terminal durable function result", {
      durable_id: id,
      target_function: definition.name,
      max_retries: maxRetries,
      replay_result_available: (this.replay?.results.get(id)?.length ?? 0) > 0,
    });
    // The server owns allocation retry policy and retains the watcher route
    // across retryable attempt failures. It emits exactly one result here:
    // the eventual success or terminal failure.
    const result = this.replay?.takeResult(id) ?? await this.waitForLiveEvent(
      (entry) => entry.functionCallWatcherResult?.functionCallId === id,
    ).then((entry) => entry.functionCallWatcherResult);
    const resolved = await this.resolveWatcherResult<T>(result);
    this.log("info", "durable function call completed", {
      durable_id: id,
      target_function: definition.name,
      max_retries: maxRetries,
      duration_ms: Date.now() - startedAt,
    });
    return resolved;
  }

  private async createChildCall(
    id: string,
    definition: RegisteredDefinition,
    args: readonly unknown[],
    delaySeconds: number,
  ): Promise<void> {
    const replayed = this.replay?.takeOrdered("call", id);
    if (replayed != null) {
      this.log("debug", "replaying durable function creation", { durable_id: id });
      const error = statusError(replayed.status, "Failed to start function call");
      if (error != null) throw error;
      return;
    }
    const prepared: PreparedSerializedObject[] = [];
    let offset = 0;
    for (const arg of args) {
      const item = prepareSerializedObject(arg, offset);
      prepared.push(item);
      offset += item.bytes.byteLength;
    }
    let argsBlob: BlobValue | undefined;
    if (prepared.length > 0) {
      this.log("debug", "requesting durable function arguments blob", {
        durable_id: id,
        argument_count: args.length,
        serialized_bytes: offset,
      });
      argsBlob = await this.requestOutputBlob(offset);
      argsBlob = await this.uploadBlob(argsBlob, joinPrepared(prepared), "durable function arguments", {
        durable_id: id,
      });
    }
    const metadata = Buffer.from(JSON.stringify({
      format: "tensorlake.typescript.function-call.v1",
      id,
      functionName: definition.name,
      argumentCount: args.length,
    }), "utf8");
    const updates: Message = {
      updates: [{ functionCall: {
        id,
        target: {
          namespace: this.functionRef.namespace,
          applicationName: this.functionRef.applicationName,
          applicationVersion: this.functionRef.applicationVersion,
          functionName: definition.name,
        },
        args: prepared.map((item) => ({ value: item.object })),
        callMetadata: metadata,
      } }],
      rootFunctionCallId: id,
    };
    if (delaySeconds > 0) updates.startAt = protocolTimestamp(new Date(Date.now() + delaySeconds * 1000));
    this.log("debug", "queueing durable function creation event", {
      durable_id: id,
      target_function: definition.name,
      delay_seconds: delaySeconds,
    });
    this.addBatch([{ createFunctionCall: { updates, argsBlob } }]);
    this.log("debug", "waiting for durable function creation acknowledgement", { durable_id: id });
    const created = await this.waitForLiveEvent((entry) => entry.functionCallCreated?.functionCallId === id);
    const error = statusError(created.functionCallCreated?.status, "Failed to start function call");
    if (error != null) throw error;
  }

  private async createWatcher(id: string): Promise<void> {
    const replayed = this.replay?.takeOrdered("watcher", id);
    if (replayed != null) {
      this.log("debug", "replaying durable function watcher creation", { durable_id: id });
      const error = statusError(replayed.status, "Failed to create function call watcher");
      if (error != null) throw error;
      return;
    }
    this.log("debug", "queueing durable function watcher event", { durable_id: id });
    this.addBatch([{ createFunctionCallWatcher: { functionCallId: id } }]);
    this.log("debug", "waiting for durable function watcher acknowledgement", { durable_id: id });
    const created = await this.waitForLiveEvent((entry) => entry.functionCallWatcherCreated?.functionCallId === id);
    const error = statusError(created.functionCallWatcherCreated?.status, "Failed to create function call watcher");
    if (error != null) throw error;
  }

  private async resolveWatcherResult<T>(event: Message): Promise<T> {
    this.log("debug", "resolving durable function watcher result", {
      durable_id: event.functionCallId,
      watcher_status: event.watcherStatus,
      outcome_code: event.outcomeCode,
      has_value_output: event.valueOutput != null,
      has_request_error_output: event.requestErrorOutput != null,
    });
    if (WATCHER_TIMED_OUT.has(event.watcherStatus)) throw new FunctionError("Function call watcher timed out");
    if (OUTCOME_SUCCESS.has(event.outcomeCode)) {
      const downloaded = await this.downloadSerializedObject(
        event.valueOutput,
        event.valueBlob,
        "durable function result",
        { durable_id: event.functionCallId },
      );
      return deserializeValueFromProtocol(downloaded) as T;
    }
    if (event.requestErrorOutput != null) {
      const downloaded = await this.downloadSerializedObject(
        event.requestErrorOutput,
        event.requestErrorBlob,
        "durable function request error",
        { durable_id: event.functionCallId },
      );
      throw new RequestError(new TextDecoder("utf-8", { fatal: true }).decode(downloaded.data));
    }
    throw new FunctionError("Function call failed");
  }

  private async startTailCall(tailCall: TailCall<unknown>): Promise<string> {
    const id = this.nextDurableId(tailCall.definition);
    this.log("info", "tail call starting", {
      durable_id: id,
      target_function: tailCall.definition.name,
      argument_count: tailCall.args.length,
    });
    await this.createChildCall(id, tailCall.definition, tailCall.args, 0);
    return id;
  }

  private async run(): Promise<void> {
    let handlerArgs: unknown[];
    let headers: Record<string, string> = {};
    this.log("info", "allocation execution started", {
      replay_mode: this.allocation.replayMode,
      function: this.functionRef.functionName,
    });
    this.emitLifecycleEvent("allocations_started", "Starting allocations");
    try {
      const inputs = this.allocation.inputs ?? {};
      headers = Object.fromEntries((inputs.requestContext?.headers ?? []).map((header: Message) => [
        String(header.name ?? "").toLowerCase(),
        String(header.value ?? ""),
      ]));
      const objects = inputs.args ?? [];
      const blobs = inputs.argBlobs ?? [];
      this.log("debug", "allocation inputs validated", {
        object_count: objects.length,
        blob_count: blobs.length,
        request_header_count: Object.keys(headers).length,
        function_call_metadata_bytes: inputs.functionCallMetadata?.length ?? 0,
      });
      if (objects.length !== blobs.length) throw new Error("Function argument and BLOB counts differ");
      const values = await Promise.all(objects.map(
        (object: SerializedObjectInsideBlobValue, index: number) => this.downloadSerializedObject(
          object,
          blobs[index],
          "function input",
          { input_index: index },
        ),
      ));
      this.log("info", "all function inputs downloaded", {
        input_count: values.length,
        total_data_bytes: values.reduce((total, value) => total + value.data.byteLength, 0),
      });
      const metadata = Buffer.from(inputs.functionCallMetadata ?? []);
      if (metadata.byteLength === 0) {
        this.log("debug", "deserializing application invocation input", {
          content_type: values[0]?.contentType,
        });
        if (values.length !== 1) throw new Error("Application calls require exactly one input payload");
        const parsed = await deserializeApplicationArguments(this.definition, values[0]);
        handlerArgs = parsed.args;
        headers = { ...headers, ...parsed.headers };
        this.log("debug", "application invocation input deserialized", {
          handler_argument_count: handlerArgs.length,
          request_header_count: Object.keys(headers).length,
        });
      } else {
        this.log("debug", "deserializing internal function invocation inputs", {
          metadata_bytes: metadata.byteLength,
        });
        const call = JSON.parse(metadata.toString("utf8")) as Message;
        if (call.format !== "tensorlake.typescript.function-call.v1") {
          throw new Error("Function call metadata was produced by a different SDK language");
        }
        if (call.argumentCount !== values.length) throw new Error("Function call metadata argument count differs");
        handlerArgs = values.map(deserializeValueFromProtocol);
        this.log("debug", "internal function invocation inputs deserialized", {
          handler_argument_count: handlerArgs.length,
          metadata_function: call.functionName,
        });
      }

      if (REPLAY_STRICT.has(this.allocation.replayMode)) {
        this.log("info", "strict replay history loading");
        await this.loadReplayHistory();
      } else {
        this.log("debug", "strict replay disabled");
      }
      const requestContext = this.createRequestContext(headers);
      let result: unknown | TailCall<unknown>;
      const handlerStartedAt = Date.now();
      this.log("info", "user function handler starting", {
        handler_argument_count: handlerArgs.length,
      });
      try {
        result = await runWithRequestContext(requestContext, () =>
          runWithFunctionRuntime(this, () => executeHandlerResult(this.definition, handlerArgs)),
        );
      } catch (error) {
        if (error instanceof ReplayMismatchError || error instanceof AllocationProtocolError) {
          throw error;
        }
        this.log("error", "user function handler failed", {
          duration_ms: Date.now() - handlerStartedAt,
        }, error);
        await this.finishUserError(error);
        return;
      }
      this.log("info", "user function handler completed", {
        duration_ms: Date.now() - handlerStartedAt,
        result_kind: typeof result === "object" && result != null && (result as TailCall<unknown>).kind === "tensorlake-tail-call"
          ? "tail_call"
          : "value",
        result_type: result == null ? String(result) : typeof result,
      });
      if (this.protocolError != null) throw this.protocolError;
      this.replay?.assertNoMismatch();
      if (typeof result === "object" && result != null && (result as TailCall<unknown>).kind === "tensorlake-tail-call") {
        const id = await this.startTailCall(result as TailCall<unknown>);
        this.replay?.assertConsumed();
        this.log("info", "queueing successful tail-call allocation finish", { tail_call_durable_id: id });
        this.queueTerminalBatch({
          outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
          tailCallDurableId: id,
        });
      } else {
        this.replay?.assertConsumed();
        let prepared: PreparedSerializedObject;
        try {
          prepared = prepareSerializedObject(result, 0, String(this.allocation.functionCallId));
        } catch (error) {
          this.log("error", "function output serialization failed", {}, error);
          await this.finishUserError(error);
          return;
        }
        this.log("debug", "function output serialized", {
          output_bytes: prepared.bytes.byteLength,
          output_encoding: prepared.object.manifest?.encoding,
          output_content_type: prepared.object.manifest?.contentType,
        });
        let outputBlob = await this.requestOutputBlob(prepared.bytes.byteLength);
        outputBlob = await this.uploadBlob(outputBlob, prepared.bytes, "function output");
        this.log("info", "queueing successful value allocation finish", {
          output_blob_id: outputBlob.id,
          output_bytes: prepared.bytes.byteLength,
        });
        this.queueTerminalBatch({
          outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
          value: prepared.object,
          uploadedFunctionOutputsBlob: outputBlob,
        });
      }
    } catch (error) {
      const replayMismatch = error instanceof ReplayMismatchError;
      this.log("error", replayMismatch ? "durable replay mismatch" : "allocation internal failure", {}, error);
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: replayMismatch
          ? "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH"
          : "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      });
    } finally {
      this.log("info", "allocation execution stopping", {
        duration_ms: Date.now() - this.startedAt,
        pending_output_blob_requests: this.outputBlobRequests.size,
        pending_state_operations: this.stateOperationRequests.size,
        pending_event_waiters: this.liveWaiters.length,
      });
      this.emitLifecycleEvent("allocations_finished", "Allocations completed");
      this.finish();
    }
  }

  private async finishUserError(error: unknown): Promise<void> {
    const rendered = error instanceof Error ? error.message : String(error);
    printCloudEvent({
      level: "error",
      event: "function_call_failed",
      message: rendered,
      namespace: this.functionRef.namespace,
      application: this.functionRef.applicationName,
      application_version: this.functionRef.applicationVersion,
      function: this.functionRef.functionName,
      request_id: this.allocation.requestId,
      function_call_id: this.allocation.functionCallId,
      allocation_id: this.allocation.allocationId,
      error: error instanceof Error ? error.stack?.split("\n") ?? [rendered] : [rendered],
    });
    if (isRequestError(error)) {
      const output = prepareTextObject(error.message);
      const requestErrorBlob = this.allocation.inputs?.requestErrorBlob as BlobValue | undefined;
      if (requestErrorBlob == null) throw new Error("Allocation has no request error BLOB");
      const uploaded = await this.uploadBlob(requestErrorBlob, output.bytes, "request error output");
      this.log("info", "queueing request-error allocation finish", {
        request_error_bytes: output.bytes.byteLength,
        request_error_blob_id: uploaded.id,
      });
      this.queueTerminalBatch({
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_REQUEST_ERROR",
        requestErrorOutput: output.object,
        uploadedRequestErrorBlob: uploaded,
      });
      return;
    }
    this.log("info", "queueing function-error allocation finish", {}, error);
    this.queueTerminalBatch({
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
    });
  }

  private createRequestContext(headers: Record<string, string>): RequestContextValue {
    return {
      requestId: String(this.allocation.requestId),
      headers: Object.freeze({ ...headers }),
      signal: this.controller.signal,
      state: {
        get: async <T>(key: string, defaultValue?: T) => {
          const operationId = randomUUID();
          this.log("debug", "request state read starting", {
            state_key: key,
            operation_id: operationId,
            has_default: defaultValue !== undefined,
          });
          const result = await this.requestStateOperation({ operationId, stateKey: key, prepareRead: {} });
          if (result.status?.code === NOT_FOUND) {
            this.log("debug", "request state key not found", { state_key: key, operation_id: operationId });
            return defaultValue;
          }
          const error = statusError(result.status, `Failed to read request state '${key}'`);
          if (error != null) throw error;
          const data = await this.downloadBlob(result.prepareRead?.blob, "request state value", {
            state_key: key,
            operation_id: operationId,
          });
          const value = deserializeJSON(data) as T;
          this.log("debug", "request state read completed", {
            state_key: key,
            operation_id: operationId,
            value_bytes: data.byteLength,
          });
          return value;
        },
        set: async (key: string, value: unknown) => {
          const serialized = serializeValue(value);
          if (serialized.encoding !== "json") throw new Error("Request state values must be JSON values");
          const prepareOperationId = randomUUID();
          this.log("debug", "request state write preparation starting", {
            state_key: key,
            operation_id: prepareOperationId,
            value_bytes: serialized.data.byteLength,
          });
          const prepared = await this.requestStateOperation({
            operationId: prepareOperationId,
            stateKey: key,
            prepareWrite: { size: serialized.data.byteLength },
          });
          const error = statusError(prepared.status, `Failed to prepare request state '${key}'`);
          if (error != null) throw error;
          const blob = await this.uploadBlob(prepared.prepareWrite?.blob, serialized.data, "request state value", {
            state_key: key,
            operation_id: prepareOperationId,
          });
          const commitOperationId = randomUUID();
          this.log("debug", "request state write commit starting", {
            state_key: key,
            operation_id: commitOperationId,
            blob_id: blob.id,
          });
          const committed = await this.requestStateOperation({
            operationId: commitOperationId,
            stateKey: key,
            commitWrite: { blob },
          });
          const commitError = statusError(committed.status, `Failed to commit request state '${key}'`);
          if (commitError != null) throw commitError;
          this.log("debug", "request state write completed", {
            state_key: key,
            operation_id: commitOperationId,
            value_bytes: serialized.data.byteLength,
          });
        },
      },
      metrics: {
        counter: async (name, value = 1) => this.emitUserEvent("ai.tensorlake.metric.counter.inc", {
          counter_name: name,
          counter_inc: value,
        }),
        timer: async (name, value) => this.emitUserEvent("ai.tensorlake.metric.timer", {
          timer_name: name,
          timer_value: value,
        }),
      },
      progress: {
        update: async (current, total, options) => {
          this.log("debug", "request progress update", {
            current,
            total,
            has_custom_message: options?.message != null,
            attribute_count: Object.keys(options?.attributes ?? {}).length,
          });
          this.state.progress = { current, total };
          this.publishState();
          const message = options?.message ?? `${this.functionRef.functionName}: executing step ${current} of ${total}`;
          printCloudEvent({ RequestProgressUpdated: {
            request_id: this.allocation.requestId,
            function_name: this.functionRef.functionName,
            function_run_id: this.allocation.functionCallId,
            allocation_id: this.allocation.allocationId,
            message,
            step: current,
            total,
            attributes: options?.attributes,
            created_at: new Date().toISOString(),
          } }, {
            type: "ai.tensorlake.progress_update",
            source: "/tensorlake/applications/progress",
            message,
          });
        },
      },
    };
  }

  private emitUserEvent(type: string, data: Message): void {
    this.log("debug", "user metric event emitted", {
      event_type: type,
      metric_name: data.counter_name ?? data.timer_name,
    });
    printCloudEvent({
        request_id: this.allocation.requestId,
        function_name: this.functionRef.functionName,
        function_run_id: this.allocation.functionCallId,
        allocation_id: this.allocation.allocationId,
        ...data,
    }, { type, source: "/tensorlake/applications/metrics" });
  }

  private emitLifecycleEvent(event: string, message: string): void {
    printCloudEvent({
      event,
      message,
      allocations: [{
        namespace: this.functionRef.namespace,
        application: this.functionRef.applicationName,
        application_version: this.functionRef.applicationVersion,
        function: this.functionRef.functionName,
        request_id: this.allocation.requestId,
        function_call_id: this.allocation.functionCallId,
        allocation_id: this.allocation.allocationId,
      }],
    });
  }

  private async loadReplayHistory(): Promise<void> {
    const entries: Message[] = [];
    let afterClock = 0;
    let page = 0;
    while (true) {
      page += 1;
      this.log("debug", "requesting replay history page", { page, after_clock: afterClock });
      const requestedAfterClock = afterClock;
      const response = await this.readEvents(afterClock);
      entries.push(...(response.entries ?? []));
      this.log("debug", "replay history page received", {
        page,
        entry_count: response.entries?.length ?? 0,
        last_clock: response.lastClock,
        has_more: response.hasMore,
      });
      const responseClock = Number(response.lastClock ?? requestedAfterClock);
      if (responseClock < requestedAfterClock) {
        throw new ReplayMismatchError(
          `Replay event log moved backwards from clock ${requestedAfterClock} to ${responseClock}`,
        );
      }
      if (response.hasMore && responseClock === requestedAfterClock) {
        throw new ReplayMismatchError(
          `Replay event log reported more entries without advancing clock ${requestedAfterClock}`,
        );
      }
      afterClock = responseClock;
      if (!response.hasMore) break;
    }
    this.lastEventClock = afterClock;
    this.replay = new ReplayHistory(entries);
    this.log("info", "strict replay history loaded", {
      page_count: page,
      entry_count: entries.length,
      last_clock: afterClock,
    });
  }

  private waitForLiveEvent(predicate: (event: Message) => boolean): Promise<Message> {
    const index = this.liveBacklog.findIndex(predicate);
    if (index >= 0) {
      this.log("debug", "durable event satisfied from backlog", {
        backlog_size: this.liveBacklog.length,
        pending_waiters: this.liveWaiters.length,
      });
      return Promise.resolve(this.liveBacklog.splice(index, 1)[0]);
    }
    const result = deferred<Message>();
    this.liveWaiters.push({ predicate, result });
    this.log("debug", "waiting for durable event", {
      backlog_size: this.liveBacklog.length,
      pending_waiters: this.liveWaiters.length,
    });
    this.ensureLivePump();
    return result.promise;
  }

  private ensureLivePump(): void {
    if (this.livePump != null) {
      this.log("debug", "durable event live pump already running");
      return;
    }
    this.log("info", "durable event live pump starting", { after_clock: this.lastEventClock });
    const pump = (async () => {
      // Only issue event-log reads while user code is blocked on a durable
      // event. This leaves no orphaned long poll after the final watcher result
      // and makes stream shutdown deterministic.
      while (!this.finished && this.liveWaiters.length > 0) {
        const requestedAfterClock = this.lastEventClock;
        const response = await this.readEvents(this.lastEventClock);
        this.log("debug", "durable event log page received", {
          requested_after_clock: requestedAfterClock,
          entry_count: response.entries?.length ?? 0,
          last_clock: response.lastClock,
          has_more: response.hasMore,
        });
        const responseClock = Number(response.lastClock ?? requestedAfterClock);
        if (responseClock < requestedAfterClock) {
          throw new AllocationProtocolError(
            `Allocation event log moved backwards from clock ${requestedAfterClock} to ${responseClock}`,
          );
        }
        if (response.hasMore && responseClock === requestedAfterClock) {
          throw new AllocationProtocolError(
            `Allocation event log reported more entries without advancing clock ${requestedAfterClock}`,
          );
        }
        this.lastEventClock = responseClock;
        for (const entry of response.entries ?? []) this.dispatchLiveEvent(entry);
      }
    })();
    this.livePump = pump;
    void pump.then(() => {
      if (this.livePump !== pump) return;
      this.livePump = undefined;
      this.log("debug", "durable event live pump stopped", {
        finished: this.finished,
        pending_waiters: this.liveWaiters.length,
        after_clock: this.lastEventClock,
      });
      // A waiter can be installed between the loop condition and this
      // continuation. Restart in that case so it cannot be stranded.
      if (!this.finished && this.liveWaiters.length > 0) this.ensureLivePump();
    }, (error) => {
      if (this.livePump === pump) this.livePump = undefined;
      if (!this.finished) {
        const protocolError = error instanceof AllocationProtocolError
          ? error
          : new AllocationProtocolError(
            error instanceof Error ? error.message : String(error),
          );
        this.protocolError = protocolError;
        this.log("error", "durable event live pump failed", {
          pending_waiters: this.liveWaiters.length,
        }, protocolError);
        for (const waiter of this.liveWaiters.splice(0)) waiter.result.reject(protocolError);
      }
    });
  }

  private dispatchLiveEvent(entry: Message): void {
    const index = this.liveWaiters.findIndex((waiter) => waiter.predicate(entry));
    const eventKind = Object.keys(entry).find((key) => key !== "clock") ?? "unknown";
    if (index < 0) {
      this.liveBacklog.push(entry);
      this.log("debug", "durable event added to backlog", {
        event_kind: eventKind,
        event_clock: entry.clock,
        backlog_size: this.liveBacklog.length,
      });
    } else {
      this.liveWaiters.splice(index, 1)[0].result.resolve(entry);
      this.log("debug", "durable event delivered to waiter", {
        event_kind: eventKind,
        event_clock: entry.clock,
        pending_waiters: this.liveWaiters.length,
      });
    }
  }

  private readEvents(afterClock: number): Promise<Message> {
    if (this.currentRead != null) {
      throw new AllocationProtocolError("Allocation event log already has a pending read");
    }
    const response = deferred<Message>();
    const request = {
      allocationId: this.allocation.allocationId,
      afterClock,
      maxEntries: 100,
    };
    this.currentRead = { request, response };
    this.log("debug", "event log read requested", {
      after_clock: afterClock,
      max_entries: request.maxEntries,
      connected_streams: this.eventReadStreams.size,
    });
    for (const stream of this.eventReadStreams) stream.write(request);
    return response.promise;
  }

  watchEventLogReads(stream: StreamCall): void {
    this.eventReadStreams.add(stream);
    this.log("info", "event log read stream connected", {
      connected_streams: this.eventReadStreams.size,
      has_pending_read: this.currentRead != null,
    });
    if (this.currentRead != null) {
      this.log("debug", "sending pending event log read to new stream", {
        after_clock: this.currentRead.request.afterClock,
        max_entries: this.currentRead.request.maxEntries,
      });
      stream.write(this.currentRead.request);
    }
    let streamRemoved = false;
    const removeStream = (reason: string) => {
      if (streamRemoved) return;
      streamRemoved = true;
      this.eventReadStreams.delete(stream);
      this.log("info", "event log read stream disconnected", {
        reason,
        connected_streams: this.eventReadStreams.size,
        has_pending_read: this.currentRead != null,
      });
    };
    stream.on("cancelled", () => removeStream("cancelled"));
    stream.on("close", () => removeStream("closed"));
    if (this.finished && this.currentRead == null) stream.end();
  }

  deliverEventLogResponse(response: Message): void {
    if (this.currentRead == null) {
      this.log("warn", "unexpected event log response ignored", {
        entry_count: response.entries?.length ?? 0,
        last_clock: response.lastClock,
      });
      return;
    }
    const pending = this.currentRead;
    this.currentRead = undefined;
    this.log("debug", "event log response delivered", {
      requested_after_clock: pending.request.afterClock,
      entry_count: response.entries?.length ?? 0,
      last_clock: response.lastClock,
      has_more: response.hasMore,
    });
    pending.response.resolve(response);
  }

  private requestOutputBlob(size: number): Promise<BlobValue> {
    const id = randomUUID();
    const request = deferred<Message>();
    this.outputBlobRequests.set(id, request);
    this.state.outputBlobRequests.push({ id, size });
    this.log("debug", "output blob requested", {
      blob_id: id,
      requested_bytes: size,
      pending_output_blob_requests: this.outputBlobRequests.size,
    });
    this.publishState();
    return request.promise.then((output) => {
      this.log("debug", "output blob response received", {
        blob_id: id,
        status_code: output.status?.code,
        chunk_count: output.blob?.chunks?.length ?? 0,
      });
      const error = statusError(output.status, "Failed to create output BLOB");
      if (error != null) throw error;
      return output.blob as BlobValue;
    });
  }

  private requestStateOperation(operation: Message): Promise<Message> {
    const request = deferred<Message>();
    this.stateOperationRequests.set(operation.operationId, request);
    this.state.requestStateOperations.push(operation);
    this.log("debug", "request state operation queued", {
      operation_id: operation.operationId,
      state_key: operation.stateKey,
      operation_kind: this.stateOperationKind(operation),
      requested_bytes: operation.prepareWrite?.size,
      pending_state_operations: this.stateOperationRequests.size,
    });
    this.publishState();
    return request.promise;
  }

  deliverUpdate(update: Message): void {
    if (update.outputBlob != null) {
      let id = String(update.outputBlob.blob?.id ?? "");
      let matchedByOrder = false;
      if (!id) {
        // Error responses do not contain a BLOB, and therefore carry no BLOB ID.
        // The dataplane reconciles state requests and sends their responses in
        // state-list order, so match a BLOB-less response to the oldest request.
        id = String(this.state.outputBlobRequests[0]?.id ?? "");
        matchedByOrder = id !== "";
      }
      const request = this.outputBlobRequests.get(id);
      if (request == null) {
        this.log("warn", "unmatched output blob update ignored", {
          blob_id: id || undefined,
          status_code: update.outputBlob.status?.code,
          pending_output_blob_requests: this.outputBlobRequests.size,
        });
        return;
      }
      this.outputBlobRequests.delete(id);
      this.state.outputBlobRequests = this.state.outputBlobRequests.filter((item: Message) => item.id !== id);
      this.log("debug", "output blob update matched", {
        blob_id: id,
        status_code: update.outputBlob.status?.code,
        chunk_count: update.outputBlob.blob?.chunks?.length ?? 0,
        matched_by_order: matchedByOrder,
        pending_output_blob_requests: this.outputBlobRequests.size,
      });
      this.publishState();
      request.resolve(update.outputBlob);
      return;
    }
    const result = update.requestStateOperationResult;
    if (result != null) {
      const id = String(result.operationId);
      const request = this.stateOperationRequests.get(id);
      if (request == null) {
        this.log("warn", "unmatched request state operation result ignored", {
          operation_id: id,
          status_code: result.status?.code,
          pending_state_operations: this.stateOperationRequests.size,
        });
        return;
      }
      const operation = this.state.requestStateOperations.find((item: Message) => item.operationId === id);
      this.stateOperationRequests.delete(id);
      this.state.requestStateOperations = this.state.requestStateOperations.filter(
        (item: Message) => item.operationId !== id,
      );
      this.log("debug", "request state operation result matched", {
        operation_id: id,
        state_key: operation?.stateKey,
        operation_kind: this.stateOperationKind(operation),
        status_code: result.status?.code,
        pending_state_operations: this.stateOperationRequests.size,
      });
      this.publishState();
      request.resolve(result);
      return;
    }
    this.log("warn", "allocation update contained no recognized payload");
  }

  watchState(stream: StreamCall): void {
    this.stateStreams.add(stream);
    this.log("info", "allocation state stream connected", {
      connected_streams: this.stateStreams.size,
      state_hash: this.state.sha256Hash,
      pending_output_blob_requests: this.outputBlobRequests.size,
      pending_state_operations: this.stateOperationRequests.size,
    });
    stream.write(this.state);
    let streamRemoved = false;
    const removeStream = (reason: string) => {
      if (streamRemoved) return;
      streamRemoved = true;
      this.stateStreams.delete(stream);
      this.log("info", "allocation state stream disconnected", {
        reason,
        connected_streams: this.stateStreams.size,
      });
    };
    stream.on("cancelled", () => removeStream("cancelled"));
    stream.on("close", () => removeStream("closed"));
    if (this.finished) stream.end();
  }

  private updateStateHash(): void {
    const content = JSON.stringify({
      progress: this.state.progress,
      outputBlobRequests: this.state.outputBlobRequests,
      requestStateOperations: this.state.requestStateOperations,
    });
    this.state.sha256Hash = createHash("sha256").update(content).digest("hex");
  }

  private publishState(): void {
    this.updateStateHash();
    this.log("debug", "allocation state published", {
      state_hash: this.state.sha256Hash,
      connected_streams: this.stateStreams.size,
      pending_output_blob_requests: this.outputBlobRequests.size,
      pending_state_operations: this.stateOperationRequests.size,
      progress_current: this.state.progress?.current,
      progress_total: this.state.progress?.total,
    });
    for (const stream of this.stateStreams) stream.write(this.state);
  }

  private addBatch(events: Message[]): void {
    if (this.terminalBatchQueued && events.every((event) => event.finishAllocation == null)) {
      this.log("error", "execution event rejected after terminal batch", {
        event_count: events.length,
        event_kinds: events.map((event) => Object.keys(event)[0] ?? "unknown"),
      });
      return;
    }
    this.executionBatches.push(events);
    this.log("debug", "execution log batch queued", {
      event_count: events.length,
      event_kinds: events.map((event) => Object.keys(event)[0] ?? "unknown"),
      queued_batches: this.executionBatches.length,
      waiting_consumers: this.executionBatchWaiters.length,
    });
    for (const waiter of this.executionBatchWaiters.splice(0)) waiter(events);
  }

  private queueTerminalBatch(finishAllocation: Message): void {
    if (this.terminalBatchQueued) {
      this.log("error", "duplicate terminal allocation batch rejected", {
        outcome_code: finishAllocation.outcomeCode,
        failure_reason: finishAllocation.failureReason,
      });
      return;
    }
    this.terminalBatchQueued = true;
    this.addBatch([{ finishAllocation }]);
  }

  async getExecutionBatch(): Promise<Message[]> {
    if (this.executionBatches.length > 0) {
      this.log("debug", "returning queued execution log batch", {
        event_count: this.executionBatches[0].length,
        queued_batches: this.executionBatches.length,
      });
      return this.executionBatches[0];
    }
    if (this.finished) {
      this.log("debug", "returning empty execution log batch for finished allocation");
      return [];
    }
    this.log("debug", "execution log batch consumer waiting", {
      waiting_consumers: this.executionBatchWaiters.length + 1,
    });
    return new Promise((resolve) => this.executionBatchWaiters.push(resolve));
  }

  advanceExecutionBatch(): void {
    const advanced = this.executionBatches.shift();
    this.log("debug", "execution log batch advanced", {
      advanced_event_count: advanced?.length ?? 0,
      queued_batches: this.executionBatches.length,
    });
  }

  get isFinished(): boolean {
    return this.finished;
  }

  private finish(): void {
    if (this.finished) {
      this.log("debug", "allocation finish ignored because runner is already finished");
      return;
    }
    this.finished = true;
    this.controller.abort();
    this.log("info", "allocation runner finished", {
      duration_ms: Date.now() - this.startedAt,
      queued_execution_batches: this.executionBatches.length,
      state_streams: this.stateStreams.size,
      event_log_streams: this.eventReadStreams.size,
    });
    for (const stream of this.stateStreams) stream.end();
    for (const stream of this.eventReadStreams) stream.end();
    for (const waiter of this.executionBatchWaiters.splice(0)) waiter([]);
  }

  private stateOperationKind(operation: Message | undefined): string {
    if (operation?.prepareRead != null) return "prepare_read";
    if (operation?.prepareWrite != null) return "prepare_write";
    if (operation?.commitWrite != null) return "commit_write";
    return "unknown";
  }

  private blobLogFields(blob: BlobValue | undefined): Message {
    const chunks = blob?.chunks ?? [];
    return {
      blob_id: blob?.id,
      chunk_count: chunks.length,
      declared_bytes: chunks.reduce((total, chunk) => total + (chunk.size ?? 0), 0),
    };
  }

  private async downloadBlob(
    blob: BlobValue | undefined,
    purpose: string,
    fields: Message = {},
  ): Promise<Uint8Array> {
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "blob download starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
    });
    try {
      const data = await downloadBlobData(blob, (message, details) => this.log("debug", `blob ${message}`, {
        purpose,
        ...fields,
        ...details,
      }));
      this.log("debug", "blob download completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        downloaded_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return data;
    } catch (error) {
      this.log("error", "blob download failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private async downloadSerializedObject(
    object: SerializedObjectInsideBlobValue | undefined,
    blob: BlobValue | undefined,
    purpose: string,
    fields: Message = {},
  ): Promise<{ data: Uint8Array; metadata: Uint8Array; contentType?: string; encoding?: string | number }> {
    if (object == null) throw new Error(`Missing serialized object for ${purpose}`);
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "serialized object download starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
      object_offset: object.offset ?? 0,
      object_bytes: object.manifest?.size,
      metadata_bytes: object.manifest?.metadataSize,
      content_type: object.manifest?.contentType,
      encoding: object.manifest?.encoding,
    });
    try {
      const value = await downloadSerializedObjectData(object, blob, (message, details) => this.log(
        "debug",
        `blob ${message}`,
        { purpose, ...fields, ...details },
      ));
      this.log("debug", "serialized object download completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        object_bytes: value.data.byteLength + value.metadata.byteLength,
        data_bytes: value.data.byteLength,
        metadata_bytes: value.metadata.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return value;
    } catch (error) {
      this.log("error", "serialized object download failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private async uploadBlob(
    blob: BlobValue | undefined,
    data: Uint8Array,
    purpose: string,
    fields: Message = {},
  ): Promise<BlobValue> {
    if (blob == null) throw new Error(`Missing BLOB for ${purpose}`);
    const startedAt = Date.now();
    this.log("debug", "blob upload starting", {
      purpose,
      ...fields,
      ...this.blobLogFields(blob),
      upload_bytes: data.byteLength,
    });
    try {
      const uploaded = await uploadBlobData(blob, data, (message, details) => this.log("debug", `blob ${message}`, {
        purpose,
        ...fields,
        ...details,
      }));
      this.log("debug", "blob upload completed", {
        purpose,
        ...fields,
        ...this.blobLogFields(uploaded),
        upload_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      });
      return uploaded;
    } catch (error) {
      this.log("error", "blob upload failed", {
        purpose,
        ...fields,
        ...this.blobLogFields(blob),
        upload_bytes: data.byteLength,
        duration_ms: Date.now() - startedAt,
      }, error);
      throw error;
    }
  }

  private log(level: LogLevel, message: string, fields: Message = {}, error?: unknown): void {
    const rendered = error instanceof Error
      ? error.stack?.split("\n") ?? [`${error.name}: ${error.message}`]
      : error == null ? undefined : [String(error)];
    process.stderr.write(`${JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      component: "typescript_function_executor_allocation",
      message,
      allocation_id: this.allocation.allocationId,
      request_id: this.allocation.requestId,
      function_call_id: this.allocation.functionCallId,
      namespace: this.functionRef.namespace,
      application: this.functionRef.applicationName,
      application_version: this.functionRef.applicationVersion,
      function: this.functionRef.functionName,
      elapsed_ms: Date.now() - this.startedAt,
      ...fields,
      ...(rendered == null ? {} : { error: rendered }),
    })}\n`);
  }
}
