import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import * as grpc from "@grpc/grpc-js";
import { unzipSync } from "fflate";
import { SDK_VERSION } from "../defaults.js";
import type { RegisteredDefinition } from "../applications/function.js";
import { restoreRegistry, snapshotRegistry } from "../applications/registry.js";
import { AllocationRunner } from "./allocation.js";
import { printCloudEvent } from "./user-events.js";

type Message = Record<string, any>;
type UnaryCall = grpc.ServerUnaryCall<Message, Message>;
type Callback = grpc.sendUnaryData<Message>;
type WritableCall = grpc.ServerWritableStream<Message, Message>;
type LogLevel = "debug" | "info" | "warn" | "error";

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

const nativeImport = new Function("specifier", "return import(specifier)") as (
  specifier: string,
) => Promise<unknown>;

function rpcError(code: grpc.status, details: string): grpc.ServiceError {
  return Object.assign(new Error(details), { code, details, metadata: new grpc.Metadata() });
}

function requireSafeSize(value: unknown, field: string): number {
  if (!Number.isSafeInteger(value) || Number(value) < 0) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field} must be a non-negative safe integer`);
  }
  return Number(value);
}

function validateSerializedObjectManifest(manifest: Message | undefined, field: string): void {
  if (manifest == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.manifest is required`);
  }
  if (manifest.encoding == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.manifest.encoding is required`);
  }
  if (manifest.encodingVersion == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.manifest.encoding_version is required`);
  }
  requireSafeSize(manifest.encodingVersion, `${field}.manifest.encoding_version`);
  requireSafeSize(manifest.size, `${field}.manifest.size`);
  if (manifest.metadataSize == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.manifest.metadata_size is required`);
  }
  const metadataSize = requireSafeSize(manifest.metadataSize, `${field}.manifest.metadata_size`);
  if (metadataSize > Number(manifest.size)) {
    throw rpcError(
      grpc.status.INVALID_ARGUMENT,
      `${field}.manifest.metadata_size cannot exceed size`,
    );
  }
  if (typeof manifest.sha256Hash !== "string" || manifest.sha256Hash.length === 0) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.manifest.sha256_hash is required`);
  }
}

function validateBlob(blob: Message | undefined, field: string): void {
  if (blob == null) throw rpcError(grpc.status.INVALID_ARGUMENT, `${field} is required`);
  if (!Array.isArray(blob.chunks)) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.chunks must be an array`);
  }
  blob.chunks.forEach((chunk: Message | undefined, index: number) => {
    if (chunk == null || typeof chunk.uri !== "string" || chunk.uri.length === 0) {
      throw rpcError(grpc.status.INVALID_ARGUMENT, `${field}.chunks[${index}].uri is required`);
    }
    requireSafeSize(chunk.size, `${field}.chunks[${index}].size`);
  });
}

function validateNewAllocation(allocation: Message | undefined): asserts allocation is Message {
  if (allocation == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, "CreateAllocationRequest.allocation is required");
  }
  for (const [field, value] of [
    ["request_id", allocation.requestId],
    ["function_call_id", allocation.functionCallId],
    ["allocation_id", allocation.allocationId],
  ] as const) {
    if (typeof value !== "string" || value.length === 0) {
      throw rpcError(grpc.status.INVALID_ARGUMENT, `Allocation.${field} is required`);
    }
  }
  const inputs = allocation.inputs;
  if (inputs == null) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, "Allocation.inputs is required");
  }
  if (!Array.isArray(inputs.args) || !Array.isArray(inputs.argBlobs)) {
    throw rpcError(grpc.status.INVALID_ARGUMENT, "Allocation input args and arg_blobs must be arrays");
  }
  if (inputs.args.length !== inputs.argBlobs.length) {
    throw rpcError(
      grpc.status.INVALID_ARGUMENT,
      `Mismatched function arguments and argument blobs lengths, ${inputs.args.length} != ${inputs.argBlobs.length}`,
    );
  }
  inputs.args.forEach((argument: Message | undefined, index: number) => {
    if (argument == null) {
      throw rpcError(grpc.status.INVALID_ARGUMENT, `Allocation.inputs.args[${index}] is required`);
    }
    requireSafeSize(argument.offset, `Allocation.inputs.args[${index}].offset`);
    validateSerializedObjectManifest(argument.manifest, `Allocation.inputs.args[${index}]`);
    validateBlob(inputs.argBlobs[index], `Allocation.inputs.arg_blobs[${index}]`);
  });
  validateBlob(inputs.requestErrorBlob, "Allocation.inputs.request_error_blob");
}

function safeArchivePath(root: string, archivePath: string): string {
  const output = path.resolve(root, archivePath);
  if (output !== root && !output.startsWith(`${root}${path.sep}`)) {
    throw new Error(`Application archive contains unsafe path '${archivePath}'`);
  }
  return output;
}

async function unpackApplicationCode(data: Uint8Array): Promise<{ root: string; manifest: CodeManifest }> {
  const root = await mkdtemp(path.join(os.tmpdir(), "tensorlake-typescript-app-"));
  try {
    const files = unzipSync(data);
    for (const [name, content] of Object.entries(files)) {
      const destination = safeArchivePath(root, name);
      await mkdir(path.dirname(destination), { recursive: true });
      await writeFile(destination, content, { mode: 0o600 });
    }
    const manifest = JSON.parse(
      await readFile(path.join(root, ".tensorlake_code_manifest.json"), "utf8"),
    ) as CodeManifest;
    return { root, manifest };
  } catch (error) {
    await rm(root, { recursive: true, force: true });
    throw error;
  }
}

export class FunctionExecutorService {
  private readonly allocations = new Map<string, AllocationRunner>();
  private functionRef?: Message;
  private definition?: RegisteredDefinition;
  private applicationDefinition?: RegisteredDefinition;
  private initialization?: Promise<void>;
  private stopping = false;
  private shutdownPromise?: Promise<void>;

  readonly implementation: grpc.UntypedServiceImplementation = {
    initialize: this.initialize.bind(this),
    listAllocations: this.listAllocations.bind(this),
    createAllocation: this.createAllocation.bind(this),
    deleteAllocation: this.deleteAllocation.bind(this),
    watchAllocationState: this.watchAllocationState.bind(this),
    sendAllocationUpdate: this.sendAllocationUpdate.bind(this),
    getAllocationExecutionLogBatch: this.getAllocationExecutionLogBatch.bind(this),
    advanceAllocationExecutionLogBatch: this.advanceAllocationExecutionLogBatch.bind(this),
    watchAllocationEventLogReads: this.watchAllocationEventLogReads.bind(this),
    sendAllocationEventLogReadResponse: this.sendAllocationEventLogReadResponse.bind(this),
    checkHealth: this.checkHealth.bind(this),
    getInfo: this.getInfo.bind(this),
  };

  shutdown(reason: unknown = new Error("Function executor is shutting down")): Promise<void> {
    if (this.shutdownPromise != null) return this.shutdownPromise;
    this.stopping = true;
    const shutdown = (async () => {
      this.log("info", "function executor service shutdown requested", {
        allocation_count: this.allocations.size,
      });
      const allocations = [...this.allocations.values()];
      for (const allocation of allocations) allocation.cancel(reason);
      await Promise.all(allocations.map((allocation) => allocation.waitForCompletion()));
      this.log("info", "function executor service shutdown completed", {
        allocation_count: allocations.length,
      });
    })();
    this.shutdownPromise = shutdown;
    return shutdown;
  }

  private initialize(call: UnaryCall, callback: Callback): void {
    if (this.stopping) {
      callback(rpcError(grpc.status.UNAVAILABLE, "Function Executor is shutting down"));
      return;
    }
    const startedAt = Date.now();
    this.log("info", "initialize RPC received", {
      ...this.functionLogFields(call.request.function),
      application_code_bytes: call.request.applicationCode?.data?.length,
    });
    this.emitInitializationEvent(call.request.function, "function_executor_initialization_started", "Initializing function executor");
    void this.initializeAsync(call.request).then(
      () => {
        this.log("info", "initialize RPC completed", {
          ...this.functionLogFields(call.request.function),
          duration_ms: Date.now() - startedAt,
          outcome: "success",
        });
        this.emitInitializationEvent(call.request.function, "function_executor_initialization_finished", "Function executor initialization completed");
        callback(null, { outcomeCode: "INITIALIZATION_OUTCOME_CODE_SUCCESS" });
      },
      (error: unknown) => {
        const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
        this.log("error", "initialize RPC failed", {
          ...this.functionLogFields(call.request.function),
          duration_ms: Date.now() - startedAt,
          outcome: "failure",
        }, error);
        this.emitInitializationEvent(
          call.request.function,
          "function_executor_initialization_failed",
          "Function executor initialization failed",
          error,
        );
        callback(null, {
          outcomeCode: "INITIALIZATION_OUTCOME_CODE_FAILURE",
          failureReason: "INITIALIZATION_FAILURE_REASON_FUNCTION_ERROR",
          errorMessage: message,
        });
      },
    );
  }

  private emitInitializationEvent(
    functionRef: Message | undefined,
    event: string,
    message: string,
    error?: unknown,
  ): void {
    const rendered = error instanceof Error ? error.stack?.split("\n") : error == null ? undefined : [String(error)];
    printCloudEvent({
      ...(error == null ? {} : { level: "error" }),
      event,
      message,
      namespace: functionRef?.namespace,
      application: functionRef?.applicationName,
      application_version: functionRef?.applicationVersion,
      function: functionRef?.functionName,
      ...(rendered == null ? {} : { error: rendered }),
    });
  }

  private async initializeAsync(request: Message): Promise<void> {
    if (this.definition != null) throw new Error("Function Executor is already initialized");
    if (this.initialization != null) throw new Error("Function Executor initialization is already in progress");
    const initialization = this.initializeAttempt(request);
    this.initialization = initialization;
    try {
      await initialization;
    } finally {
      if (this.initialization === initialization) this.initialization = undefined;
    }
  }

  private async initializeAttempt(request: Message): Promise<void> {
    if (request.function == null) throw new Error("InitializeRequest.function is required");
    if (request.function.namespace == null) throw new Error("InitializeRequest.function.namespace is required");
    if (request.function.applicationName == null) throw new Error("InitializeRequest.function.application_name is required");
    if (request.function.functionName == null) throw new Error("InitializeRequest.function.function_name is required");
    if (request.function.applicationVersion == null) throw new Error("InitializeRequest.function.application_version is required");
    if (request.applicationCode == null) throw new Error("InitializeRequest.application_code is required");
    if (request.applicationCode.manifest == null) throw new Error("InitializeRequest.application_code.manifest is required");
    if (request.applicationCode.data == null) throw new Error("InitializeRequest.application_code.data is required");
    const codeManifest = request.applicationCode.manifest;
    if (codeManifest.encoding == null) throw new Error("InitializeRequest.application_code.manifest.encoding is required");
    if (codeManifest.encodingVersion == null) throw new Error("InitializeRequest.application_code.manifest.encoding_version is required");
    if (codeManifest.size == null) throw new Error("InitializeRequest.application_code.manifest.size is required");
    if (codeManifest.sha256Hash == null) throw new Error("InitializeRequest.application_code.manifest.sha256_hash is required");
    if (!new Set(["SERIALIZED_OBJECT_ENCODING_BINARY_ZIP", 4]).has(codeManifest.encoding)) {
      throw new Error(
        `Invalid application code encoding: ${String(codeManifest.encoding)}. Expected: BINARY_ZIP`,
      );
    }
    const archive = new Uint8Array(request.applicationCode.data);
    const declaredSize = requireSafeSize(
      codeManifest.size,
      "InitializeRequest.application_code.manifest.size",
    );
    if (declaredSize !== archive.byteLength) {
      throw new Error(
        `Application code size mismatch: manifest declares ${declaredSize} bytes, received ${archive.byteLength}`,
      );
    }
    const metadataSize = codeManifest.metadataSize == null
      ? 0
      : requireSafeSize(
        codeManifest.metadataSize,
        "InitializeRequest.application_code.manifest.metadata_size",
      );
    if (metadataSize !== 0) {
      throw new Error("Application code metadata is not supported");
    }
    const actualHash = createHash("sha256").update(archive).digest("hex");
    if (String(codeManifest.sha256Hash).toLowerCase() !== actualHash) {
      throw new Error(
        `Application code SHA-256 mismatch: expected ${String(codeManifest.sha256Hash)}, received ${actualHash}`,
      );
    }
    this.log("debug", "unpacking application code", {
      ...this.functionLogFields(request.function),
      application_code_bytes: archive.byteLength,
    });
    const unpackStartedAt = Date.now();
    const { root, manifest } = await unpackApplicationCode(archive);
    let registrySnapshot: ReturnType<typeof snapshotRegistry> | undefined;
    try {
      this.log("debug", "application code unpacked", {
        ...this.functionLogFields(request.function),
        duration_ms: Date.now() - unpackStartedAt,
        manifest_format_version: manifest.format_version,
        manifest_runtime: manifest.runtime,
        manifest_function_count: Object.keys(manifest.functions ?? {}).length,
      });
      if (manifest.format_version !== 2 || manifest.runtime !== "typescript") {
        throw new Error("Application code is not a Tensorlake TypeScript bundle");
      }
      const nodeMajor = Number(process.versions.node.split(".")[0]);
      if ((manifest.minimum_node_major ?? 24) > nodeMajor) {
        throw new Error(`Application requires Node ${manifest.minimum_node_major} or newer; executor has ${nodeMajor}`);
      }
      const moduleName = manifest.module;
      if (!moduleName) throw new Error("Application code manifest does not name a runtime module");
      const functionName = String(request.function.functionName);
      if (manifest.functions?.[functionName] == null) {
        throw new Error(`Function '${functionName}' is not present in the application bundle`);
      }
      const modulePath = safeArchivePath(root, moduleName);
      this.log("debug", "importing application runtime module", {
        ...this.functionLogFields(request.function),
        module: moduleName,
      });
      const importStartedAt = Date.now();
      registrySnapshot = snapshotRegistry();
      const runtime = await nativeImport(`${pathToFileURL(modulePath).href}?executor=${Date.now()}`) as RuntimeModule;
      if (typeof runtime.__tensorlakeGetFunction !== "function") {
        throw new Error("Application runtime does not export __tensorlakeGetFunction");
      }
      const definition = runtime.__tensorlakeGetFunction(functionName);
      const applicationDefinition = runtime.__tensorlakeGetFunction(request.function.applicationName);
      if (applicationDefinition.application == null) {
        throw new Error(`'${request.function.applicationName}' is not a Tensorlake application`);
      }
      this.functionRef = request.function;
      this.definition = definition;
      this.applicationDefinition = applicationDefinition;
      this.log("debug", "application function resolved", {
        ...this.functionLogFields(request.function),
        duration_ms: Date.now() - importStartedAt,
        parameter_count: definition.parameters.length,
        application_max_retries: applicationDefinition.application.retries.maxRetries,
      });
      registrySnapshot = undefined;
    } catch (error) {
      if (registrySnapshot != null) restoreRegistry(registrySnapshot);
      this.functionRef = undefined;
      this.definition = undefined;
      this.applicationDefinition = undefined;
      await rm(root, { recursive: true, force: true });
      throw error;
    }
  }

  private getAllocation(id: string): AllocationRunner {
    const allocation = this.allocations.get(id);
    if (allocation == null) throw rpcError(grpc.status.NOT_FOUND, `Allocation ${id} not found`);
    return allocation;
  }

  private listAllocations(_call: UnaryCall, callback: Callback): void {
    this.log("debug", "list allocations RPC received", { allocation_count: this.allocations.size });
    callback(null, { allocations: [...this.allocations.values()].map((runner) => runner.allocation) });
  }

  private createAllocation(call: UnaryCall, callback: Callback): void {
    void this.createAllocationAsync(call).then(
      () => callback(null, {}),
      (error: unknown) => {
        this.log(
          "error",
          "create allocation RPC failed",
          this.allocationLogFields(call.request.allocation),
          error,
        );
        callback(error as grpc.ServiceError);
      },
    );
  }

  private async createAllocationAsync(call: UnaryCall): Promise<void> {
    if (this.stopping) {
      throw rpcError(grpc.status.UNAVAILABLE, "Function Executor is shutting down");
    }
    const initialization = this.initialization;
    if (initialization != null) {
      this.log("info", "create allocation RPC waiting for initialization", {
        ...this.allocationLogFields(call.request.allocation),
      });
      await this.waitForInitialization(call, initialization);
      this.log("info", "create allocation RPC resumed after initialization", {
        ...this.allocationLogFields(call.request.allocation),
      });
    }
    this.assertAllocationAdmissionActive(call);
    if (this.stopping) {
      throw rpcError(grpc.status.UNAVAILABLE, "Function Executor is shutting down");
    }
    if (this.definition == null || this.applicationDefinition == null || this.functionRef == null) {
      throw rpcError(grpc.status.FAILED_PRECONDITION, "Function Executor is not initialized");
    }
    const allocation = call.request.allocation;
    validateNewAllocation(allocation);
    const id = String(allocation.allocationId);
    this.log("info", "create allocation RPC received", {
      ...this.allocationLogFields(allocation),
      replay_mode: allocation?.replayMode,
      input_count: allocation?.inputs?.args?.length ?? 0,
    });
    if (this.allocations.has(id)) throw rpcError(grpc.status.ALREADY_EXISTS, `Allocation ${id} already exists`);
    const runner = new AllocationRunner(
      allocation,
      this.functionRef,
      this.definition,
      this.applicationDefinition,
    );
    this.allocations.set(id, runner);
    runner.start();
    this.log("info", "allocation runner started", this.allocationLogFields(allocation));
  }

  private waitForInitialization(call: UnaryCall, initialization: Promise<void>): Promise<void> {
    return new Promise((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      let settled = false;
      const deadline = typeof call.getDeadline === "function" ? call.getDeadline() : Infinity;
      const deadlineMs = deadline instanceof Date ? deadline.getTime() : Number(deadline);
      const cancellationError = () =>
        Number.isFinite(deadlineMs) && deadlineMs <= Date.now()
          ? rpcError(
            grpc.status.DEADLINE_EXCEEDED,
            "Initialization did not finish before the allocation deadline",
          )
          : rpcError(
            grpc.status.CANCELLED,
            "Create allocation RPC was cancelled during initialization",
          );
      const onCancelled = () => finish(cancellationError());
      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        if (timer != null) clearTimeout(timer);
        if (typeof call.off === "function") call.off("cancelled", onCancelled);
        if (error == null) resolve();
        else reject(error);
      };

      if (
        call.cancelled
        || (Number.isFinite(deadlineMs) && deadlineMs <= Date.now())
      ) {
        finish(cancellationError());
        return;
      }
      if (typeof call.once === "function") call.once("cancelled", onCancelled);

      if (Number.isFinite(deadlineMs)) {
        const remainingMs = deadlineMs - Date.now();
        if (remainingMs <= 0) {
          finish(rpcError(grpc.status.DEADLINE_EXCEEDED, "Initialization did not finish before the allocation deadline"));
          return;
        }
        timer = setTimeout(() => {
          finish(rpcError(
            grpc.status.DEADLINE_EXCEEDED,
            "Initialization did not finish before the allocation deadline",
          ));
        }, remainingMs);
      }

      initialization.then(
        () => finish(
          call.cancelled || (Number.isFinite(deadlineMs) && deadlineMs <= Date.now())
            ? cancellationError()
            : undefined,
        ),
        () => finish(rpcError(
          grpc.status.FAILED_PRECONDITION,
          "Function Executor initialization failed",
        )),
      );
    });
  }

  private assertAllocationAdmissionActive(call: UnaryCall): void {
    const deadline = typeof call.getDeadline === "function" ? call.getDeadline() : Infinity;
    const deadlineMs = deadline instanceof Date ? deadline.getTime() : Number(deadline);
    if (Number.isFinite(deadlineMs) && deadlineMs <= Date.now()) {
      throw rpcError(grpc.status.DEADLINE_EXCEEDED, "Create allocation RPC deadline exceeded");
    }
    if (call.cancelled) {
      throw rpcError(grpc.status.CANCELLED, "Create allocation RPC was cancelled before allocation start");
    }
  }

  private deleteAllocation(call: UnaryCall, callback: Callback): void {
    try {
      const id = String(call.request.allocationId ?? "");
      const allocation = this.getAllocation(id);
      this.log("debug", "delete allocation RPC received", {
        allocation_id: id,
        finished: allocation.isFinished,
      });
      if (!allocation.isFinished) {
        throw rpcError(grpc.status.FAILED_PRECONDITION, `Allocation ${id} is still running`);
      }
      this.allocations.delete(id);
      this.log("info", "allocation deleted", { allocation_id: id });
      callback(null, {});
    } catch (error) {
      this.log("error", "delete allocation RPC failed", { allocation_id: call.request.allocationId }, error);
      callback(error as grpc.ServiceError);
    }
  }

  private watchAllocationState(call: WritableCall): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      this.log("debug", "allocation state stream opening", { allocation_id: allocationId });
      this.getAllocation(allocationId).watchState(call);
    } catch (error) {
      this.log("error", "allocation state stream failed to open", { allocation_id: call.request.allocationId }, error);
      call.destroy(error as Error);
    }
  }

  private sendAllocationUpdate(call: UnaryCall, callback: Callback): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      this.log("debug", "allocation update RPC received", {
        allocation_id: allocationId,
        update_kind: call.request.outputBlob != null
          ? "output_blob"
          : call.request.requestStateOperationResult != null
            ? "request_state_operation_result"
            : "unknown",
      });
      const allocation = this.getAllocation(allocationId);
      if (allocation.isFinished) throw rpcError(grpc.status.FAILED_PRECONDITION, "Allocation is already finished");
      allocation.deliverUpdate(call.request);
      callback(null, {});
    } catch (error) {
      this.log("error", "allocation update RPC failed", { allocation_id: call.request.allocationId }, error);
      callback(error as grpc.ServiceError);
    }
  }

  private getAllocationExecutionLogBatch(call: UnaryCall, callback: Callback): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      this.log("debug", "execution log batch RPC waiting", { allocation_id: allocationId });
      const allocation = this.getAllocation(allocationId);
      void allocation.getExecutionBatch().then(
        (events) => {
          this.log("debug", "execution log batch RPC completed", {
            allocation_id: allocationId,
            event_count: events.length,
          });
          callback(null, { events });
        },
        (error) => {
          this.log("error", "execution log batch RPC failed", { allocation_id: allocationId }, error);
          callback(error as grpc.ServiceError);
        },
      );
    } catch (error) {
      this.log("error", "execution log batch RPC failed", { allocation_id: call.request.allocationId }, error);
      callback(error as grpc.ServiceError);
    }
  }

  private advanceAllocationExecutionLogBatch(call: UnaryCall, callback: Callback): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      this.log("debug", "advance execution log batch RPC received", { allocation_id: allocationId });
      this.getAllocation(allocationId).advanceExecutionBatch();
      callback(null, {});
    } catch (error) {
      this.log("error", "advance execution log batch RPC failed", { allocation_id: call.request.allocationId }, error);
      callback(error as grpc.ServiceError);
    }
  }

  private watchAllocationEventLogReads(call: WritableCall): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      const allocation = this.getAllocation(allocationId);
      this.log("debug", "event log read stream opening", { allocation_id: allocationId });

      // grpc-js sends response metadata lazily on the first message. EventLog
      // streams normally have no message until user code starts a durable call,
      // while the dataplane waits for response metadata before it starts polling
      // execution batches. Send headers explicitly to break that startup cycle.
      call.sendMetadata(new grpc.Metadata());
      this.log("debug", "event log read stream established", { allocation_id: allocationId });
      allocation.watchEventLogReads(call);
    } catch (error) {
      this.log("error", "event log read stream failed to open", { allocation_id: call.request.allocationId }, error);
      call.destroy(error as Error);
    }
  }

  private sendAllocationEventLogReadResponse(call: UnaryCall, callback: Callback): void {
    try {
      const allocationId = String(call.request.allocationId ?? "");
      this.log("debug", "event log read response RPC received", {
        allocation_id: allocationId,
        entry_count: call.request.entries?.length ?? 0,
        last_clock: call.request.lastClock,
        has_more: call.request.hasMore,
      });
      this.getAllocation(allocationId).deliverEventLogResponse(call.request);
      callback(null, {});
    } catch (error) {
      this.log("error", "event log read response RPC failed", { allocation_id: call.request.allocationId }, error);
      callback(error as grpc.ServiceError);
    }
  }

  private checkHealth(_call: UnaryCall, callback: Callback): void {
    this.log("debug", "health check RPC received", { initialized: this.definition != null });
    if (this.definition == null) {
      callback(rpcError(grpc.status.UNAVAILABLE, "Function Executor is not initialized"));
    } else {
      callback(null, { healthy: true, statusMessage: "ok" });
    }
  }

  private getInfo(_call: UnaryCall, callback: Callback): void {
    this.log("debug", "executor info RPC received", { protocol_version: "0.1.3" });
    callback(null, {
      version: "0.1.3",
      sdkVersion: SDK_VERSION,
      sdkLanguage: "typescript",
      sdkLanguageVersion: process.versions.node,
    });
  }

  private functionLogFields(functionRef: Message | undefined): Message {
    return {
      namespace: functionRef?.namespace,
      application: functionRef?.applicationName,
      application_version: functionRef?.applicationVersion,
      function: functionRef?.functionName,
    };
  }

  private allocationLogFields(allocation: Message | undefined): Message {
    return {
      allocation_id: allocation?.allocationId,
      request_id: allocation?.requestId,
      function_call_id: allocation?.functionCallId,
    };
  }

  private log(level: LogLevel, message: string, fields: Message = {}, error?: unknown): void {
    const rendered = error instanceof Error
      ? error.stack?.split("\n") ?? [`${error.name}: ${error.message}`]
      : error == null ? undefined : [String(error)];
    process.stderr.write(`${JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      component: "typescript_function_executor_service",
      message,
      ...fields,
      ...(rendered == null ? {} : { error: rendered }),
    })}\n`);
  }
}
