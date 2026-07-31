import { AsyncResource } from "node:async_hooks";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import {
  Future,
  RequestContext,
  registerApplication,
  registerFunction,
  retries,
  schema,
} from "../src/applications/index.js";
import type { RegisteredDefinition } from "../src/applications/function.js";
import { clearRegistryForTest } from "../src/applications/registry.js";
import {
  AllocationRunner,
  ReplayCausality,
} from "../src/function-executor/allocation.js";
import {
  deserializeValueFromProtocol,
  downloadSerializedObject,
  prepareSerializedObject,
} from "../src/function-executor/blob.js";

type Message = Record<string, any>;

const temporaryDirectories: string[] = [];
const temporaryIntervals: NodeJS.Timeout[] = [];
let customResolveAnyCalls = 0;
let customThenableAnyCalls = 0;
const promiseRaceCapturedBeforeAllocation = Promise.race.bind(Promise) as <T>(
  values: Iterable<T | PromiseLike<T>>,
) => Promise<Awaited<T>>;

function testDurableHash(values: string[]): string {
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

afterEach(async () => {
  for (const interval of temporaryIntervals.splice(0)) clearInterval(interval);
  await Promise.all(temporaryDirectories.splice(0).map((directory) =>
    rm(directory, { recursive: true, force: true }),
  ));
});

async function withDeadline<T>(promise: Promise<T>, description: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_resolve, reject) => {
        timer = setTimeout(() => reject(new Error(`Timed out waiting for ${description}`)), 1_500);
      }),
    ]);
  } finally {
    if (timer != null) clearTimeout(timer);
  }
}

async function createRunner(
  definition: RegisteredDefinition,
  options: { replayMode?: string; requestErrorCapacity?: number } = {},
): Promise<{
  directory: string;
  requestErrorPath?: string;
  runner: AllocationRunner;
}> {
  const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-protocol-test-"));
  temporaryDirectories.push(directory);
  const input = new TextEncoder().encode("{}");
  const inputPath = path.join(directory, "input");
  await writeFile(inputPath, input);
  const requestErrorPath = options.requestErrorCapacity == null
    ? undefined
    : path.join(directory, "request-error");
  const applicationName = definition.name;
  const runner = new AllocationRunner({
    requestId: `request-${applicationName}`,
    functionCallId: `call-${applicationName}`,
    allocationId: `allocation-${applicationName}`,
    replayMode: options.replayMode ?? "REPLAY_MODE_NONE",
    inputs: {
      args: [{
        offset: 0,
        manifest: {
          encoding: "SERIALIZED_OBJECT_ENCODING_RAW",
          size: input.byteLength,
          metadataSize: 0,
          sha256Hash: createHash("sha256").update(input).digest("hex"),
          contentType: "application/json",
        },
      }],
      argBlobs: [{
        id: "input",
        chunks: [{ uri: pathToFileURL(inputPath).href, size: input.byteLength }],
      }],
      functionCallMetadata: Buffer.alloc(0),
      ...(requestErrorPath == null ? {} : {
        requestErrorBlob: {
          id: "request-error",
          chunks: [{
            uri: pathToFileURL(requestErrorPath).href,
            size: options.requestErrorCapacity,
          }],
        },
      }),
    },
  }, {
    namespace: "default",
    applicationName,
    applicationVersion: "v1",
    functionName: applicationName,
  }, definition);
  return { directory, requestErrorPath, runner };
}

function attachBlobResponder(runner: AllocationRunner, directory: string): void {
  const seen = new Set<string>();
  runner.watchState({
    write(state) {
      for (const request of state.outputBlobRequests ?? []) {
        const id = String(request.id);
        if (seen.has(id)) continue;
        seen.add(id);
        queueMicrotask(() => runner.deliverUpdate({
          outputBlob: {
            status: { code: 0 },
            blob: {
              id,
              chunks: [{
                uri: pathToFileURL(path.join(directory, `output-${id}`)).href,
                size: request.size,
              }],
            },
          },
        }));
      }
      return true;
    },
    end() {},
    on() {},
  });
}

function attachEventDriver(runner: AllocationRunner): {
  readonly readCount: number;
  respond(entries: Message[], lastClock: number, hasMore?: boolean): Promise<Message>;
} {
  const reads: Message[] = [];
  const waiters: Array<(request: Message) => void> = [];
  let readCount = 0;
  runner.watchEventLogReads({
    write(request) {
      readCount += 1;
      const waiter = waiters.shift();
      if (waiter == null) reads.push(request);
      else waiter(request);
      return true;
    },
    end() {},
    on() {},
  });
  const nextRead = (): Promise<Message> => {
    const request = reads.shift();
    if (request != null) return Promise.resolve(request);
    return new Promise((resolve) => waiters.push(resolve));
  };
  return {
    get readCount() {
      return readCount;
    },
    async respond(entries, lastClock, hasMore = false) {
      const request = await withDeadline(nextRead(), "an event-log read");
      runner.deliverEventLogResponse({
        allocationId: request.allocationId,
        entries,
        lastClock,
        hasMore,
      });
      return request;
    },
  };
}

async function readFinishValue(finish: Message): Promise<unknown> {
  const value = await downloadSerializedObject(
    finish.value,
    finish.uploadedFunctionOutputsBlob,
  );
  return deserializeValueFromProtocol(value);
}

describe("TypeScript function executor protocol conformance", () => {
  it("weakly references promises and releases destroyed replay resources", async () => {
    const causality = new ReplayCausality(() => undefined);
    const internals = causality as unknown as {
      promiseIds: WeakMap<object, number>;
      resources: Map<number, { promise?: WeakRef<object> }>;
    };
    try {
      const promise = new Promise<void>(() => undefined);
      const promiseId = internals.promiseIds.get(promise);
      expect(promiseId).toBeTypeOf("number");
      expect(internals.resources.get(promiseId as number)?.promise).toBeInstanceOf(WeakRef);

      const resource = new AsyncResource("TensorlakeReplayCleanupTest");
      const resourceId = resource.asyncId();
      expect(internals.resources.has(resourceId)).toBe(true);
      resource.emitDestroy();
      await new Promise<void>((resolve) => setImmediate(resolve));
      expect(internals.resources.has(resourceId)).toBe(false);
    } finally {
      causality.stop();
    }
  });

  it("does not assign application causality to executor-owned async resources", () => {
    const causality = new ReplayCausality(() => undefined);
    const internals = causality as unknown as {
      owners: Map<number, Set<number>>;
    };
    const root = new AsyncResource("TensorlakeReplayOwnershipRoot");
    let applicationResource!: AsyncResource;
    let executorResource!: AsyncResource;
    try {
      root.runInAsyncScope(() => {
        causality.beginCurrentContinuation();
        applicationResource = new AsyncResource("TensorlakeApplicationResource");
        executorResource = causality.runWithoutOwnership(
          () => new AsyncResource("TensorlakeExecutorLogResource"),
        );
      });

      expect(internals.owners.get(applicationResource.asyncId())?.size).toBeGreaterThan(0);
      expect(internals.owners.has(executorResource.asyncId())).toBe(false);
    } finally {
      applicationResource?.emitDestroy();
      executorResource?.emitDestroy();
      root.emitDestroy();
      causality.stop();
    }
  });

  it("aborts a running handler and emits one terminal failure", async () => {
    clearRegistryForTest();
    let handlerStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      handlerStarted = resolve;
    });
    let observedSignal: AbortSignal | undefined;
    const application = registerApplication("protocol_cancellable_parent", async () => {
      observedSignal = RequestContext.get().signal;
      handlerStarted();
      await new Promise<never>(() => undefined);
    });
    const { runner } = await createRunner(application.definition);

    runner.start();
    await withDeadline(started, "the cancellable handler to start");
    const reason = new Error("executor shutdown");
    runner.cancel(reason);

    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the cancellation terminal batch");
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
      },
    }]);
    expect(observedSignal?.aborted).toBe(true);
    expect(observedSignal?.reason).toBe(reason);
    runner.advanceExecutionBatch();
    expect(await runner.getExecutionBatch()).toEqual([]);
  });

  it("finishes an allocation after user code replaces both process output writers", async () => {
    clearRegistryForTest();
    const stdoutDescriptor = Object.getOwnPropertyDescriptor(process.stdout, "write");
    const stderrDescriptor = Object.getOwnPropertyDescriptor(process.stderr, "write");
    const replaceWriter = (stream: NodeJS.WriteStream, name: string) => {
      Object.defineProperty(stream, "write", {
        configurable: true,
        writable: true,
        value: () => {
          throw new Error(`${name} was replaced by user code`);
        },
      });
    };
    const restoreWriter = (
      stream: NodeJS.WriteStream,
      descriptor: PropertyDescriptor | undefined,
    ) => {
      if (descriptor == null) delete (stream as unknown as { write?: unknown }).write;
      else Object.defineProperty(stream, "write", descriptor);
    };
    const application = registerApplication(
      "protocol_damaged_stdio_parent",
      async () => {
        replaceWriter(process.stdout, "stdout");
        replaceWriter(process.stderr, "stderr");
        throw new Error("expected user failure");
      },
    );
    const { runner } = await createRunner(application.definition);
    let terminalBatch: Message[] | undefined;

    try {
      runner.start();
      terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        "the damaged-stdio terminal batch",
      );
      await runner.waitForCompletion();
    } finally {
      restoreWriter(process.stdout, stdoutDescriptor);
      restoreWriter(process.stderr, stderrDescriptor);
    }

    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
      },
    }]);
    const internals = runner as unknown as {
      currentRead?: unknown;
      liveWaiters: unknown[];
    };
    expect(internals.liveWaiters).toHaveLength(0);
    expect(internals.currentRead).toBeUndefined();
  });

  it("closes every protocol stream when individual stream endings fail", async () => {
    clearRegistryForTest();
    const application = registerApplication(
      "protocol_broken_stream_end_parent",
      async () => {
        throw new Error("expected user failure");
      },
    );
    const { runner } = await createRunner(application.definition);
    let stateEndCount = 0;
    let eventEndCount = 0;
    runner.watchState({
      write: () => true,
      end() {
        stateEndCount += 1;
        throw new Error("state stream end failed");
      },
      on() {},
    });
    runner.watchEventLogReads({
      write: () => true,
      end() {
        eventEndCount += 1;
        throw new Error("event stream end failed");
      },
      on() {},
    });

    runner.start();
    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the broken-stream terminal batch",
    );
    await withDeadline(runner.waitForCompletion(), "broken-stream completion");

    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
      },
    }]);
    expect(stateEndCount).toBe(1);
    expect(eventEndCount).toBe(1);
  });

  it("aborts an in-flight output upload instead of reporting success", async () => {
    clearRegistryForTest();
    const application = registerApplication("protocol_cancellable_upload", async () => "finished");
    const { runner } = await createRunner(application.definition);
    let uploadStarted!: () => void;
    const started = new Promise<void>((resolve) => {
      uploadStarted = resolve;
    });
    const server = createServer((request) => {
      request.on("error", () => undefined);
      request.resume();
      uploadStarted();
    });
    const port = await new Promise<number>((resolve, reject) => {
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        if (typeof address === "object" && address != null) resolve(address.port);
        else reject(new Error("test HTTP server did not bind to a TCP port"));
      });
      server.once("error", reject);
    });
    let outputDelivered = false;
    runner.watchState({
      write(state) {
        for (const request of state.outputBlobRequests ?? []) {
          if (outputDelivered) continue;
          outputDelivered = true;
          queueMicrotask(() => runner.deliverUpdate({
            outputBlob: {
              status: { code: 0 },
              blob: {
                id: request.id,
                chunks: [{
                  uri: `http://127.0.0.1:${port}/blocked-output`,
                  size: request.size,
                }],
              },
            },
          }));
        }
        return true;
      },
      end() {},
      on() {},
    });
    try {
      runner.start();
      await withDeadline(started, "the output upload to start");
      runner.cancel(new Error("executor shutdown"));
      await withDeadline(runner.waitForCompletion(), "the cancelled allocation to finish");

      const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the upload cancellation terminal batch");
      expect(terminalBatch).toEqual([{
        finishAllocation: {
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
        },
      }]);
      runner.advanceExecutionBatch();
      expect(await runner.getExecutionBatch()).toEqual([]);
    } finally {
      server.closeAllConnections();
      await new Promise<void>((resolve, reject) =>
        server.close((error) => error == null ? resolve() : reject(error))
      );
    }
  });

  it("settles detached durable waiters when the allocation finishes", async () => {
    clearRegistryForTest();
    const child = registerFunction("protocol_detached_child", async () => 1);
    const application = registerApplication("protocol_detached_parent", async () => {
      void child.future().run();
      // Give the detached operation enough time to emit its creation event and
      // start waiting for the acknowledgement that this test intentionally
      // never sends.
      await Promise.resolve();
      await Promise.resolve();
      return 7;
    });
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);

    runner.start();
    const createBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the detached child creation batch",
    );
    expect(createBatch).toHaveLength(1);
    expect(createBatch[0].createFunctionCall).toBeDefined();
    runner.advanceExecutionBatch();

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the detached parent terminal batch",
    );
    expect(terminalBatch[0].finishAllocation.outcomeCode).toBe(
      "ALLOCATION_OUTCOME_CODE_SUCCESS",
    );
    await withDeadline(runner.waitForCompletion(), "detached parent completion");
    await Promise.resolve();

    const internals = runner as unknown as {
      currentRead?: unknown;
      liveWaiters: unknown[];
      outputBlobRequests: Map<string, unknown>;
      stateOperationRequests: Map<string, unknown>;
    };
    expect(internals.currentRead).toBeUndefined();
    expect(internals.liveWaiters).toHaveLength(0);
    expect(internals.outputBlobRequests.size).toBe(0);
    expect(internals.stateOperationRequests.size).toBe(0);
  });

  it("consumes reordered and duplicate durable events and emits exactly one terminal batch", async () => {
    clearRegistryForTest();
    const child = registerFunction(async (value: number) => value * 2, {
      name: "protocol_reordered_child",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
    });
    const application = registerApplication(async () => (await child(5)) + 1, {
      name: "protocol_reordered_parent",
      parameters: [] as const,
      returns: schema.number(),
    });
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    runner.start();
    const createBatch = await withDeadline(runner.getExecutionBatch(), "the child creation batch");
    expect(createBatch).toHaveLength(1);
    const durableId = String(createBatch[0].createFunctionCall.updates.rootFunctionCallId);
    runner.advanceExecutionBatch();

    const childOutput = prepareSerializedObject(10, 0, durableId);
    const childOutputPath = path.join(directory, "child-output");
    await writeFile(childOutputPath, childOutput.bytes);
    const watcherResult = {
      functionCallId: durableId,
      watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
      outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
      valueOutput: childOutput.object,
      valueBlob: {
        id: "child-output",
        chunks: [{ uri: pathToFileURL(childOutputPath).href, size: childOutput.bytes.byteLength }],
      },
    };
    await events.respond([
      { clock: 1, functionCallWatcherResult: watcherResult },
      { clock: 2, functionCallWatcherResult: watcherResult },
      { clock: 3, functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } } },
      { clock: 4, functionCallCreated: { functionCallId: durableId, status: { code: 0 } } },
    ], 4);

    const watcherBatch = await withDeadline(runner.getExecutionBatch(), "the child watcher batch");
    expect(watcherBatch).toEqual([{ createFunctionCallWatcher: { functionCallId: durableId } }]);
    runner.advanceExecutionBatch();
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the terminal batch");
    expect(terminalBatch).toHaveLength(1);
    expect(terminalBatch[0].finishAllocation.outcomeCode).toBe("ALLOCATION_OUTCOME_CODE_SUCCESS");
    expect(await readFinishValue(terminalBatch[0].finishAllocation)).toBe(11);
    runner.advanceExecutionBatch();

    expect(await runner.getExecutionBatch()).toEqual([]);
    expect(events.readCount).toBe(1);
  });

  it("emits concurrent map calls and watchers in invocation order and replays them", async () => {
    clearRegistryForTest();
    const child = registerFunction("protocol_ordered_map_child", async (value: number) => value * 2);
    let invocation = 0;
    const application = registerApplication("protocol_ordered_map_parent", async () => {
      const replaying = invocation > 0;
      invocation += 1;
      const delayed = (value: number) =>
        new Promise<number>((resolve) => setTimeout(() => resolve(value), 20));
      return child.map(replaying
        ? [Promise.resolve(1), delayed(2)]
        : [delayed(1), Promise.resolve(2)]);
    });
    const { directory, runner } = await createRunner(application.definition);
    const seenBlobs = new Set<string>();
    let firstBlob: Message | undefined;
    let childBlobCount = 0;
    const respond = (request: Message) => runner.deliverUpdate({
      outputBlob: {
        status: { code: 0 },
        blob: {
          id: request.id,
          chunks: [{
            uri: pathToFileURL(path.join(directory, `ordered-output-${request.id}`)).href,
            size: request.size,
          }],
        },
      },
    });
    runner.watchState({
      write(state) {
        for (const request of state.outputBlobRequests ?? []) {
          if (seenBlobs.has(request.id)) continue;
          seenBlobs.add(request.id);
          if (childBlobCount === 0) {
            firstBlob = request;
          } else if (childBlobCount === 1) {
            const delayed = firstBlob;
            queueMicrotask(() => {
              respond(request);
              if (delayed != null) respond(delayed);
            });
          } else {
            queueMicrotask(() => respond(request));
          }
          childBlobCount += 1;
        }
        return true;
      },
      end() {},
      on() {},
    });
    const events = attachEventDriver(runner);
    runner.start();

    const createBatches: Message[][] = [];
    for (let index = 0; index < 2; index += 1) {
      const batch = await withDeadline(runner.getExecutionBatch(), `ordered map creation ${index}`);
      createBatches.push(batch);
      runner.advanceExecutionBatch();
    }
    const durableIds = createBatches.map((batch) =>
      String(batch[0].createFunctionCall.updates.rootFunctionCallId)
    );
    const callValues = await Promise.all(createBatches.map(async (batch) => {
      const creation = batch[0].createFunctionCall;
      const argument = creation.updates.updates[0].functionCall.args[0].value;
      return deserializeValueFromProtocol(await downloadSerializedObject(argument, creation.argsBlob));
    }));
    expect(callValues).toEqual([1, 2]);

    await events.respond(durableIds.map((functionCallId, index) => ({
      clock: index + 1,
      functionCallCreated: { functionCallId, status: { code: 0 } },
    })), 2);
    const watcherBatches: Message[][] = [];
    for (let index = 0; index < 2; index += 1) {
      const batch = await withDeadline(runner.getExecutionBatch(), `ordered map watcher ${index}`);
      watcherBatches.push(batch);
      runner.advanceExecutionBatch();
    }
    expect(watcherBatches.map((batch) => batch[0].createFunctionCallWatcher.functionCallId)).toEqual(durableIds);

    const results = await Promise.all(durableIds.map(async (durableId, index) => {
      const prepared = prepareSerializedObject((index + 1) * 2, 0, durableId);
      const outputPath = path.join(directory, `ordered-child-${index}`);
      await writeFile(outputPath, prepared.bytes);
      return {
        functionCallId: durableId,
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        valueOutput: prepared.object,
        valueBlob: {
          id: `ordered-child-${index}`,
          chunks: [{ uri: pathToFileURL(outputPath).href, size: prepared.bytes.byteLength }],
        },
      };
    }));
    const watcherHistory = durableIds.flatMap((functionCallId, index) => [
      {
        clock: index * 2 + 3,
        functionCallWatcherCreated: { functionCallId, status: { code: 0 } },
      },
      { clock: index * 2 + 4, functionCallWatcherResult: results[index] },
    ]);
    await events.respond(watcherHistory, 6);
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "ordered map terminal batch");
    expect(await readFinishValue(terminalBatch[0].finishAllocation)).toEqual([2, 4]);

    const replay = await createRunner(application.definition, { replayMode: "REPLAY_MODE_STRICT" });
    attachBlobResponder(replay.runner, replay.directory);
    const replayEvents = attachEventDriver(replay.runner);
    replay.runner.start();
    await replayEvents.respond(durableIds.flatMap((functionCallId, index) => [
      {
        clock: index * 3 + 1,
        functionCallCreated: { functionCallId, status: { code: 0 } },
      },
      {
        clock: index * 3 + 2,
        functionCallWatcherCreated: { functionCallId, status: { code: 0 } },
      },
      { clock: index * 3 + 3, functionCallWatcherResult: results[index] },
    ]), 6);
    const replayTerminal = await withDeadline(
      replay.runner.getExecutionBatch(),
      "ordered map replay terminal batch",
    );
    expect(replayTerminal).toHaveLength(1);
    expect(await readFinishValue(replayTerminal[0].finishAllocation)).toEqual([2, 4]);
  });

  it("does not reveal watcher results before their original replay position", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    const child = registerFunction("protocol_causal_replay_child", async (value: number) => value * 2);
    const application = registerApplication("protocol_causal_replay_parent", async () => {
      const first = child.future(1).run();
      const second = child.future(2).run();
      const waited = await Future.wait([first, second], { returnWhen: "first_completed" });
      // A replayed result may resume arbitrary asynchronous application work
      // before that continuation emits its next durable call. The callback may
      // come from a resource initialized before this allocation, as it does for
      // module-scope clients and connection pools.
      await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
      const marker = await child(3);
      return {
        done: waited.done.length,
        marker,
        results: [await first, await second],
      };
    });
    const live = await createRunner(application.definition);
    attachBlobResponder(live.runner, live.directory);
    const liveEvents = attachEventDriver(live.runner);
    live.runner.start();

    const creationBatches: Message[][] = [];
    for (let index = 0; index < 2; index += 1) {
      const batch = await withDeadline(
        live.runner.getExecutionBatch(),
        `causal replay child creation ${index}`,
      );
      creationBatches.push(batch);
      live.runner.advanceExecutionBatch();
    }
    const childIds = creationBatches.map((batch) =>
      String(batch[0].createFunctionCall.updates.rootFunctionCallId)
    );
    const history: Message[] = childIds.map((functionCallId, index) => ({
      clock: index + 1,
      functionCallCreated: { functionCallId, status: { code: 0 } },
    }));
    await liveEvents.respond(history, 2);

    for (let index = 0; index < 2; index += 1) {
      const batch = await withDeadline(
        live.runner.getExecutionBatch(),
        `causal replay child watcher ${index}`,
      );
      expect(batch[0].createFunctionCallWatcher.functionCallId).toBe(childIds[index]);
      live.runner.advanceExecutionBatch();
    }

    const resultFor = async (durableId: string, value: number, name: string) => {
      const prepared = prepareSerializedObject(value, 0, durableId);
      const outputPath = path.join(live.directory, name);
      await writeFile(outputPath, prepared.bytes);
      return {
        functionCallId: durableId,
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        valueOutput: prepared.object,
        valueBlob: {
          id: name,
          chunks: [{ uri: pathToFileURL(outputPath).href, size: prepared.bytes.byteLength }],
        },
      };
    };
    const firstResult = await resultFor(childIds[0], 2, "causal-first");
    const secondResult = await resultFor(childIds[1], 4, "causal-second");
    const initialWatcherHistory = [
      {
        clock: 3,
        functionCallWatcherCreated: { functionCallId: childIds[0], status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherCreated: { functionCallId: childIds[1], status: { code: 0 } },
      },
      { clock: 5, functionCallWatcherResult: firstResult },
    ];
    history.push(...initialWatcherHistory);
    await liveEvents.respond(initialWatcherHistory, 5);

    const markerCreation = await withDeadline(
      live.runner.getExecutionBatch(),
      "the causal replay marker creation",
    );
    const markerId = String(markerCreation[0].createFunctionCall.updates.rootFunctionCallId);
    live.runner.advanceExecutionBatch();
    const markerCreated = {
      clock: 6,
      functionCallCreated: { functionCallId: markerId, status: { code: 0 } },
    };
    history.push(markerCreated);
    await liveEvents.respond([markerCreated], 6);

    const markerWatcher = await withDeadline(
      live.runner.getExecutionBatch(),
      "the causal replay marker watcher",
    );
    expect(markerWatcher[0].createFunctionCallWatcher.functionCallId).toBe(markerId);
    live.runner.advanceExecutionBatch();
    const markerResult = await resultFor(markerId, 6, "causal-marker");
    const finalHistory = [
      {
        clock: 7,
        functionCallWatcherCreated: { functionCallId: markerId, status: { code: 0 } },
      },
      { clock: 8, functionCallWatcherResult: markerResult },
      { clock: 9, functionCallWatcherResult: secondResult },
    ];
    history.push(...finalHistory);
    await liveEvents.respond(finalHistory, 9);

    const liveTerminal = await withDeadline(
      live.runner.getExecutionBatch(),
      "the causal live terminal batch",
    );
    expect(await readFinishValue(liveTerminal[0].finishAllocation)).toEqual({
      done: 1,
      marker: 6,
      results: [2, 4],
    });

    const replay = await createRunner(application.definition, { replayMode: "REPLAY_MODE_STRICT" });
    attachBlobResponder(replay.runner, replay.directory);
    const replayEvents = attachEventDriver(replay.runner);
    replay.runner.start();
    await replayEvents.respond(history, 9);
    const replayTerminal = await withDeadline(
      replay.runner.getExecutionBatch(),
      "the causal replay terminal batch",
    );
    expect(await readFinishValue(replayTerminal[0].finishAllocation)).toEqual({
      done: 1,
      marker: 6,
      results: [2, 4],
    });
  });

  it("treats one failed watcher result as terminal after the server exhausts retries", async () => {
    clearRegistryForTest();
    const child = registerFunction(async () => "unreachable", {
      name: "protocol_exhausted_child",
      parameters: [] as const,
      returns: schema.string(),
      retries: retries({ maxRetries: 3 }),
    });
    const application = registerApplication(async () => {
      try {
        await child();
        return "unexpected success";
      } catch (error) {
        return `caught:${error instanceof Error ? error.message : String(error)}`;
      }
    }, {
      name: "protocol_exhausted_parent",
      parameters: [] as const,
      returns: schema.string(),
    });
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    const createBatch = await withDeadline(runner.getExecutionBatch(), "the child creation batch");
    const durableId = String(createBatch[0].createFunctionCall.updates.rootFunctionCallId);
    runner.advanceExecutionBatch();
    await events.respond([
      { clock: 1, functionCallCreated: { functionCallId: durableId, status: { code: 0 } } },
      { clock: 2, functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } } },
      { clock: 3, functionCallWatcherResult: {
        functionCallId: durableId,
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      } },
    ], 3);

    const watcherBatch = await withDeadline(runner.getExecutionBatch(), "the child watcher batch");
    expect(watcherBatch[0].createFunctionCallWatcher.functionCallId).toBe(durableId);
    runner.advanceExecutionBatch();
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the terminal failure-handling batch");
    expect(await readFinishValue(terminalBatch[0].finishAllocation)).toBe("caught:Function call failed");
    expect(events.readCount).toBe(1);
  });

  it("keeps an invalid watcher outcome terminal when application code catches it", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_invalid_watcher_outcome_child",
      async () => 1,
    );
    const application = registerApplication(
      "protocol_invalid_watcher_outcome_parent",
      async () => {
        try {
          return await child();
        } catch {
          return new Promise<number>(() => undefined);
        }
      },
    );
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    const createBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the invalid-outcome child creation batch",
    );
    const durableId = String(createBatch[0].createFunctionCall.updates.rootFunctionCallId);
    runner.advanceExecutionBatch();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallWatcherResult: {
          functionCallId: durableId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_UNKNOWN",
        },
      },
    ], 3);

    await withDeadline(
      runner.getExecutionBatch(),
      "the invalid-outcome child watcher batch",
    );
    runner.advanceExecutionBatch();
    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the invalid-outcome terminal batch",
    );
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      },
    }]);
  });

  it("terminates when an event-log protocol failure is caught before a permanent wait", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_event_log_failure_child",
      async () => 1,
    );
    const application = registerApplication(
      "protocol_event_log_failure_parent",
      async () => {
        try {
          await child();
        } catch {
          await new Promise(() => undefined);
        }
        return 1;
      },
    );
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    await withDeadline(
      runner.getExecutionBatch(),
      "the event-log-failure child creation batch",
    );
    runner.advanceExecutionBatch();
    await events.respond([], 0, true);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the event-log-failure terminal batch",
    );
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      },
    }]);
  });

  it.each([
    {
      name: "successful watcher with no value payload",
      result: {
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
      },
    },
    {
      name: "request-error watcher with no BLOB payload",
      result: {
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        requestErrorOutput: {
          offset: 0,
          manifest: {
            encoding: "SERIALIZED_OBJECT_ENCODING_UTF8_TEXT",
            encodingVersion: 0,
            size: 0,
            metadataSize: 0,
            sha256Hash: createHash("sha256").update(new Uint8Array()).digest("hex"),
          },
        },
      },
    },
  ])("keeps a malformed $name terminal when application code catches it", async ({ result }) => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_malformed_watcher_payload_child",
      async () => 1,
    );
    const application = registerApplication(
      "protocol_malformed_watcher_payload_parent",
      async () => {
        try {
          return await child();
        } catch {
          return -1;
        }
      },
    );
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    const createBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the malformed-payload child creation batch",
    );
    const durableId = String(createBatch[0].createFunctionCall.updates.rootFunctionCallId);
    runner.advanceExecutionBatch();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallWatcherResult: {
          functionCallId: durableId,
          ...result,
        },
      },
    ], 3);

    await withDeadline(
      runner.getExecutionBatch(),
      "the malformed-payload child watcher batch",
    );
    runner.advanceExecutionBatch();
    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the malformed-payload terminal batch",
    );
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      },
    }]);
  });

  it("keeps a child-argument BLOB transport failure terminal when application code catches it", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_child_blob_failure_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_child_blob_failure_parent",
      async () => {
        try {
          return await child(1);
        } catch {
          return -1;
        }
      },
    );
    const { directory, runner } = await createRunner(application.definition);
    const seen = new Set<string>();
    let requestCount = 0;
    runner.watchState({
      write(state) {
        for (const request of state.outputBlobRequests ?? []) {
          const id = String(request.id);
          if (seen.has(id)) continue;
          seen.add(id);
          requestCount += 1;
          queueMicrotask(() => runner.deliverUpdate({
            outputBlob: requestCount === 1
              ? { status: { code: 13, message: "argument BLOB unavailable" } }
              : {
                  status: { code: 0 },
                  blob: {
                    id,
                    chunks: [{
                      uri: pathToFileURL(path.join(directory, `output-${id}`)).href,
                      size: request.size,
                    }],
                  },
                },
          }));
        }
        return true;
      },
      end() {},
      on() {},
    });

    runner.start();
    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the child-argument BLOB failure terminal batch",
    );
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
      },
    }]);
    expect(requestCount).toBe(1);
  });

  it("executes reduce as a function-call chain and strictly replays it", async () => {
    clearRegistryForTest();
    const add = registerFunction(async (accumulator: number, value: number) => accumulator + value, {
      name: "protocol_reduce_add",
      parameters: [
        schema.parameter("accumulator", schema.number()),
        schema.parameter("value", schema.number()),
      ] as const,
      returns: schema.number(),
    });
    const application = registerApplication(async () => add.reduce([1, 2, 3], 10), {
      name: "protocol_reduce_parent",
      parameters: [] as const,
      returns: schema.number(),
    });
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);
    runner.start();
    const createBatch = await withDeadline(runner.getExecutionBatch(), "reduce creation batch");
    const creation = createBatch[0].createFunctionCall;
    const calls = creation.updates.updates.map((update: Message) => update.functionCall);
    const durableId = String(creation.updates.rootFunctionCallId);
    expect(calls).toHaveLength(3);
    expect(calls[2].id).toBe(durableId);
    expect(calls.every((call: Message) => call.target.functionName === "protocol_reduce_add")).toBe(true);
    expect(calls[1].args[0].functionCallId).toBe(calls[0].id);
    expect(calls[2].args[0].functionCallId).toBe(calls[1].id);
    const inlineValues = [calls[0].args[0], ...calls.map((call: Message) => call.args[1])];
    expect(await Promise.all(inlineValues.map(async (arg: Message) =>
      deserializeValueFromProtocol(await downloadSerializedObject(arg.value, creation.argsBlob))
    ))).toEqual([10, 1, 2, 3]);
    expect(JSON.parse(Buffer.from(calls[0].callMetadata).toString("utf8"))).toMatchObject({
      format: "tensorlake.typescript.function-call.v1",
      functionName: "protocol_reduce_add",
      argumentCount: 2,
      operation: "reduce",
      reduceRootId: durableId,
      reduceStep: 0,
      reduceStepCount: 3,
    });
    expect(creation.updates.updates.every((update: Message) => update.reduce == null)).toBe(true);
    runner.advanceExecutionBatch();

    const prepared = prepareSerializedObject(16, 0, durableId);
    const resultPath = path.join(directory, "reduce-result");
    await writeFile(resultPath, prepared.bytes);
    const history = [
      { clock: 1, functionCallCreated: { functionCallId: durableId, status: { code: 0 } } },
      { clock: 2, functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } } },
      { clock: 3, functionCallWatcherResult: {
        functionCallId: durableId,
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        valueOutput: prepared.object,
        valueBlob: {
          id: "reduce-result",
          chunks: [{ uri: pathToFileURL(resultPath).href, size: prepared.bytes.byteLength }],
        },
      } },
    ];
    await events.respond(history, 3);

    const watcherBatch = await withDeadline(runner.getExecutionBatch(), "reduce watcher batch");
    expect(watcherBatch).toEqual([{ createFunctionCallWatcher: { functionCallId: durableId } }]);
    runner.advanceExecutionBatch();
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the reduce terminal batch");
    expect(await readFinishValue(terminalBatch[0].finishAllocation)).toBe(16);
    expect(events.readCount).toBe(1);

    const replay = await createRunner(application.definition, { replayMode: "REPLAY_MODE_STRICT" });
    attachBlobResponder(replay.runner, replay.directory);
    const replayEvents = attachEventDriver(replay.runner);
    replay.runner.start();
    await replayEvents.respond(history, 3);
    const replayTerminalBatch = await withDeadline(
      replay.runner.getExecutionBatch(),
      "the replayed reduce terminal batch",
    );
    expect(replayTerminalBatch).toHaveLength(1);
    expect(replayTerminalBatch[0].finishAllocation).toBeDefined();
    expect(await readFinishValue(replayTerminalBatch[0].finishAllocation)).toBe(16);
  });

  it("fails a multi-plan reduce when one creation fails without waiting for later acknowledgements", async () => {
    clearRegistryForTest();
    const add = registerFunction(
      "protocol_failed_reduce_add",
      async (accumulator: number, value: number) => accumulator + value,
    );
    const application = registerApplication(
      "protocol_failed_reduce_parent",
      async () => add.reduce(
        Array.from({ length: 513 }, (_, index) => index + 1),
        0,
      ),
    );
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    const creationBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the multi-plan reduce creation batch",
    );
    expect(creationBatch).toHaveLength(2);
    const firstRootId = String(
      creationBatch[0].createFunctionCall.updates.rootFunctionCallId,
    );
    runner.advanceExecutionBatch();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: {
          functionCallId: firstRootId,
          status: { code: 13, message: "reduce creation failed" },
        },
      },
    ], 1);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the failed multi-plan reduce terminal batch",
    );
    expect(terminalBatch).toEqual([{
      finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
      },
    }]);
  });

  it("reports strict replay divergence as a replay-history mismatch", async () => {
    clearRegistryForTest();
    const child = registerFunction(async () => 1, {
      name: "protocol_replay_child",
      parameters: [] as const,
      returns: schema.number(),
    });
    const application = registerApplication(async () => {
      try {
        return await child();
      } catch {
        // Replay divergence is an executor invariant and must remain terminal
        // even when application code catches the error thrown by the call.
        return -1;
      }
    }, {
      name: "protocol_replay_parent",
      parameters: [] as const,
      returns: schema.number(),
    });
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);

    runner.start();
    await events.respond([{ clock: 1, functionCallCreated: {
      functionCallId: "a-different-durable-id",
      status: { code: 0 },
    } }], 1);
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the replay mismatch terminal batch");
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("rejects duplicate watcher results in strict replay history", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_duplicate_result_child",
      async () => 1,
    );
    const application = registerApplication(
      "protocol_duplicate_result_parent",
      async () => {
        try {
          return await child();
        } catch {
          return -1;
        }
      },
    );
    const { directory, runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const durableId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const failedResult = {
      functionCallId: durableId,
      watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
    };

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } },
      },
      { clock: 3, functionCallWatcherResult: failedResult },
      { clock: 4, functionCallWatcherResult: failedResult },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the duplicate watcher-result replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("reports a blocked cross-kind replay boundary as a mismatch", async () => {
    clearRegistryForTest();
    const child = registerFunction("protocol_cross_kind_child", async (value: number) => value);
    const application = registerApplication("protocol_cross_kind_parent", async () => {
      const first = child.future(1).run();
      await first;
      return child.tailCall(2);
    });
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
    ], 2);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the cross-kind replay mismatch terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("reports a result blocked behind an unreachable replay call as a mismatch", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_blocked_result_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_blocked_result_parent",
      async () => {
        const first = child.future(1).run();
        await first;
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the blocked replay result mismatch terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("detects blocked replay when durable results use explicit promise chaining", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_blocked_then_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_blocked_then_parent",
      async () => {
        const first = child.future(1).run();
        await first.then(
          () => undefined,
          () => undefined,
        );
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the explicitly chained blocked replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("detects blocked replay when a resolved durable wrapper is awaited directly", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_blocked_resolved_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_blocked_resolved_parent",
      async () => {
        const first = child.future(1).run();
        await Promise.resolve(first);
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the directly awaited resolved durable replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it.each([
    [
      "direct_race",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => Promise.race([durable, external]),
    ],
    [
      "species_race",
      async (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        class SpeciesPromise<T> extends Promise<T> {
          static speciesAccesses = 0;

          static get [Symbol.species](): PromiseConstructor {
            SpeciesPromise.speciesAccesses += 1;
            return Promise;
          }
        }
        const baseline = SpeciesPromise.race([
          Promise.resolve(1),
          Promise.resolve(2),
        ]);
        const nativeConstructionAccesses = SpeciesPromise.speciesAccesses;
        await baseline;
        SpeciesPromise.speciesAccesses = 0;
        const result = SpeciesPromise.race([durable, external]);
        if (SpeciesPromise.speciesAccesses !== nativeConstructionAccesses) {
          throw new Error(
            "Promise.race executor bookkeeping performed an observable Symbol.species lookup",
          );
        }
        return result;
      },
    ],
    [
      "resolved_race",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => Promise.race([Promise.resolve(durable), external]),
    ],
    [
      "pre_resolved_race",
      async (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        const resolved = Promise.resolve(durable);
        await Promise.resolve();
        return Promise.race([resolved, external]);
      },
    ],
    [
      "reused_resolved_race",
      async (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        const resolved = Promise.resolve(durable);
        await Promise.race([resolved, external]);
        return Promise.race([resolved, Promise.resolve(0)]);
      },
    ],
    [
      "chained_race",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => Promise.race([durable.then((value) => value), external]),
    ],
    [
      "captured_race",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => promiseRaceCapturedBeforeAllocation([durable, external]),
    ],
    [
      "direct_any",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => Promise.any([durable, external]),
    ],
    [
      "species_any",
      async (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        class SpeciesPromise<T> extends Promise<T> {
          static speciesAccesses = 0;

          static get [Symbol.species](): PromiseConstructor {
            SpeciesPromise.speciesAccesses += 1;
            return Promise;
          }
        }
        const baseline = SpeciesPromise.any([
          Promise.resolve(1),
          Promise.resolve(2),
        ]);
        const nativeConstructionAccesses = SpeciesPromise.speciesAccesses;
        await baseline;
        SpeciesPromise.speciesAccesses = 0;
        const result = SpeciesPromise.any([durable, external]);
        if (SpeciesPromise.speciesAccesses !== nativeConstructionAccesses) {
          throw new Error(
            "Promise.any executor bookkeeping performed an observable Symbol.species lookup",
          );
        }
        return result;
      },
    ],
    [
      "reused_resolved_any",
      async (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        const resolved = Promise.resolve(durable);
        await Promise.any([resolved, external]);
        return Promise.any([resolved, Promise.resolve(0)]);
      },
    ],
    [
      "subclass_any",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        class CustomPromise<T> extends Promise<T> {}
        const result = CustomPromise.any([durable, external]);
        if (!(result instanceof CustomPromise)) {
          throw new Error("Promise.any did not preserve the subclass result");
        }
        return result;
      },
    ],
    [
      "custom_resolve_subclass_any",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        class CustomResolvePromise<T> extends Promise<T> {
          static override resolve<T>(
            value: T | PromiseLike<T>,
          ): CustomResolvePromise<Awaited<T>> {
            customResolveAnyCalls += 1;
            return super.resolve(value) as CustomResolvePromise<Awaited<T>>;
          }
        }
        const result = CustomResolvePromise.any([durable, external]);
        if (!(result instanceof CustomResolvePromise)) {
          throw new Error("Promise.any did not preserve the custom-resolve subclass result");
        }
        return result;
      },
    ],
    [
      "custom_thenable_resolve_any",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        class CustomThenableResolvePromise<T> extends Promise<T> {
          static override resolve<T>(
            value: T | PromiseLike<T>,
          ): CustomThenableResolvePromise<Awaited<T>> {
            customResolveAnyCalls += 1;
            if (value === external) {
              return {
                then: (onfulfilled, onrejected) => {
                  customThenableAnyCalls += 1;
                  return external.then(onfulfilled, onrejected);
                },
              } as unknown as CustomThenableResolvePromise<Awaited<T>>;
            }
            return super.resolve(value) as CustomThenableResolvePromise<Awaited<T>>;
          }
        }
        return CustomThenableResolvePromise.any([durable, external]);
      },
    ],
    [
      "custom_thenable_any",
      (
        durable: PromiseLike<number>,
        external: Promise<number>,
      ) => {
        const externalThenable: PromiseLike<number> = {
          then: (onfulfilled, onrejected) => {
            customThenableAnyCalls += 1;
            return external.then(onfulfilled, onrejected);
          },
        };
        return Promise.any([durable, externalThenable]);
      },
    ],
  ] as const)(
    "allows an external contender to advance a mixed durable %s",
    async (variant, waitForContender) => {
      clearRegistryForTest();
      customResolveAnyCalls = 0;
      customThenableAnyCalls = 0;
      const preinitializedTicks = new EventEmitter();
      const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
      tickInterval.unref();
      temporaryIntervals.push(tickInterval);
      const child = registerFunction(
        `protocol_mixed_${variant}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_mixed_${variant}_parent`,
        async () => {
          const first = child.future(1).run();
          await waitForContender(
            first,
            new Promise<number>((resolve) =>
              preinitializedTicks.once("tick", () => resolve(0))
            ),
          );
          // The losing durable contender is no longer an application-level
          // await. A later callback from the same preinitialized resource must
          // therefore be allowed to run before the next durable operation.
          await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
          return child.tailCall(2);
        },
      );
      const { runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          },
        },
      ], 4);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        `the mixed ${variant} tail-call terminal batch`,
      );
      expect(terminalBatch).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        tailCallDurableId: secondId,
      } }]);
      if (variant === "custom_thenable_any" || variant === "custom_thenable_resolve_any") {
        expect(customThenableAnyCalls).toBe(1);
      }
      if (
        variant === "custom_resolve_subclass_any"
        || variant === "custom_thenable_resolve_any"
      ) {
        expect(customResolveAnyCalls).toBe(2);
      }
    },
  );

  it.each([
    "all",
    "allSettled",
    "race",
    "any",
  ] as const)(
    "does not treat constructing an unawaited Promise.%s as a blocked durable await",
    async (variant) => {
      clearRegistryForTest();
      const preinitializedTicks = new EventEmitter();
      const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
      tickInterval.unref();
      temporaryIntervals.push(tickInterval);
      const child = registerFunction(
        `protocol_unawaited_${variant}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_unawaited_${variant}_parent`,
        async () => {
          const first = child.future(1).run();
          const aggregate: Promise<unknown> = variant === "all"
            ? Promise.all([first])
            : variant === "allSettled"
              ? Promise.allSettled([first])
            : variant === "race"
              ? Promise.race([first])
              : Promise.any([first]);
          // Aggregate construction installs internal durable reactions, but
          // user code does not consume the aggregate until after this separate
          // asynchronous continuation and durable call have completed.
          await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
          try {
            await child(2);
          } catch {
            // The marker failure is expected in this synthetic replay history.
          }
          const value = await aggregate;
          return variant === "all"
            ? (value as number[])[0]
            : variant === "allSettled"
              ? (value as PromiseFulfilledResult<number>[])[0].value
              : value;
        },
      );
      const { directory, runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      attachBlobResponder(runner, directory);
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);
      const prepared = prepareSerializedObject(7, 0, firstId);
      const resultPath = path.join(directory, `unawaited-${variant}-result`);
      await writeFile(resultPath, prepared.bytes);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 5,
          functionCallWatcherResult: {
            functionCallId: secondId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          },
        },
        {
          clock: 6,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
            valueOutput: prepared.object,
            valueBlob: {
              id: `unawaited-${variant}-result`,
              chunks: [{
                uri: pathToFileURL(resultPath).href,
                size: prepared.bytes.byteLength,
              }],
            },
          },
        },
      ], 6);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        `the delayed Promise.${variant} consumption terminal batch`,
      );
      expect(await readFinishValue(terminalBatch[0].finishAllocation)).toBe(7);
    },
  );

  it("keeps a Promise.any producer alive while a fulfilled value is being adopted", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    let customThenCalls = 0;
    const child = registerFunction(
      "protocol_adopted_any_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_adopted_any_parent",
      async () => {
        const first = child.future(1).run();
        const marker = {};
        const adopted = new Promise<number>((resolve) =>
          preinitializedTicks.once("tick", () => resolve(0))
        );
        class AdoptingResolvePromise<T> extends Promise<T> {
          static override resolve<T>(
            value: T | PromiseLike<T>,
          ): AdoptingResolvePromise<Awaited<T>> {
            if (value === marker) {
              return {
                then: (
                  onfulfilled:
                    | ((value: Promise<number>) => unknown)
                    | null
                    | undefined,
                ) => {
                  customThenCalls += 1;
                  onfulfilled?.(adopted);
                },
              } as unknown as AdoptingResolvePromise<Awaited<T>>;
            }
            return super.resolve(value) as AdoptingResolvePromise<Awaited<T>>;
          }
        }
        await AdoptingResolvePromise.any([first, marker]);
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the adopted Promise.any tail-call terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
      tailCallDurableId: secondId,
    } }]);
    expect(customThenCalls).toBe(1);
  });

  it.each([
    "native",
    "species_subclass",
    "custom_resolve_subclass",
    "custom_thenable_resolve",
  ] as const)(
    "allows a handled ordinary Promise.all rejection to advance replay for %s",
    async (variant) => {
      clearRegistryForTest();
      const preinitializedTicks = new EventEmitter();
      const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
      tickInterval.unref();
      temporaryIntervals.push(tickInterval);
      const child = registerFunction(
        `protocol_mixed_all_${variant}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_mixed_all_${variant}_parent`,
        async () => {
          const first = child.future(1).run();
          const ordinaryError = new Error("ordinary Promise.all contender rejected");
          const ordinary = Promise.reject<number>(ordinaryError);
          try {
            if (variant === "native") {
              await Promise.all([first, ordinary]);
            } else if (variant === "species_subclass") {
              class SpeciesPromise<T> extends Promise<T> {
                static speciesAccesses = 0;

                static get [Symbol.species](): PromiseConstructor {
                  SpeciesPromise.speciesAccesses += 1;
                  return Promise;
                }
              }
              const baseline = SpeciesPromise.all([
                Promise.resolve(1),
                Promise.resolve(2),
              ]);
              const nativeConstructionAccesses = SpeciesPromise.speciesAccesses;
              await baseline;
              SpeciesPromise.speciesAccesses = 0;
              const result = SpeciesPromise.all([first, ordinary]);
              if (SpeciesPromise.speciesAccesses !== nativeConstructionAccesses) {
                throw new Error(
                  "Promise.all executor bookkeeping performed an observable Symbol.species lookup",
                );
              }
              await result;
            } else if (variant === "custom_resolve_subclass") {
              class CustomResolvePromise<T> extends Promise<T> {
                static resolveCalls = 0;

                static override resolve<T>(
                  value: T | PromiseLike<T>,
                ): CustomResolvePromise<Awaited<T>> {
                  CustomResolvePromise.resolveCalls += 1;
                  return super.resolve(value) as CustomResolvePromise<Awaited<T>>;
                }
              }
              const result = CustomResolvePromise.all([first, ordinary]);
              if (!(result instanceof CustomResolvePromise)) {
                throw new Error("Promise.all did not preserve the subclass result");
              }
              if (CustomResolvePromise.resolveCalls !== 2) {
                throw new Error("Promise.all did not invoke custom resolve exactly once per input");
              }
              await result;
            } else {
              let ordinaryThenCalls = 0;
              class CustomThenableResolvePromise<T> extends Promise<T> {
                static override resolve<T>(
                  value: T | PromiseLike<T>,
                ): CustomThenableResolvePromise<Awaited<T>> {
                  if (value === ordinary) {
                    return {
                      then: (onfulfilled, onrejected) => {
                        ordinaryThenCalls += 1;
                        return ordinary.then(onfulfilled, onrejected);
                      },
                    } as unknown as CustomThenableResolvePromise<Awaited<T>>;
                  }
                  return super.resolve(value) as CustomThenableResolvePromise<Awaited<T>>;
                }
              }
              const result = CustomThenableResolvePromise.all([first, ordinary]);
              if (ordinaryThenCalls !== 1) {
                throw new Error("Promise.all invoked a custom resolve thenable more than once");
              }
              await result;
            }
          } catch (error) {
            if (error !== ordinaryError) throw error;
          }
          // The rejected aggregate no longer awaits its durable input. A later
          // callback owned by a module-initialized resource must be allowed to
          // run before the next durable operation.
          await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
          return child.tailCall(2);
        },
      );
      const { runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          },
        },
      ], 4);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        `the mixed Promise.all ${variant} tail-call terminal batch`,
      );
      expect(terminalBatch).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        tailCallDurableId: secondId,
      } }]);
    },
  );

  it.each([
    "race",
    "any",
  ] as const)(
    "detaches losing durable contenders after a durable-only Promise.%s settles",
    async (variant) => {
      clearRegistryForTest();
      const preinitializedTicks = new EventEmitter();
      const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
      tickInterval.unref();
      temporaryIntervals.push(tickInterval);
      const child = registerFunction(
        `protocol_durable_only_${variant}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_durable_only_${variant}_parent`,
        async () => {
          const first = child.future(1).run();
          const second = child.future(2).run();
          if (variant === "race") {
            await Promise.race([first, second]);
          } else {
            await Promise.any([first, second]);
          }
          await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
          return child.tailCall(3);
        },
      );
      const { directory, runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);
      const thirdId = testDurableHash([
        rootId,
        secondId,
        "FunctionCall",
        child.definition.name,
      ]);
      const prepared = prepareSerializedObject(1, 0, firstId);
      const resultPath = path.join(directory, `durable-only-${variant}-result`);
      await writeFile(resultPath, prepared.bytes);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 5,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
            valueOutput: prepared.object,
            valueBlob: {
              id: `durable-only-${variant}-result`,
              chunks: [{
                uri: pathToFileURL(resultPath).href,
                size: prepared.bytes.byteLength,
              }],
            },
          },
        },
        {
          clock: 6,
          functionCallCreated: { functionCallId: thirdId, status: { code: 0 } },
        },
      ], 6);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        `the durable-only Promise.${variant} tail-call terminal batch`,
      );
      expect(terminalBatch).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        tailCallDurableId: thirdId,
      } }]);
    },
  );

  it("detects replay blocking when a detached resolved wrapper is awaited again", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    const child = registerFunction(
      "protocol_reawait_resolved_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_reawait_resolved_parent",
      async () => {
        const first = child.future(1).run();
        const wrapped = Promise.resolve(first);
        await Promise.race([
          wrapped,
          new Promise<number>((resolve) =>
            preinitializedTicks.once("tick", () => resolve(0))
          ),
        ]);
        await wrapped;
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const unreachableId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: unreachableId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the re-awaited resolved durable replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("still detects a later durable replay cycle after an external contender wins", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    const child = registerFunction(
      "protocol_post_external_race_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_post_external_race_parent",
      async () => {
        const first = child.future(1).run();
        await Promise.race([
          first,
          new Promise<number>((resolve) =>
            preinitializedTicks.once("tick", () => resolve(0))
          ),
        ]);
        return child(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);
    const unreachableId = testDurableHash([
      rootId,
      secondId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 5,
        functionCallCreated: { functionCallId: unreachableId, status: { code: 0 } },
      },
      {
        clock: 6,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
      {
        clock: 7,
        functionCallWatcherResult: {
          functionCallId: secondId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 7);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the post-external-race blocked replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("does not detach an independent durable chain when a mixed race settles", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    const child = registerFunction(
      "protocol_independent_race_chain_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_independent_race_chain_parent",
      async () => {
        const first = child.future(1).run();
        const raced = Promise.race([
          first,
          new Promise<number>((resolve) =>
            preinitializedTicks.once("tick", () => resolve(0))
          ),
        ]);
        const independent = first.then((value) => value);
        await raced;
        await independent;
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const unreachableId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: unreachableId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the independent durable chain replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it.each([
    "native",
    "custom_resolve_subclass",
    "custom_thenable_resolve",
    "reused_resolved",
  ] as const)(
    "detects blocked replay after every ordinary Promise.any contender rejects for %s",
    async (variant) => {
      clearRegistryForTest();
      const child = registerFunction(
        `protocol_rejected_any_${variant}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_rejected_any_${variant}_parent`,
        async () => {
          const first = child.future(1).run();
          const durable = variant === "reused_resolved"
            ? Promise.resolve(first)
            : first;
          if (variant === "reused_resolved") {
            await Promise.any([durable, Promise.resolve(0)]);
          }
          const ordinary = Promise.reject(new Error("ordinary contender rejected"));
          const contenders = [durable, ordinary];
          if (variant !== "custom_resolve_subclass") {
            if (variant !== "custom_thenable_resolve") {
              await Promise.any(contenders);
            } else {
              class CustomThenableResolvePromise<T> extends Promise<T> {
                static override resolve<T>(
                  value: T | PromiseLike<T>,
                ): CustomThenableResolvePromise<Awaited<T>> {
                  if (value === ordinary) {
                    return {
                      then: (onfulfilled, onrejected) =>
                        ordinary.then(onfulfilled, onrejected),
                    } as unknown as CustomThenableResolvePromise<Awaited<T>>;
                  }
                  return super.resolve(value) as CustomThenableResolvePromise<Awaited<T>>;
                }
              }
              await CustomThenableResolvePromise.any(contenders);
            }
          } else {
            class CustomResolvePromise<T> extends Promise<T> {
              static override resolve<T>(
                value: T | PromiseLike<T>,
              ): CustomResolvePromise<Awaited<T>> {
                return super.resolve(value) as CustomResolvePromise<Awaited<T>>;
              }
            }
            await CustomResolvePromise.any(contenders);
          }
          return child.tailCall(2);
        },
      );
      const { runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          },
        },
      ], 4);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        "the rejected Promise.any blocked replay terminal batch",
      );
      expect(terminalBatch).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
      } }]);
    },
  );

  it("stops tracking ordinary Promise.any contenders after the aggregate settles", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_settled_any_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_settled_any_parent",
      async () => {
        const first = child.future(1).run();
        await Promise.any([
          first,
          new Promise<number>(() => undefined),
        ]);
        return child(2);
      },
    );
    const { directory, runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);
    const unreachableId = testDurableHash([
      rootId,
      secondId,
      "FunctionCall",
      child.definition.name,
    ]);
    const prepared = prepareSerializedObject(1, 0, firstId);
    const resultPath = path.join(directory, "settled-any-result");
    await writeFile(resultPath, prepared.bytes);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
          valueOutput: prepared.object,
          valueBlob: {
            id: "settled-any-result",
            chunks: [{
              uri: pathToFileURL(resultPath).href,
              size: prepared.bytes.byteLength,
            }],
          },
        },
      },
      {
        clock: 4,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 5,
        functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 6,
        functionCallCreated: { functionCallId: unreachableId, status: { code: 0 } },
      },
      {
        clock: 7,
        functionCallWatcherResult: {
          functionCallId: secondId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 7);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the post-Promise.any blocked replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("still detects blocked replay in a durable-only Promise.race", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_durable_race_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_durable_race_parent",
      async () => {
        const first = child.future(1).run();
        await Promise.race([first.then((value) => value)]);
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the durable-only Promise.race blocked replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("classifies the contender returned by a Promise subclass resolve", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_normalized_race_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_normalized_race_parent",
      async () => {
        const first = child.future(1).run();
        const marker = {};
        class RedirectingPromise<T> extends Promise<T> {
          static override resolve<T>(
            value: T | PromiseLike<T>,
          ): RedirectingPromise<Awaited<T>> {
            if (value === marker) {
              return first as unknown as RedirectingPromise<Awaited<T>>;
            }
            return super.resolve(value) as RedirectingPromise<Awaited<T>>;
          }
        }
        const aggregate = RedirectingPromise.race([marker]);
        if (!(aggregate instanceof RedirectingPromise)) {
          throw new Error("Promise.race did not preserve the subclass result");
        }
        await aggregate;
        return child.tailCall(2);
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 4);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the constructor-normalized Promise.race replay terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it.each([
    "all_completed",
    "first_completed",
    "first_failure",
  ] as const)(
    "reports a result blocked behind unreachable replay history during Future.wait(%s)",
    async (returnWhen) => {
      clearRegistryForTest();
      const child = registerFunction(
        `protocol_blocked_wait_${returnWhen}_child`,
        async (value: number) => value,
      );
      const application = registerApplication(
        `protocol_blocked_wait_${returnWhen}_parent`,
        async () => {
          const first = child.future(1).run();
          await Future.wait([first], { returnWhen });
          return child.tailCall(2);
        },
      );
      const { runner } = await createRunner(application.definition, {
        replayMode: "REPLAY_MODE_STRICT",
      });
      const events = attachEventDriver(runner);
      const rootId = `call-${application.definition.name}`;
      const firstId = testDurableHash([
        rootId,
        rootId,
        "FunctionCall",
        child.definition.name,
      ]);
      const secondId = testDurableHash([
        rootId,
        firstId,
        "FunctionCall",
        child.definition.name,
      ]);

      runner.start();
      await events.respond([
        {
          clock: 1,
          functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 2,
          functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
        },
        {
          clock: 3,
          functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
        },
        {
          clock: 4,
          functionCallWatcherResult: {
            functionCallId: firstId,
            watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
            outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
          },
        },
      ], 4);

      const terminalBatch = await withDeadline(
        runner.getExecutionBatch(),
        `the Future.wait(${returnWhen}) blocked replay mismatch terminal batch`,
      );
      expect(terminalBatch).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
      } }]);
    },
  );

  it("does not treat an unawaited Future.wait as a blocked application continuation", async () => {
    clearRegistryForTest();
    const preinitializedTicks = new EventEmitter();
    const tickInterval = setInterval(() => preinitializedTicks.emit("tick"), 20);
    tickInterval.unref();
    temporaryIntervals.push(tickInterval);
    const child = registerFunction(
      "protocol_unawaited_wait_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_unawaited_wait_parent",
      async () => {
        const first = child.future(1).run();
        const waiting = Future.wait([first], { returnWhen: "first_completed" });
        await new Promise<void>((resolve) => preinitializedTicks.once("tick", resolve));
        try {
          await child(2);
        } catch {
          // The marker failure is expected in this synthetic replay history.
        }
        const waited = await waiting;
        return waited.done.length;
      },
    );
    const { directory, runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 5,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
      {
        clock: 6,
        functionCallWatcherResult: {
          functionCallId: secondId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 6);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the unawaited Future.wait replay terminal batch",
    );
    expect(await readFinishValue(terminalBatch[0].finishAllocation)).toBe(1);
  });

  it("does not strand a replay result when another result continuation cannot advance history", async () => {
    clearRegistryForTest();
    const child = registerFunction(
      "protocol_concurrent_blocked_result_child",
      async (value: number) => value,
    );
    const application = registerApplication(
      "protocol_concurrent_blocked_result_parent",
      async () => {
        const first = child.future(1).run();
        const second = child.future(2).run();
        try {
          await first;
        } catch {
          // The first result is handled but does not create the stale third call
          // recorded in replay history.
        }
        return second;
      },
    );
    const { runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    const events = attachEventDriver(runner);
    const rootId = `call-${application.definition.name}`;
    const firstId = testDurableHash([
      rootId,
      rootId,
      "FunctionCall",
      child.definition.name,
    ]);
    const secondId = testDurableHash([
      rootId,
      firstId,
      "FunctionCall",
      child.definition.name,
    ]);
    const unreachableId = testDurableHash([
      rootId,
      secondId,
      "FunctionCall",
      child.definition.name,
    ]);

    runner.start();
    await events.respond([
      {
        clock: 1,
        functionCallCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 2,
        functionCallCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 3,
        functionCallWatcherCreated: { functionCallId: firstId, status: { code: 0 } },
      },
      {
        clock: 4,
        functionCallWatcherCreated: { functionCallId: secondId, status: { code: 0 } },
      },
      {
        clock: 5,
        functionCallWatcherResult: {
          functionCallId: firstId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
      {
        clock: 6,
        functionCallCreated: { functionCallId: unreachableId, status: { code: 0 } },
      },
      {
        clock: 7,
        functionCallWatcherResult: {
          functionCallId: secondId,
          watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
          outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        },
      },
    ], 7);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the concurrent blocked replay result mismatch terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
  });

  it("fails malformed non-advancing event pages instead of polling forever", async () => {
    clearRegistryForTest();
    const child = registerFunction(async () => 1, {
      name: "protocol_stalled_child",
      parameters: [] as const,
      returns: schema.number(),
    });
    const application = registerApplication(async () => {
      try {
        return await child();
      } catch {
        // Protocol corruption is not a user function error and cannot be
        // converted into a successful allocation by catching it here.
        return -1;
      }
    }, {
      name: "protocol_stalled_parent",
      parameters: [] as const,
      returns: schema.number(),
    });
    const { runner } = await createRunner(application.definition);
    const events = attachEventDriver(runner);

    runner.start();
    await withDeadline(runner.getExecutionBatch(), "the child creation batch");
    runner.advanceExecutionBatch();
    await events.respond([], 0, true);
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the protocol failure terminal batch");
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
    } }]);
    expect(events.readCount).toBe(1);
  });

  it("fails an event-log entry without a protocol payload instead of polling forever", async () => {
    clearRegistryForTest();
    const child = registerFunction("protocol_empty_event_child", async () => 1);
    const application = registerApplication(
      "protocol_empty_event_parent",
      async () => {
        try {
          await child();
        } catch {
          await new Promise(() => undefined);
        }
        return 1;
      },
    );
    const { runner } = await createRunner(application.definition);
    const events = attachEventDriver(runner);

    runner.start();
    await withDeadline(runner.getExecutionBatch(), "the empty-event child creation batch");
    runner.advanceExecutionBatch();
    await events.respond([{ clock: 1 }], 1);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the empty-event protocol failure terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
    } }]);
    expect(events.readCount).toBe(1);
  });

  it("fails a state-operation result without its correlation ID instead of hanging", async () => {
    clearRegistryForTest();
    const application = registerApplication(
      "protocol_missing_state_operation_id",
      async () => {
        try {
          await RequestContext.get().state.get("missing");
        } catch {
          await new Promise(() => undefined);
        }
        return 1;
      },
    );
    const { runner } = await createRunner(application.definition);
    let resolveOperation!: () => void;
    const operationPublished = new Promise<void>((resolve) => {
      resolveOperation = resolve;
    });
    runner.watchState({
      write(state) {
        if ((state.requestStateOperations?.length ?? 0) > 0) resolveOperation();
        return true;
      },
      end() {},
      on() {},
    });

    runner.start();
    await withDeadline(operationPublished, "a request-state operation");
    expect(() => runner.deliverUpdate({
      requestStateOperationResult: {
        status: { code: 5, message: "missing" },
      },
    })).toThrow("request state operation result without an operation ID");

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the missing-state-operation-ID terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
    } }]);
  });

  it.each([
    { name: "non-finite", lastClock: Number.NaN },
    { name: "fractional", lastClock: 0.5 },
  ])("fails a $name live event-log clock instead of polling with an invalid cursor", async ({
    lastClock,
  }) => {
    clearRegistryForTest();
    const child = registerFunction("protocol_invalid_clock_child", async () => 1);
    const application = registerApplication(
      "protocol_invalid_clock_parent",
      async () => {
        try {
          await child();
        } catch {
          await new Promise(() => undefined);
        }
        return 1;
      },
    );
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    await withDeadline(runner.getExecutionBatch(), "the invalid-clock child creation batch");
    runner.advanceExecutionBatch();
    await events.respond([], lastClock);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the invalid-clock terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
    } }]);
    expect(events.readCount).toBe(1);
  });

  it("classifies an invalid strict-replay clock as a replay-history mismatch", async () => {
    clearRegistryForTest();
    const application = registerApplication(
      "protocol_invalid_replay_clock",
      async () => 1,
    );
    const { directory, runner } = await createRunner(application.definition, {
      replayMode: "REPLAY_MODE_STRICT",
    });
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    await events.respond([], Number.NaN);

    const terminalBatch = await withDeadline(
      runner.getExecutionBatch(),
      "the invalid replay-clock terminal batch",
    );
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REPLAY_EVENT_HISTORY_MISMATCH",
    } }]);
    expect(events.readCount).toBe(1);
  });

  it("finishes a tail call without creating a result watcher", async () => {
    clearRegistryForTest();
    const child = registerFunction(async (value: number) => value + 1, {
      name: "protocol_tail_child",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
    });
    const application = registerApplication(async () => child.tailCall(41), {
      name: "protocol_tail_parent",
      parameters: [] as const,
      returns: schema.number(),
    });
    const { directory, runner } = await createRunner(application.definition);
    attachBlobResponder(runner, directory);
    const events = attachEventDriver(runner);

    runner.start();
    runner.start();
    const createBatch = await withDeadline(runner.getExecutionBatch(), "the tail-call creation batch");
    const durableId = String(createBatch[0].createFunctionCall.updates.rootFunctionCallId);
    runner.advanceExecutionBatch();
    await events.respond([{ clock: 1, functionCallCreated: {
      functionCallId: durableId,
      status: { code: 0 },
    } }], 1);

    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the tail-call terminal batch");
    expect(terminalBatch).toEqual([{ finishAllocation: {
      outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
      tailCallDurableId: durableId,
    } }]);
    runner.advanceExecutionBatch();
    expect(await runner.getExecutionBatch()).toEqual([]);
    expect(events.readCount).toBe(1);
  });

  it("classifies a cross-bundle branded RequestError at the allocation boundary", async () => {
    clearRegistryForTest();
    const application = registerApplication(async () => {
      throw Object.assign(new Error("foreign request error"), {
        [Symbol.for("tensorlake.applications.request-error.v1")]: true,
      });
    }, {
      name: "protocol_foreign_request_error",
      parameters: [] as const,
      returns: schema.null(),
    });
    const { runner } = await createRunner(application.definition, { requestErrorCapacity: 1_024 });

    runner.start();
    const terminalBatch = await withDeadline(runner.getExecutionBatch(), "the request-error terminal batch");
    const finish = terminalBatch[0].finishAllocation;
    expect(finish).toMatchObject({
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_REQUEST_ERROR",
    });
    const requestError = await downloadSerializedObject(
      finish.requestErrorOutput,
      finish.uploadedRequestErrorBlob,
    );
    expect(new TextDecoder().decode(requestError.data)).toBe("foreign request error");
  });
});
