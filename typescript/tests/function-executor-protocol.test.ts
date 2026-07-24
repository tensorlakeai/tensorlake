import { createHash } from "node:crypto";
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
import { AllocationRunner } from "../src/function-executor/allocation.js";
import {
  deserializeValueFromProtocol,
  downloadSerializedObject,
  prepareSerializedObject,
} from "../src/function-executor/blob.js";

type Message = Record<string, any>;

const temporaryDirectories: string[] = [];

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
    const application = registerApplication("protocol_ordered_map_parent", async () => child.map([1, 2]));
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
    const child = registerFunction("protocol_causal_replay_child", async (value: number) => value * 2);
    const application = registerApplication("protocol_causal_replay_parent", async () => {
      const first = child.future(1).run();
      const second = child.future(2).run();
      const waited = await Future.wait([first, second], { returnWhen: "first_completed" });
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
