import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { getProtoPath } from "google-proto-files";
import { zipSync } from "fflate";
import { afterEach, describe, expect, it } from "vitest";
import { File, registerApplication, retries, schema } from "../src/applications/index.js";
import { registerFunction } from "../src/applications/index.js";
import { clearRegistryForTest } from "../src/applications/registry.js";
import {
  AllocationRunner,
  deserializeApplicationArguments,
} from "../src/function-executor/allocation.js";
import {
  deserializeValueFromProtocol,
  downloadBlob,
  downloadSerializedObject,
  prepareSerializedObject,
  uploadBlob,
} from "../src/function-executor/blob.js";
import { FunctionExecutorService } from "../src/function-executor/service.js";

const temporaryDirectories: string[] = [];

afterEach(async () => {
  await Promise.all(temporaryDirectories.splice(0).map((directory) =>
    rm(directory, { recursive: true, force: true }),
  ));
});

describe("TypeScript function executor", () => {
  it("writes shared local-file blob chunks at their blob offsets", async () => {
    const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-blob-upload-test-"));
    temporaryDirectories.push(directory);
    const outputPath = path.join(directory, "output");
    await writeFile(outputPath, new Uint8Array());
    const uri = pathToFileURL(outputPath).href;

    const uploaded = await uploadBlob({
      id: "shared-write",
      chunks: [{ uri, size: 4 }, { uri, size: 4 }],
    }, new TextEncoder().encode("abcdefgh"));

    expect(await readFile(outputPath, "utf8")).toBe("abcdefgh");
    expect(uploaded.chunks).toEqual([
      { uri, size: 4 },
      { uri, size: 4 },
    ]);
  });

  it("downloads shared-URL blob chunks with byte ranges and uploads an empty multipart chunk", async () => {
    const source = Buffer.from("abcdefgh", "utf8");
    const ranges: Array<string | undefined> = [];
    let emptyUploadBytes = -1;
    const server = createServer((request, response) => {
      if (request.method === "PUT") {
        const parts: Buffer[] = [];
        request.on("data", (part: Buffer) => parts.push(part));
        request.on("end", () => {
          emptyUploadBytes = Buffer.concat(parts).byteLength;
          response.writeHead(200, { etag: '"empty-etag"' });
          response.end();
        });
        return;
      }
      const range = request.headers.range;
      ranges.push(range);
      const match = /^bytes=(\d+)-(\d+)$/.exec(range ?? "");
      if (match == null) {
        response.writeHead(200, { "content-length": source.byteLength });
        response.end(source);
        return;
      }
      const start = Number(match[1]);
      const end = Number(match[2]);
      const part = source.subarray(start, end + 1);
      response.writeHead(206, {
        "content-length": part.byteLength,
        "content-range": `bytes ${start}-${end}/${source.byteLength}`,
      });
      response.end(part);
    });
    const port = await new Promise<number>((resolve, reject) => {
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        if (typeof address === "object" && address != null) resolve(address.port);
        else reject(new Error("test HTTP server did not bind to a TCP port"));
      });
      server.once("error", reject);
    });
    try {
      const uri = `http://127.0.0.1:${port}/blob`;
      const downloaded = await downloadBlob({
        id: "shared-read",
        chunks: [{ uri, size: 4 }, { uri, size: 4 }],
      });
      expect(Buffer.from(downloaded).toString("utf8")).toBe("abcdefgh");
      expect(ranges).toEqual(["bytes=0-3", "bytes=4-7"]);

      ranges.length = 0;
      const objectBytes = source.subarray(2, 5);
      const object = await downloadSerializedObject({
        offset: 2,
        manifest: {
          encoding: "SERIALIZED_OBJECT_ENCODING_UTF8_TEXT",
          size: objectBytes.byteLength,
          metadataSize: 0,
          sha256Hash: createHash("sha256").update(objectBytes).digest("hex"),
        },
      }, {
        id: "ranged-object",
        chunks: [{ uri, size: source.byteLength }],
      });
      expect(new TextDecoder().decode(object.data)).toBe("cde");
      expect(ranges).toEqual(["bytes=2-4"]);

      const uploaded = await uploadBlob({
        id: "empty-write",
        chunks: [{ uri: `http://127.0.0.1:${port}/upload`, size: 0 }],
      }, new Uint8Array());
      expect(emptyUploadBytes).toBe(0);
      expect(uploaded.chunks).toEqual([expect.objectContaining({ etag: '"empty-etag"' })]);
    } finally {
      await new Promise<void>((resolve, reject) => server.close((error) => error == null ? resolve() : reject(error)));
    }
  });

  it("runs an application allocation and uploads its JSON output", async () => {
    clearRegistryForTest();
    const application = registerApplication(async (name: string) => `Hello, ${name}!`, {
      name: "hello",
      parameters: [schema.parameter("name", schema.string())] as const,
      returns: schema.string(),
    });
    const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-executor-test-"));
    temporaryDirectories.push(directory);
    const input = new TextEncoder().encode(JSON.stringify("Ada"));
    const inputPath = path.join(directory, "input");
    await writeFile(inputPath, input);
    const allocation = {
      requestId: "request-1",
      functionCallId: "call-1",
      allocationId: "allocation-1",
      replayMode: "REPLAY_MODE_NONE",
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
        argBlobs: [{ id: "input", chunks: [{ uri: pathToFileURL(inputPath).href, size: input.byteLength }] }],
        functionCallMetadata: Buffer.alloc(0),
      },
    };
    const runner = new AllocationRunner(allocation, {
      namespace: "default",
      applicationName: "hello",
      applicationVersion: "v1",
      functionName: "hello",
    }, application.definition);
    const seen = new Set<string>();
    runner.watchState({
      write(state) {
        for (const request of state.outputBlobRequests ?? []) {
          if (seen.has(request.id)) continue;
          seen.add(request.id);
          const outputPath = path.join(directory, request.id);
          queueMicrotask(() => runner.deliverUpdate({
            outputBlob: {
              status: { code: 0 },
              blob: { id: request.id, chunks: [{ uri: pathToFileURL(outputPath).href, size: request.size }] },
            },
          }));
        }
        return true;
      },
      end() {},
      on() {},
    });
    runner.start();
    const events = await runner.getExecutionBatch();
    const finish = events[0].finishAllocation;
    expect(finish.outcomeCode).toBe("ALLOCATION_OUTCOME_CODE_SUCCESS");
    const output = await downloadSerializedObject(finish.value, finish.uploadedFunctionOutputsBlob);
    expect(deserializeValueFromProtocol(output)).toBe("Hello, Ada!");
    expect(await readFile(fileURLToPath(finish.uploadedFunctionOutputsBlob.chunks[0].uri))).not.toHaveLength(0);
  });

  it("decodes multipart JSON blobs according to parameter schemas", async () => {
    clearRegistryForTest();
    const application = registerApplication(async (
      count: number,
      config: { enabled: boolean },
      document: File,
    ) => ({ count, config, document }), {
      name: "multipart_inputs",
      parameters: [
        schema.parameter("count", schema.number()),
        schema.parameter("config", schema.object({ enabled: schema.boolean() })),
        schema.parameter("document", schema.file()),
      ] as const,
      returns: schema.json(),
    });
    const form = new FormData();
    form.append("0", new Blob(["6"]), "0");
    form.append(
      "1",
      new Blob(['{"enabled":true}'], { type: "application/json" }),
      "1",
    );
    form.append(
      "2",
      new Blob(['{"raw":true}'], { type: "application/json" }),
      "2",
    );
    const request = new Request("http://localhost/invoke", { method: "POST", body: form });
    const parsed = await deserializeApplicationArguments(application.definition, {
      data: new Uint8Array(await request.arrayBuffer()),
      contentType: request.headers.get("content-type") ?? undefined,
    });

    expect(parsed.args.slice(0, 2)).toEqual([6, { enabled: true }]);
    expect(parsed.args[2]).toBeInstanceOf(File);
    expect((parsed.args[2] as File).contentType).toBe("application/json");
    expect(new TextDecoder().decode((parsed.args[2] as File).content)).toBe('{"raw":true}');
  });

  it("rejects malformed JSON in non-file multipart parts", async () => {
    clearRegistryForTest();
    const application = registerApplication(async (count: number) => count, {
      name: "malformed_multipart",
      parameters: [schema.parameter("count", schema.number())] as const,
      returns: schema.number(),
    });
    const form = new FormData();
    form.append("count", "not-json");
    const request = new Request("http://localhost/invoke", { method: "POST", body: form });

    await expect(deserializeApplicationArguments(application.definition, {
      data: new Uint8Array(await request.arrayBuffer()),
      contentType: request.headers.get("content-type") ?? undefined,
    })).rejects.toThrow("Failed to deserialize JSON value");
  });

  it("preserves JSON MIME files across protocol value boundaries", () => {
    const content = new TextEncoder().encode('{"value":21}');
    const value = deserializeValueFromProtocol({
      data: content,
      contentType: "application/json",
      encoding: "SERIALIZED_OBJECT_ENCODING_RAW",
    });

    expect(value).toBeInstanceOf(File);
    expect((value as File).contentType).toBe("application/json");
    expect((value as File).content).toEqual(content);
  });

  it("reports output serialization failures as function errors", async () => {
    clearRegistryForTest();
    const application = registerApplication(async () => undefined, {
      name: "invalid_output",
      parameters: [] as const,
      returns: schema.custom<undefined>({}),
    });
    const runner = new AllocationRunner({
      requestId: "request-invalid-output",
      functionCallId: "call-invalid-output",
      allocationId: "allocation-invalid-output",
      replayMode: "REPLAY_MODE_NONE",
      inputs: {
        args: [{
          offset: 0,
          manifest: {
            encoding: "SERIALIZED_OBJECT_ENCODING_RAW",
            size: 0,
            metadataSize: 0,
            contentType: "application/json",
          },
        }],
        argBlobs: [{ id: "empty", chunks: [] }],
        functionCallMetadata: Buffer.alloc(0),
      },
    }, {
      namespace: "default",
      applicationName: "invalid_output",
      applicationVersion: "v1",
      functionName: "invalid_output",
    }, application.definition);

    runner.start();
    const events = await runner.getExecutionBatch();
    expect(events[0].finishAllocation).toMatchObject({
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
    });
  });

  it.each(["application", "internal"] as const)(
    "reports malformed %s invocation values as function errors",
    async (invocationKind) => {
      clearRegistryForTest();
      const application = registerApplication(async (value: unknown) => value, {
        name: `malformed_${invocationKind}_input`,
        parameters: [schema.parameter("value", schema.json())] as const,
        returns: schema.json(),
      });
      const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-malformed-input-test-"));
      temporaryDirectories.push(directory);
      const input = new TextEncoder().encode("not-json");
      const inputPath = path.join(directory, "input");
      await writeFile(inputPath, input);
      const runner = new AllocationRunner({
        requestId: `request-malformed-${invocationKind}`,
        functionCallId: `call-malformed-${invocationKind}`,
        allocationId: `allocation-malformed-${invocationKind}`,
        replayMode: "REPLAY_MODE_NONE",
        inputs: {
          args: [{
            offset: 0,
            manifest: {
              encoding: invocationKind === "application"
                ? "SERIALIZED_OBJECT_ENCODING_RAW"
                : "SERIALIZED_OBJECT_ENCODING_UTF8_JSON",
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
          functionCallMetadata: invocationKind === "application"
            ? Buffer.alloc(0)
            : Buffer.from(JSON.stringify({
                format: "tensorlake.typescript.function-call.v1",
                functionName: application.definition.name,
                argumentCount: 1,
              })),
        },
      }, {
        namespace: "default",
        applicationName: application.definition.name,
        applicationVersion: "v1",
        functionName: application.definition.name,
      }, application.definition);

      runner.start();
      const events = await runner.getExecutionBatch();
      expect(events).toEqual([{ finishAllocation: {
        outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
        failureReason: "ALLOCATION_FAILURE_REASON_FUNCTION_ERROR",
      } }]);
    },
  );

  it("finishes instead of hanging when output blob creation fails without a blob ID", async () => {
    clearRegistryForTest();
    const application = registerApplication(async () => "result", {
      name: "blob_failure",
      parameters: [] as const,
      returns: schema.string(),
    });
    const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-blob-failure-test-"));
    temporaryDirectories.push(directory);
    const inputPath = path.join(directory, "input");
    await writeFile(inputPath, "{}");
    const runner = new AllocationRunner({
      requestId: "request-blob-failure",
      functionCallId: "call-blob-failure",
      allocationId: "allocation-blob-failure",
      replayMode: "REPLAY_MODE_NONE",
      inputs: {
        args: [{
          offset: 0,
          manifest: {
            encoding: "SERIALIZED_OBJECT_ENCODING_RAW",
            size: 2,
            metadataSize: 0,
            sha256Hash: createHash("sha256").update("{}").digest("hex"),
            contentType: "application/json",
          },
        }],
        argBlobs: [{
          id: "input",
          chunks: [{ uri: pathToFileURL(inputPath).href, size: 2 }],
        }],
        functionCallMetadata: Buffer.alloc(0),
      },
    }, {
      namespace: "default",
      applicationName: "blob_failure",
      applicationVersion: "v1",
      functionName: "blob_failure",
    }, application.definition);
    runner.watchState({
      write(state) {
        if ((state.outputBlobRequests?.length ?? 0) > 0) {
          queueMicrotask(() => runner.deliverUpdate({
            outputBlob: {
              status: { code: 13, message: "output blob creation failed" },
            },
          }));
        }
        return true;
      },
      end() {},
      on() {},
    });
    runner.start();

    const events = await runner.getExecutionBatch();
    expect(events[0].finishAllocation).toMatchObject({
      outcomeCode: "ALLOCATION_OUTCOME_CODE_FAILURE",
      failureReason: "ALLOCATION_FAILURE_REASON_INTERNAL_ERROR",
    });
  });

  it("creates durable child calls and strictly replays their result", async () => {
    clearRegistryForTest();
    const child = registerFunction(async (value: number) => value * 2, {
      name: "child",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
      retries: retries({ maxRetries: 1 }),
    });
    const application = registerApplication(async (value: number) => (await child(value)) + 1, {
      name: "parent",
      parameters: [schema.parameter("value", schema.number())] as const,
      returns: schema.number(),
    });
    const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-durable-test-"));
    temporaryDirectories.push(directory);
    const input = new TextEncoder().encode("5");
    const inputPath = path.join(directory, "input");
    await writeFile(inputPath, input);
    const baseAllocation = {
      requestId: "request-child",
      functionCallId: "parent-call",
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
        argBlobs: [{ id: "input", chunks: [{ uri: pathToFileURL(inputPath).href, size: input.byteLength }] }],
        functionCallMetadata: Buffer.alloc(0),
      },
    };
    const functionRef = {
      namespace: "default",
      applicationName: "parent",
      applicationVersion: "v1",
      functionName: "parent",
    };

    const addBlobResponder = (runner: AllocationRunner) => {
      const seen = new Set<string>();
      runner.watchState({
        write(state) {
          for (const request of state.outputBlobRequests ?? []) {
            if (seen.has(request.id)) continue;
            seen.add(request.id);
            queueMicrotask(() => runner.deliverUpdate({
              outputBlob: {
                status: { code: 0 },
                blob: {
                  id: request.id,
                  chunks: [{ uri: pathToFileURL(path.join(directory, request.id)).href, size: request.size }],
                },
              },
            }));
          }
          return true;
        },
        end() {},
        on() {},
      });
    };

    const readRequests: Record<string, any>[] = [];
    let readWaiter: ((request: Record<string, any>) => void) | undefined;
    const nextRead = async () => {
      const existing = readRequests.shift();
      if (existing != null) return existing;
      return new Promise<Record<string, any>>((resolve) => { readWaiter = resolve; });
    };
    const runner = new AllocationRunner({
      ...baseAllocation,
      allocationId: "allocation-live",
      replayMode: "REPLAY_MODE_NONE",
    }, functionRef, application.definition);
    addBlobResponder(runner);
    runner.watchEventLogReads({
      write(request) {
        if (readWaiter != null) {
          const resolve = readWaiter;
          readWaiter = undefined;
          resolve(request);
        } else {
          readRequests.push(request);
        }
        return true;
      },
      end() {},
      on() {},
    });
    runner.start();

    const createBatch = await runner.getExecutionBatch();
    const durableId = createBatch[0].createFunctionCall.updates.rootFunctionCallId as string;
    expect(createBatch[0].createFunctionCall.updates.updates[0].functionCall.target.functionName).toBe("child");
    runner.advanceExecutionBatch();
    await nextRead();
    runner.deliverEventLogResponse({
      allocationId: "allocation-live",
      entries: [{ clock: 1, functionCallCreated: { functionCallId: durableId, status: { code: 0 } } }],
      lastClock: 1,
      hasMore: false,
    });

    const watcherBatch = await runner.getExecutionBatch();
    expect(watcherBatch[0].createFunctionCallWatcher.functionCallId).toBe(durableId);
    runner.advanceExecutionBatch();
    await nextRead();
    runner.deliverEventLogResponse({
      allocationId: "allocation-live",
      entries: [{ clock: 2, functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } } }],
      lastClock: 2,
      hasMore: false,
    });

    const childOutput = prepareSerializedObject(10, 0, durableId);
    const childOutputPath = path.join(directory, "child-output");
    await writeFile(childOutputPath, childOutput.bytes);
    const resultEvent = {
      clock: 3,
      functionCallWatcherResult: {
        functionCallId: durableId,
        watcherStatus: "FUNCTION_CALL_WATCHER_STATUS_COMPLETED",
        outcomeCode: "ALLOCATION_OUTCOME_CODE_SUCCESS",
        valueOutput: childOutput.object,
        valueBlob: {
          id: "child-output",
          chunks: [{ uri: pathToFileURL(childOutputPath).href, size: childOutput.bytes.byteLength }],
        },
      },
    };
    await nextRead();
    runner.deliverEventLogResponse({
      allocationId: "allocation-live",
      entries: [resultEvent],
      lastClock: 3,
      hasMore: false,
    });
    const finishBatch = await runner.getExecutionBatch();
    const liveFinish = finishBatch[0].finishAllocation;
    expect(deserializeValueFromProtocol(
      await downloadSerializedObject(liveFinish.value, liveFinish.uploadedFunctionOutputsBlob),
    )).toBe(11);

    const replay = new AllocationRunner({
      ...baseAllocation,
      allocationId: "allocation-replay",
      replayMode: "REPLAY_MODE_STRICT",
    }, functionRef, application.definition);
    addBlobResponder(replay);
    replay.watchEventLogReads({
      write(request) {
        queueMicrotask(() => replay.deliverEventLogResponse({
          allocationId: request.allocationId,
          entries: [
            { clock: 1, functionCallCreated: { functionCallId: durableId, status: { code: 0 } } },
            { clock: 2, functionCallWatcherCreated: { functionCallId: durableId, status: { code: 0 } } },
            resultEvent,
          ],
          lastClock: 3,
          hasMore: false,
        }));
        return true;
      },
      end() {},
      on() {},
    });
    replay.start();
    const replayBatch = await replay.getExecutionBatch();
    expect(replayBatch).toHaveLength(1);
    expect(replayBatch[0].finishAllocation).toBeDefined();
    expect(deserializeValueFromProtocol(
      await downloadSerializedObject(
        replayBatch[0].finishAllocation.value,
        replayBatch[0].finishAllocation.uploadedFunctionOutputsBlob,
      ),
    )).toBe(11);
  });

  it("serves protocol info and initializes from an ESM application bundle", async () => {
    clearRegistryForTest();
    const runtime = new TextEncoder().encode(`
      const definition = {
        name: "hello_world",
        handler: async () => null,
        parameters: [],
        returns: { jsonSchema: {} },
        options: {},
        application: { tags: {}, retries: { maxRetries: 0 }, version: "v1" },
      };
      export function __tensorlakeGetFunction(name) {
        if (name !== definition.name) throw new Error("unknown function " + name);
        return definition;
      }
    `);
    const manifest = new TextEncoder().encode(JSON.stringify({
      format_version: 2,
      runtime: "typescript",
      minimum_node_major: 24,
      module: "runtime.mjs",
      functions: { hello_world: { name: "hello_world" } },
    }));
    const codeZip = zipSync({
      "runtime.mjs": runtime,
      ".tensorlake_code_manifest.json": manifest,
    });
    const protoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../proto");
    const definition = loadSync(
      path.join(protoRoot, "tensorlake/function_executor/proto/function_executor.proto"),
      {
        includeDirs: [path.dirname(path.join(protoRoot, "tensorlake/function_executor/proto/function_executor.proto")), protoRoot, getProtoPath()],
        keepCase: false,
        longs: Number,
        enums: String,
        defaults: false,
        oneofs: true,
      },
    );
    const loaded = grpc.loadPackageDefinition(definition) as Record<string, any>;
    const Executor = loaded.function_executor_service.FunctionExecutor;
    const server = new grpc.Server();
    server.addService(Executor.service, new FunctionExecutorService().implementation);
    const port = await new Promise<number>((resolve, reject) => {
      server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (error, boundPort) => {
        if (error != null) reject(error);
        else resolve(boundPort);
      });
    });
    const client = new Executor(`127.0.0.1:${port}`, grpc.credentials.createInsecure());
    const unary = (method: string, request: Record<string, unknown>) => new Promise<Record<string, any>>(
      (resolve, reject) => client[method](request, (error: Error | null, response: Record<string, any>) => {
        if (error != null) reject(error);
        else resolve(response);
      }),
    );
    try {
      const info = await unary("getInfo", {});
      expect(info.sdkLanguage).toBe("typescript");
      expect(info.version).toBe("0.1.3");
      const initialized = await unary("initialize", {
        function: {
          namespace: "default",
          applicationName: "hello_world",
          applicationVersion: "v1",
          functionName: "hello_world",
        },
        applicationCode: {
          manifest: {
            encoding: "SERIALIZED_OBJECT_ENCODING_BINARY_ZIP",
            size: codeZip.byteLength,
            metadataSize: 0,
          },
          data: codeZip,
        },
      });
      expect(initialized.outcomeCode).toBe("INITIALIZATION_OUTCOME_CODE_SUCCESS");
      expect((await unary("checkHealth", {})).healthy).toBe(true);
    } finally {
      client.close();
      server.forceShutdown();
    }
  }, 20_000);

  it("can retry initialization after validation fails", async () => {
    const manifest = new TextEncoder().encode(JSON.stringify({
      format_version: 2,
      runtime: "typescript",
      minimum_node_major: 24,
      module: "runtime.mjs",
      functions: { child: { name: "child" }, app: { name: "app" } },
    }));
    const runtime = (application: boolean) => new TextEncoder().encode(`
      const child = {
        name: "child", handler: async () => null, parameters: [],
        returns: { jsonSchema: {} }, options: {},
      };
      const app = {
        ...child,
        name: "app",
        application: ${application ? '{ tags: {}, retries: { maxRetries: 0 }, version: "v1" }' : "undefined"},
      };
      export function __tensorlakeGetFunction(name) {
        if (name === "child") return child;
        if (name === "app") return app;
        throw new Error("unknown function " + name);
      }
    `);
    const request = (application: boolean) => ({
      function: {
        namespace: "default",
        applicationName: "app",
        applicationVersion: "v1",
        functionName: "child",
      },
      applicationCode: {
        manifest: {
          encoding: "SERIALIZED_OBJECT_ENCODING_BINARY_ZIP",
          size: 0,
          metadataSize: 0,
        },
        data: zipSync({
          "runtime.mjs": runtime(application),
          ".tensorlake_code_manifest.json": manifest,
        }),
      },
    });
    const service = new FunctionExecutorService();
    const initialize = (application: boolean) => new Promise<Record<string, any>>((resolve, reject) => {
      const call = (service.implementation.initialize as any).bind(service.implementation);
      call({ request: request(application) }, (error: Error | null, response: Record<string, any>) => {
        if (error != null) reject(error);
        else resolve(response);
      });
    });

    expect((await initialize(false)).outcomeCode).toBe("INITIALIZATION_OUTCOME_CODE_FAILURE");
    expect((await initialize(true)).outcomeCode).toBe("INITIALIZATION_OUTCOME_CODE_SUCCESS");
  });

  it("reconciles request state and output blobs over the gRPC transport", async () => {
    const runtime = new TextEncoder().encode(`
      const definition = {
        name: "stateful",
        handler: async (input) => {
          const storage = globalThis[Symbol.for("tensorlake.applications.request-context-storage.v1")];
          const context = storage?.getStore();
          if (context == null) throw new Error("request context is unavailable");
          await context.progress.update(1, 1);
          await context.state.set("label", input.label);
          return input.label;
        },
        parameters: [{ name: "input", schema: { jsonSchema: {} } }],
        returns: { jsonSchema: {} },
        options: {},
        application: { tags: {}, retries: { maxRetries: 0 }, version: "v1" },
      };
      export function __tensorlakeGetFunction(name) {
        if (name !== definition.name) throw new Error("unknown function " + name);
        return definition;
      }
    `);
    const manifest = new TextEncoder().encode(JSON.stringify({
      format_version: 2,
      runtime: "typescript",
      minimum_node_major: 24,
      module: "runtime.mjs",
      functions: { stateful: { name: "stateful" } },
    }));
    const codeZip = zipSync({
      "runtime.mjs": runtime,
      ".tensorlake_code_manifest.json": manifest,
    });
    const protoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../proto");
    const protoPath = path.join(protoRoot, "tensorlake/function_executor/proto/function_executor.proto");
    const definition = loadSync(protoPath, {
      includeDirs: [path.dirname(protoPath), protoRoot, getProtoPath()],
      keepCase: false,
      longs: Number,
      enums: String,
      defaults: false,
      oneofs: true,
    });
    const loaded = grpc.loadPackageDefinition(definition) as Record<string, any>;
    const Executor = loaded.function_executor_service.FunctionExecutor;
    const server = new grpc.Server();
    server.addService(Executor.service, new FunctionExecutorService().implementation);
    const port = await new Promise<number>((resolve, reject) => {
      server.bindAsync("127.0.0.1:0", grpc.ServerCredentials.createInsecure(), (error, boundPort) => {
        if (error != null) reject(error);
        else resolve(boundPort);
      });
    });
    const client = new Executor(`127.0.0.1:${port}`, grpc.credentials.createInsecure());
    const unary = (method: string, request: Record<string, unknown>) => new Promise<Record<string, any>>(
      (resolve, reject) => client[method](request, (error: Error | null, response: Record<string, any>) => {
        if (error != null) reject(error);
        else resolve(response);
      }),
    );
    const directory = await mkdtemp(path.join(os.tmpdir(), "tensorlake-grpc-state-test-"));
    temporaryDirectories.push(directory);
    const input = new TextEncoder().encode(JSON.stringify({ label: "transport-ok" }));
    const inputPath = path.join(directory, "input");
    await writeFile(inputPath, input);
    const allocationId = "allocation-stateful";
    const seenOperations = new Set<string>();
    const seenBlobs = new Set<string>();
    let stateStream: grpc.ClientReadableStream<Record<string, any>> | undefined;
    let eventReadStream: grpc.ClientReadableStream<Record<string, any>> | undefined;
    try {
      expect((await unary("initialize", {
        function: {
          namespace: "default",
          applicationName: "stateful",
          applicationVersion: "v1",
          functionName: "stateful",
        },
        applicationCode: {
          manifest: {
            encoding: "SERIALIZED_OBJECT_ENCODING_BINARY_ZIP",
            size: codeZip.byteLength,
            metadataSize: 0,
          },
          data: codeZip,
        },
      })).outcomeCode).toBe("INITIALIZATION_OUTCOME_CODE_SUCCESS");
      await unary("createAllocation", {
        allocation: {
          requestId: "request-stateful",
          functionCallId: "call-stateful",
          allocationId,
          replayMode: "REPLAY_MODE_NONE",
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
          },
        },
      });

      // The dataplane waits for this stream's response metadata before it starts
      // polling execution batches. No durable event read exists yet, so the
      // service must establish the stream without waiting for a first message.
      eventReadStream = client.watchAllocationEventLogReads({ allocationId });
      await new Promise<void>((resolve, reject) => {
        const timeout = setTimeout(
          () => reject(new Error("event log read stream metadata was not sent")),
          1_000,
        );
        eventReadStream?.once("metadata", () => {
          clearTimeout(timeout);
          resolve();
        });
        eventReadStream?.once("error", (error: Error) => {
          clearTimeout(timeout);
          reject(error);
        });
      });

      stateStream = client.watchAllocationState({ allocationId });
      stateStream.on("data", (state: Record<string, any>) => {
        for (const operation of state.requestStateOperations ?? []) {
          const operationId = String(operation.operationId);
          if (seenOperations.has(operationId)) continue;
          seenOperations.add(operationId);
          if (operation.prepareWrite != null) {
            const statePath = path.join(directory, `state-${operationId}`);
            void unary("sendAllocationUpdate", {
              allocationId,
              requestStateOperationResult: {
                operationId,
                status: { code: 0 },
                prepareWrite: {
                  blob: {
                    id: `state-${operationId}`,
                    chunks: [{
                      uri: pathToFileURL(statePath).href,
                      size: operation.prepareWrite.size,
                    }],
                  },
                },
              },
            });
          } else if (operation.commitWrite != null) {
            void unary("sendAllocationUpdate", {
              allocationId,
              requestStateOperationResult: {
                operationId,
                status: { code: 0 },
                commitWrite: {},
              },
            });
          }
        }
        for (const request of state.outputBlobRequests ?? []) {
          const id = String(request.id);
          if (seenBlobs.has(id)) continue;
          seenBlobs.add(id);
          const outputPath = path.join(directory, `output-${id}`);
          void unary("sendAllocationUpdate", {
            allocationId,
            outputBlob: {
              status: { code: 0 },
              blob: {
                id,
                chunks: [{ uri: pathToFileURL(outputPath).href, size: request.size }],
              },
            },
          });
        }
      });

      const response = await unary("getAllocationExecutionLogBatch", { allocationId });
      const finish = response.events[0].finishAllocation;
      expect(finish.outcomeCode).toBe("ALLOCATION_OUTCOME_CODE_SUCCESS");
      expect(deserializeValueFromProtocol(
        await downloadSerializedObject(finish.value, finish.uploadedFunctionOutputsBlob),
      )).toBe("transport-ok");
      expect(seenOperations).toHaveLength(2);
      expect(seenBlobs).toHaveLength(1);
    } finally {
      eventReadStream?.cancel();
      stateStream?.cancel();
      client.close();
      server.forceShutdown();
    }
  }, 20_000);
});
