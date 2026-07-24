import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as grpc from "@grpc/grpc-js";
import { loadSync } from "@grpc/proto-loader";
import { getProtoPath } from "google-proto-files";
import { FunctionExecutorService } from "./service.js";

interface Arguments {
  address?: string;
  executorId?: string;
  functionExecutorId: string;
}

const SHUTDOWN_GRACE_PERIOD_MS = 10_000;

function parseArguments(argv: string[]): Arguments {
  const result: Arguments = { functionExecutorId: "" };
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    const [key, inline] = argument.split("=", 2);
    const value = inline ?? argv[index + 1];
    if (key === "--address") result.address = value;
    else if (key === "--executor-id") result.executorId = value;
    else if (key === "--function-executor-id") result.functionExecutorId = value ?? "";
    else continue;
    if (inline == null) index += 1;
  }
  return result;
}

function serviceDefinition(): grpc.ServiceDefinition {
  const protoRoot = functionExecutorProtoRoot();
  const protoPath = path.join(protoRoot, "tensorlake/function_executor/proto/function_executor.proto");
  const packageDefinition = loadSync(protoPath, {
    includeDirs: [path.dirname(protoPath), protoRoot, getProtoPath()],
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: false,
    oneofs: true,
  });
  const loaded = grpc.loadPackageDefinition(packageDefinition) as Record<string, any>;
  return loaded.function_executor_service.FunctionExecutor.service as grpc.ServiceDefinition;
}

function functionExecutorProtoRoot(): string {
  const relativeRoots = [
    // Standalone function-executor capsule.
    "../../proto/",
    // Function executor launched directly from the published SDK package.
    "../../runtime/function-executor/package/proto/",
    // Development checkout: both language runtimes use the repository source.
    "../../../proto/",
  ];
  for (const relativeRoot of relativeRoots) {
    const protoRoot = fileURLToPath(new URL(relativeRoot, import.meta.url));
    const protoPath = path.join(protoRoot, "tensorlake/function_executor/proto/function_executor.proto");
    if (existsSync(protoPath)) return protoRoot;
  }
  throw new Error("Cannot find the shared Tensorlake function-executor protobuf definition");
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  if (nodeMajor < 24) throw new Error(`Tensorlake TypeScript functions require Node 24 or newer; got ${process.versions.node}`);
  const args = parseArguments(argv);
  if (!args.address) throw new Error("--address argument is required");
  if (!args.executorId) throw new Error("--executor-id argument is required");

  process.stderr.write(`${JSON.stringify({
    timestamp: new Date().toISOString(),
    level: "info",
    component: "typescript_function_executor_main",
    message: "starting TypeScript function executor",
    address: args.address,
    executor_id: args.executorId,
    fn_executor_id: args.functionExecutorId,
    node_version: process.versions.node,
    pid: process.pid,
  })}\n`);

  const server = new grpc.Server({
    "grpc.max_receive_message_length": -1,
    "grpc.max_send_message_length": -1,
    "grpc.so_reuseport": 0,
  });
  const service = new FunctionExecutorService();
  server.addService(serviceDefinition(), service.implementation);
  await new Promise<void>((resolve, reject) => {
    server.bindAsync(args.address as string, grpc.ServerCredentials.createInsecure(), (error) => {
      if (error != null) reject(error);
      else resolve();
    });
  });
  process.stderr.write(`${JSON.stringify({
    timestamp: new Date().toISOString(),
    level: "info",
    component: "typescript_function_executor_main",
    message: "started TypeScript function executor",
    address: args.address,
    executor_id: args.executorId,
    fn_executor_id: args.functionExecutorId,
    node_version: process.versions.node,
  })}\n`);
  let stopping = false;
  const shutdown = (signal: string) => {
    if (stopping) return;
    stopping = true;
    process.stderr.write(`${JSON.stringify({
      timestamp: new Date().toISOString(),
      level: "info",
      component: "typescript_function_executor_main",
      message: "stopping TypeScript function executor",
      signal,
      executor_id: args.executorId,
      fn_executor_id: args.functionExecutorId,
      pid: process.pid,
    })}\n`);
    const forceShutdownTimer = setTimeout(() => {
      process.stderr.write(`${JSON.stringify({
        timestamp: new Date().toISOString(),
        level: "error",
        component: "typescript_function_executor_main",
        message: "forcing TypeScript function executor shutdown after grace period",
        signal,
        executor_id: args.executorId,
        fn_executor_id: args.functionExecutorId,
        pid: process.pid,
        grace_period_ms: SHUTDOWN_GRACE_PERIOD_MS,
      })}\n`);
      server.forceShutdown();
      process.exit(1);
    }, SHUTDOWN_GRACE_PERIOD_MS);
    const serviceShutdown = service.shutdown(new Error(`Function executor received ${signal}`));
    const serverShutdown = new Promise<void>((resolve, reject) => {
      server.tryShutdown((error) => {
        if (error != null) reject(error);
        else resolve();
      });
    });
    void Promise.all([serviceShutdown, serverShutdown]).then(
      () => {
        clearTimeout(forceShutdownTimer);
        process.stderr.write(`${JSON.stringify({
          timestamp: new Date().toISOString(),
          level: "info",
          component: "typescript_function_executor_main",
          message: "stopped TypeScript function executor",
          signal,
          executor_id: args.executorId,
          fn_executor_id: args.functionExecutorId,
          pid: process.pid,
        })}\n`);
        process.exit(0);
      },
      (error: unknown) => {
        clearTimeout(forceShutdownTimer);
        process.stderr.write(`${JSON.stringify({
          timestamp: new Date().toISOString(),
          level: "error",
          component: "typescript_function_executor_main",
          message: "TypeScript function executor shutdown failed",
          signal,
          executor_id: args.executorId,
          fn_executor_id: args.functionExecutorId,
          pid: process.pid,
          error: error instanceof Error ? `${error.name}: ${error.message}` : String(error),
        })}\n`);
        server.forceShutdown();
        process.exit(1);
      },
    );
  };
  process.once("SIGINT", () => shutdown("SIGINT"));
  process.once("SIGTERM", () => shutdown("SIGTERM"));
}
