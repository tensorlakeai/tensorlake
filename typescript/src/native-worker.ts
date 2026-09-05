import { parentPort } from "node:worker_threads";
import { loadNative } from "./native-binding.js";
import type {
  WorkerRequest,
  WorkerResponse,
} from "./native-worker-protocol.js";

const port = parentPort!;
// This module is only a worker entrypoint. All platform detection, dlopen and
// native object construction happen in this isolate.
const binding = loadNative<Record<string, any>>();
const handles = new Map<number, { value?: any; error?: unknown }>();
interface StreamState {
  cancel?: () => void;
  handleId?: number;
  ack?: () => void;
}
const streams = new Map<number, StreamState>();

function getHandle(id: number): any {
  const entry = handles.get(id);
  if (!entry) throw new Error("Tensorlake native handle has been released");
  if (entry.error) throw entry.error;
  return entry.value;
}

/** Structured clone turns Buffers into Uint8Arrays; napi expects Buffers. */
function nativeArgs(value: any): any {
  if (value instanceof Uint8Array)
    return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  if (Array.isArray(value)) return value.map(nativeArgs);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [k, nativeArgs(v)]),
    );
  return value;
}

function send(message: WorkerResponse): void {
  port.postMessage(message);
}

port.on("message", (message: WorkerRequest) => {
  if (message.type === "create") {
    try {
      const { kind, args, id } = message.handle;
      const ctor = binding[kind];
      if (typeof ctor !== "function")
        throw new Error(
          `native binding does not export ${kind}; rebuild with 'npm run build:native'`,
        );
      const value =
        message.parentId === undefined
          ? new ctor(...args)
          : getHandle(message.parentId).connectProxy(...args);
      handles.set(id, { value });
    } catch (error) {
      handles.set(message.handle.id, { error });
    }
    return;
  }
  if (message.type === "release") {
    handles.delete(message.handleId);
    for (const stream of streams.values()) {
      if (stream.handleId === message.handleId) {
        stream.cancel?.();
        stream.ack?.();
      }
    }
    return;
  }
  if (message.type === "cancel" || message.type === "ack") {
    const stream = streams.get(message.id);
    if (message.type === "cancel") stream?.cancel?.();
    stream?.ack?.();
    if (stream) stream.ack = undefined;
    return;
  }
  if (message.type === "call") void invoke(message);
});

async function invoke(
  message: Extract<WorkerRequest, { type: "call" }>,
): Promise<void> {
  try {
    const target =
      message.handleId === undefined ? binding : getHandle(message.handleId);
    const args = nativeArgs(message.args);
    let stream: StreamState | undefined;
    if (message.stream) {
      stream = { handleId: message.handleId };
      const state = stream;
      streams.set(message.id, stream);
      args.push(
        (value: string) =>
          new Promise<void>((resolve) => {
            state.ack = resolve;
            send({ type: "event", id: message.id, value });
          }),
      );
    }
    const call = target[message.method](...args);
    if (stream) stream.cancel = call.cancel;
    const value = await (stream ? call.result : call);
    send({ type: "result", id: message.id, value });
  } catch (cause) {
    const error = cause instanceof Error ? cause : new Error(String(cause));
    send({
      type: "error",
      id: message.id,
      error: { name: error.name, message: error.message, stack: error.stack },
    });
  } finally {
    streams.get(message.id)?.ack?.();
    streams.delete(message.id);
  }
}
