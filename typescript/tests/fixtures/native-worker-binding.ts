import { threadId } from "node:worker_threads";
import { setTimeout as delay } from "node:timers/promises";

// Simulate a slow dlopen before the worker can dispatch any requests.
Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 300);
let clients = 0;
let events = 0;
let cancelled = 0;
class StreamControl {
  stopped = false;
  private resolve!: () => void;
  done = new Promise<void>((resolve) => {
    this.resolve = resolve;
  });
  cancel() {
    if (!this.stopped) cancelled++;
    this.stopped = true;
    this.resolve();
  }
}
class NativeSandboxProxyClient {
  constructor(
    readonly url: string,
    readonly id: string,
    readonly key?: string,
    readonly clientId = 0,
  ) {}
  async health() {
    return {
      traceId: "trace",
      json: JSON.stringify({
        threadId,
        clients,
        clientId: this.clientId,
        key: this.key,
        events,
        cancelled,
      }),
    };
  }
  async readFile() {
    return { traceId: "bytes", data: Buffer.from([0, 255, 128, 7]) };
  }
  async writeFile(_path: string, value: Buffer) {
    if (!Buffer.isBuffer(value)) throw Error("Expected Buffer");
    return value.toString("hex");
  }
  async getProcess(id: string) {
    if (id === "crash") process.exit(17);
    if (id === "pending") await delay(10_000);
    if (id === "missing")
      throw Error(
        JSON.stringify({
          category: "remote_api",
          status: 404,
          message: "missing process",
        }),
      );
    return { traceId: "trace", json: JSON.stringify({ pid: 7 }) };
  }
  followStdout(id: string, emit: (event: string) => Promise<void>) {
    const control = new StreamControl();
    return {
      result: this.streamStdout(id, emit, control),
      cancel: () => control.cancel(),
    };
  }
  private async streamStdout(
    id: string,
    emit: (event: string) => Promise<void>,
    control: StreamControl,
  ) {
    if (id === "quiet") {
      await control.done;
      return "quiet";
    }
    for (let i = 0; i < 100 && !control.stopped; i++) {
      events++;
      await Promise.race([
        emit(JSON.stringify({ line: String(i) })),
        control.done,
      ]);
    }
    return "stream-trace";
  }
}
class NativeSandboxClient {
  readonly clientId = ++clients;
  constructor(
    readonly url: string,
    readonly key?: string,
  ) {}
  selectSandboxProxyUrl(_id: string, url: string) {
    return url;
  }
  connectProxy(url: string, id: string) {
    return new NativeSandboxProxyClient(url, id, this.key, this.clientId);
  }
}
class NativeRepositoryClient {
  async pushFilesystemFiles(_name: string, files: Array<{ content: Buffer }>) {
    if (!Buffer.isBuffer(files[0].content))
      throw Error("Expected nested Buffer");
    return { traceId: "repo", json: files[0].content.toString() };
  }
}
export function loadNative() {
  return {
    NativeSandboxClient,
    NativeSandboxProxyClient,
    NativeRepositoryClient,
    validateManagedName: (name: string) => {
      if (!name) throw Error("empty name");
    },
  };
}
