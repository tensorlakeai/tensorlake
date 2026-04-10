import * as net from "node:net";
import { once } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  static instances: MockWebSocket[] = [];

  readonly url: string;
  readonly options: { headers?: Record<string, string> } | undefined;
  readonly sent: Buffer[] = [];
  readyState = MockWebSocket.CONNECTING;

  private readonly handlers: Record<string, ((...args: any[]) => void)[]> = {};

  constructor(url: string, options?: { headers?: Record<string, string> }) {
    this.url = url;
    this.options = options;
    MockWebSocket.instances.push(this);

    queueMicrotask(() => {
      this.readyState = MockWebSocket.OPEN;
      this.emit("open");
    });
  }

  on(event: string, handler: (...args: any[]) => void): this {
    this.handlers[event] ??= [];
    this.handlers[event].push(handler);
    return this;
  }

  removeListener(event: string, handler: (...args: any[]) => void): this {
    this.handlers[event] = (this.handlers[event] ?? []).filter(
      (candidate) => candidate !== handler,
    );
    return this;
  }

  send(data: Buffer | Uint8Array, _options?: unknown, cb?: (error?: Error) => void): void {
    this.sent.push(Buffer.from(data));
    cb?.();
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED;
    this.emit("close", 1000, Buffer.alloc(0));
  }

  pong(_data?: Buffer, _mask?: boolean, cb?: () => void): void {
    cb?.();
  }

  emitMessage(data: Buffer): void {
    this.emit("message", data, true);
  }

  private emit(event: string, ...args: any[]): void {
    for (const handler of this.handlers[event] ?? []) {
      handler(...args);
    }
  }
}

vi.mock("ws", () => ({
  default: MockWebSocket,
}));

describe("TcpTunnel", () => {
  let Sandbox: typeof import("../src/sandbox.js").Sandbox;

  beforeEach(async () => {
    MockWebSocket.instances = [];
    vi.resetModules();
    ({ Sandbox } = await import("../src/sandbox.js"));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function waitForSocket(index: number): Promise<MockWebSocket> {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      const socket = MockWebSocket.instances[index];
      if (socket) {
        return socket;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for websocket instance ${index}`);
  }

  async function waitForSentFrame(
    socket: MockWebSocket,
    index: number,
  ): Promise<Buffer> {
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      const frame = socket.sent[index];
      if (frame) {
        return frame;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    throw new Error(`timed out waiting for websocket frame ${index}`);
  }

  it("binds a local port and relays bytes through the tunnel websocket", async () => {
    const sandbox = new Sandbox({
      sandboxId: "sbx-1",
      proxyUrl: "https://sandbox.tensorlake.ai",
      apiKey: "test-api-key",
    });

    const tunnel = await sandbox.createTunnel(8080, { localPort: 0 });
    const client = net.createConnection({
      host: tunnel.localHost,
      port: tunnel.localPort,
    });

    await once(client, "connect");
    const socket = await waitForSocket(0);
    expect(socket.url).toBe(
      "wss://sbx-1.sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=8080",
    );
    expect(socket.options?.headers?.Authorization).toBe("Bearer test-api-key");

    client.write("hello");
    const firstFrame = await waitForSentFrame(socket, 0);
    expect(firstFrame.toString("utf8")).toBe("hello");

    const dataPromise = once(client, "data") as Promise<[Buffer]>;
    socket.emitMessage(Buffer.from("world", "utf8"));
    const [data] = await dataPromise;
    expect(data.toString("utf8")).toBe("world");

    client.destroy();
    await tunnel.close();
  });

  it("uses the sandbox host override when tunneling through localhost proxy", async () => {
    const sandbox = new Sandbox({
      sandboxId: "sbx-local",
      proxyUrl: "http://localhost:9443",
      apiKey: "test-api-key",
    });

    const tunnel = await sandbox.createTunnel(5901, { localPort: 0 });
    const client = net.createConnection({
      host: tunnel.localHost,
      port: tunnel.localPort,
    });

    await once(client, "connect");
    const socket = await waitForSocket(0);
    expect(socket.url).toBe("ws://localhost:9443/api/v1/tunnels/tcp?port=5901");
    expect(socket.options?.headers?.Host).toBe("sbx-local.local");

    client.destroy();
    await tunnel.close();
  });
});
