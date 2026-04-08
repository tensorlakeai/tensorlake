import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
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

  send(data: Buffer | Uint8Array, cb?: (error?: Error) => void): void {
    this.sent.push(Buffer.from(data));
    cb?.();
  }

  close(code = 1000, reason = "client disconnect"): void {
    this.readyState = MockWebSocket.CLOSED;
    this.emit("close", code, Buffer.from(reason, "utf8"));
  }

  emitMessage(data: Buffer): void {
    this.emit("message", data);
  }

  emitClose(code: number, reason = ""): void {
    this.readyState = MockWebSocket.CLOSED;
    this.emit("close", code, Buffer.from(reason, "utf8"));
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

describe("Sandbox PTY", () => {
  let originalFetch: typeof globalThis.fetch;
  let Sandbox: typeof import("../src/sandbox.js").Sandbox;

  beforeEach(async () => {
    originalFetch = globalThis.fetch;
    MockWebSocket.instances = [];
    vi.resetModules();
    ({ Sandbox } = await import("../src/sandbox.js"));
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    globalThis.fetch = vi.fn(handler as typeof fetch);
  }

  function makeSandbox() {
    return new Sandbox({
      sandboxId: "sbx-1",
      proxyUrl: "https://sandbox.tensorlake.ai",
      apiKey: "test-api-key",
    });
  }

  it("creates a PTY handle, sends READY, streams data, and waits for exit", async () => {
    mockFetch((url, init) => {
      if (url.endsWith("/api/v1/pty") && init?.method === "POST") {
        return new Response(
          JSON.stringify({
            session_id: "sess-1",
            token: "tok-1",
          }),
          { status: 201 },
        );
      }
      return new Response("", { status: 404 });
    });

    const onData = vi.fn();
    const onExit = vi.fn();
    const sandbox = makeSandbox();
    const pty = await sandbox.createPty({
      command: "/bin/bash",
      onData,
      onExit,
    });

    const socket = MockWebSocket.instances[0];
    expect(socket.url).toBe(
      "wss://sbx-1.sandbox.tensorlake.ai/api/v1/pty/sess-1/ws?token=tok-1",
    );
    expect(socket.options?.headers?.["X-PTY-Token"]).toBe("tok-1");
    expect(socket.sent[0]).toEqual(Buffer.from([0x02]));

    socket.emitMessage(Buffer.from([0x00, 0x68, 0x69]));
    expect(new Uint8Array(onData.mock.calls[0][0] as Uint8Array)).toEqual(
      new Uint8Array([0x68, 0x69]),
    );

    socket.emitMessage(Buffer.from([0x03, 0x00, 0x00, 0x00, 0x07]));
    await expect(pty.wait()).resolves.toBe(7);
    expect(onExit).toHaveBeenCalledWith(7);
  });

  it("sends input and resize frames", async () => {
    mockFetch((url, init) => {
      if (url.endsWith("/api/v1/pty") && init?.method === "POST") {
        return new Response(
          JSON.stringify({
            session_id: "sess-2",
            token: "tok-2",
          }),
          { status: 201 },
        );
      }
      return new Response("", { status: 404 });
    });

    const sandbox = makeSandbox();
    const pty = await sandbox.createPty({ command: "/bin/bash" });
    const socket = MockWebSocket.instances[0];

    await pty.sendInput("pwd\n");
    await pty.resize(120, 40);

    expect(socket.sent[1]).toEqual(Buffer.from([0x00, 0x70, 0x77, 0x64, 0x0a]));
    expect(socket.sent[2]).toEqual(Buffer.from([0x01, 0x00, 0x78, 0x00, 0x28]));
  });

  it("disconnects and reconnects the same PTY handle", async () => {
    mockFetch((url, init) => {
      if (url.endsWith("/api/v1/pty") && init?.method === "POST") {
        return new Response(
          JSON.stringify({
            session_id: "sess-3",
            token: "tok-3",
          }),
          { status: 201 },
        );
      }
      return new Response("", { status: 404 });
    });

    const sandbox = makeSandbox();
    const pty = await sandbox.createPty({ command: "/bin/bash" });
    const firstSocket = MockWebSocket.instances[0];

    pty.disconnect();
    expect(firstSocket.readyState).toBe(MockWebSocket.CLOSED);

    await pty.connect();
    const secondSocket = MockWebSocket.instances[1];
    expect(secondSocket).not.toBe(firstSocket);
    expect(secondSocket.sent[0]).toEqual(Buffer.from([0x02]));

    secondSocket.emitClose(1000, "exit:0");
    await expect(pty.wait()).resolves.toBe(0);
  });

  it("kills a PTY session through the HTTP API", async () => {
    mockFetch((url, init) => {
      if (url.endsWith("/api/v1/pty") && init?.method === "POST") {
        return new Response(
          JSON.stringify({
            session_id: "sess-4",
            token: "tok-4",
          }),
          { status: 201 },
        );
      }
      if (url.endsWith("/api/v1/pty/sess-4") && init?.method === "DELETE") {
        return new Response("", { status: 200 });
      }
      return new Response("", { status: 404 });
    });

    const sandbox = makeSandbox();
    const pty = await sandbox.createPty({ command: "/bin/bash" });

    await expect(pty.kill()).resolves.toBeUndefined();
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "https://sbx-1.sandbox.tensorlake.ai/api/v1/pty/sess-4",
      expect.objectContaining({
        method: "DELETE",
      }),
    );
  });
});
