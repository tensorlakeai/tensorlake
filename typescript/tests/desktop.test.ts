import { inflateSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;

  static instances: MockWebSocket[] = [];
  static nextIncomingFrames: Buffer[] = [];

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
      setTimeout(() => {
        for (const frame of MockWebSocket.nextIncomingFrames) {
          this.emit("message", frame, true);
        }
        MockWebSocket.nextIncomingFrames = [];
      }, 0);
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

  private emit(event: string, ...args: any[]): void {
    for (const handler of this.handlers[event] ?? []) {
      handler(...args);
    }
  }
}

vi.mock("ws", () => ({
  default: MockWebSocket,
}));

class MockTransport {
  private readonly readBuffer: Buffer;
  private writeBuffer = Buffer.alloc(0);
  private offset = 0;
  closed = false;

  constructor(readBytes: Uint8Array) {
    this.readBuffer = Buffer.from(readBytes);
  }

  async readExactly(length: number): Promise<Buffer> {
    if (this.offset + length > this.readBuffer.length) {
      throw new Error("unexpected end of mock transport");
    }
    const out = this.readBuffer.subarray(this.offset, this.offset + length);
    this.offset += length;
    return out;
  }

  async writeAll(data: Uint8Array): Promise<void> {
    this.writeBuffer = Buffer.concat([this.writeBuffer, Buffer.from(data)]);
  }

  async close(): Promise<void> {
    this.closed = true;
  }

  get written(): Buffer {
    return this.writeBuffer;
  }
}

function serverInitBytes(width: number, height: number, trueColor: boolean): Buffer {
  const pixelFormat = Buffer.alloc(16);
  pixelFormat[0] = 32;
  pixelFormat[1] = 24;
  pixelFormat[2] = 0;
  pixelFormat[3] = trueColor ? 1 : 0;
  pixelFormat.writeUInt16BE(255, 4);
  pixelFormat.writeUInt16BE(255, 6);
  pixelFormat.writeUInt16BE(255, 8);
  pixelFormat[10] = 16;
  pixelFormat[11] = 8;
  pixelFormat[12] = 0;

  return Buffer.concat([
    u16(width),
    u16(height),
    pixelFormat,
    u32(4),
    Buffer.from("Test", "ascii"),
  ]);
}

function rawFramebufferUpdate(
  width: number,
  height: number,
  pixels: Array<[number, number, number, number]>,
): Buffer {
  const header = Buffer.concat([
    Buffer.from([0, 0]),
    u16(1),
    u16(0),
    u16(0),
    u16(width),
    u16(height),
    i32(0),
  ]);
  const payload = Buffer.concat(
    pixels.map(([r, g, b]) => Buffer.from([b, g, r, 0])),
  );
  return Buffer.concat([header, payload]);
}

function desktopSizeUpdate(width: number, height: number): Buffer {
  return Buffer.concat([
    Buffer.from([0, 0]),
    u16(1),
    u16(0),
    u16(0),
    u16(width),
    u16(height),
    i32(-223),
  ]);
}

function u16(value: number): Buffer {
  const out = Buffer.alloc(2);
  out.writeUInt16BE(value, 0);
  return out;
}

function u32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeUInt32BE(value, 0);
  return out;
}

function i32(value: number): Buffer {
  const out = Buffer.alloc(4);
  out.writeInt32BE(value, 0);
  return out;
}

function parsePng(png: Uint8Array): { width: number; height: number; rgba: Buffer } {
  const bytes = Buffer.from(png);
  expect(bytes.subarray(0, 8)).toEqual(
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
  );

  let offset = 8;
  let width = 0;
  let height = 0;
  const idat: Buffer[] = [];

  while (offset < bytes.length) {
    const length = bytes.readUInt32BE(offset);
    const type = bytes.subarray(offset + 4, offset + 8).toString("ascii");
    const data = bytes.subarray(offset + 8, offset + 8 + length);
    offset += 12 + length;

    if (type === "IHDR") {
      width = data.readUInt32BE(0);
      height = data.readUInt32BE(4);
    } else if (type === "IDAT") {
      idat.push(data);
    } else if (type === "IEND") {
      break;
    }
  }

  const raw = inflateSync(Buffer.concat(idat));
  const stride = width * 4;
  const rgba = Buffer.alloc(width * height * 4);
  for (let row = 0; row < height; row += 1) {
    const srcOffset = row * (stride + 1);
    expect(raw[srcOffset]).toBe(0);
    raw.copy(rgba, row * stride, srcOffset + 1, srcOffset + 1 + stride);
  }

  return { width, height, rgba };
}

function countFrame(buffer: Buffer, frame: Uint8Array): number {
  const needle = Buffer.from(frame);
  let count = 0;
  for (let index = 0; index <= buffer.length - needle.length; index += 1) {
    if (buffer.subarray(index, index + needle.length).equals(needle)) {
      count += 1;
    }
  }
  return count;
}

describe("DesktopSession", () => {
  let DesktopSession: typeof import("../src/desktop.js").DesktopSession;
  let Sandbox: typeof import("../src/sandbox.js").Sandbox;

  beforeEach(async () => {
    MockWebSocket.instances = [];
    MockWebSocket.nextIncomingFrames = [];
    vi.resetModules();
    ({ DesktopSession } = await import("../src/desktop.js"));
    ({ Sandbox } = await import("../src/sandbox.js"));
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  async function connectSession(readBytes: Uint8Array, password?: string) {
    const transport = new MockTransport(readBytes);
    const session = await DesktopSession.connect(transport, password, true);
    return { session, transport };
  }

  it("supports VNC handshake with no authentication", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 1]),
      u32(0),
      serverInitBytes(2, 1, true),
    ]);

    const { session, transport } = await connectSession(bytes);
    expect(transport.written.subarray(0, 12).toString("ascii")).toBe("RFB 003.008\n");
    expect(session.width).toBe(2);
    expect(session.height).toBe(1);
  });

  it("supports classic VNC password auth", async () => {
    const challenge = Buffer.alloc(16, 0x11);
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([2, 1, 2]),
      challenge,
      u32(0),
      serverInitBytes(1, 1, true),
    ]);

    const { transport } = await connectSession(bytes, "secret");
    expect(transport.written[12]).toBe(2);
    expect(
      transport.written.subarray(13, 29).toString("hex"),
    ).toBe("41d8f6b9c4ee03a241d8f6b9c4ee03a2");
  });

  it("rejects missing VNC password when required", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 2]),
    ]);

    await expect(connectSession(bytes)).rejects.toThrow(
      /requires password authentication/,
    );
  });

  it("decodes raw framebuffer updates into PNG screenshots", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 1]),
      u32(0),
      serverInitBytes(2, 1, true),
      rawFramebufferUpdate(2, 1, [
        [255, 0, 0, 255],
        [0, 255, 0, 255],
      ]),
    ]);

    const { session } = await connectSession(bytes);
    const png = await session.screenshot(1);
    const decoded = parsePng(png);
    expect(decoded.width).toBe(2);
    expect(decoded.height).toBe(1);
    expect(decoded.rgba.subarray(0, 8)).toEqual(
      Buffer.from([255, 0, 0, 255, 0, 255, 0, 255]),
    );
  });

  it("handles desktop resize updates before the next framebuffer", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 1]),
      u32(0),
      serverInitBytes(1, 1, true),
      desktopSizeUpdate(2, 1),
      rawFramebufferUpdate(2, 1, [
        [10, 20, 30, 255],
        [40, 50, 60, 255],
      ]),
    ]);

    const { session, transport } = await connectSession(bytes);
    await session.screenshot(1);
    expect(session.width).toBe(2);
    expect(session.height).toBe(1);
    expect(
      transport.written.filter((_, index, buffer) => buffer[index] === 3).length,
    ).toBeGreaterThan(0);
  });

  it("emits pointer events for clicks, double click, and scroll", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 1]),
      u32(0),
      serverInitBytes(10, 10, true),
    ]);

    const { session, transport } = await connectSession(bytes);
    await session.moveMouse(4, 5);
    await session.click({ button: "middle" });
    await session.doubleClick({ button: "left", x: 7, y: 8, delayMs: 0 });
    await session.click({ button: "right" });
    await session.scroll(2);
    await session.scroll(-1, 9, 6);

    const writes = transport.written;
    expect(countFrame(writes, [5, 2, 0, 4, 0, 5])).toBeGreaterThan(0);
    expect(countFrame(writes, [5, 1, 0, 7, 0, 8])).toBeGreaterThan(0);
    expect(countFrame(writes, [5, 4, 0, 7, 0, 8])).toBeGreaterThan(0);
    expect(countFrame(writes, [5, 8, 0, 7, 0, 8])).toBe(2);
    expect(countFrame(writes, [5, 16, 0, 9, 0, 6])).toBeGreaterThan(0);
  });

  it("emits key events for printable, named, combo, and unicode keys", async () => {
    const bytes = Buffer.concat([
      Buffer.from("RFB 003.008\n", "ascii"),
      Buffer.from([1, 1]),
      u32(0),
      serverInitBytes(10, 10, true),
    ]);

    const { session, transport } = await connectSession(bytes);
    await session.keyDown("a");
    await session.keyUp("a");
    await session.press(["ctrl", "c"]);
    await session.keyDown("enter");
    await session.typeText("Aé");

    const hex = transport.written.toString("hex");
    expect(hex).toContain(Buffer.from([4, 1, 0, 0, 0, 0, 0, 0x61]).toString("hex"));
    expect(hex).toContain(Buffer.from([4, 1, 0, 0, 0, 0, 0xff, 0xe3]).toString("hex"));
    expect(hex).toContain(Buffer.from([4, 1, 0, 0, 0, 0, 0xff, 0x0d]).toString("hex"));
    expect(hex).toContain(Buffer.from([4, 1, 0, 0, 0x01, 0x00, 0x00, 0xe9]).toString("hex"));
  });

  it("connects through Sandbox.connectDesktop with the tunnel websocket", async () => {
    MockWebSocket.nextIncomingFrames = [
      Buffer.concat([
        Buffer.from("RFB 003.008\n", "ascii"),
        Buffer.from([1, 1]),
        u32(0),
        serverInitBytes(2, 1, true),
        rawFramebufferUpdate(2, 1, [
          [1, 2, 3, 255],
          [4, 5, 6, 255],
        ]),
      ]),
    ];

    const sandbox = new Sandbox({
      sandboxId: "sbx-1",
      proxyUrl: "https://sandbox.tensorlake.ai",
      apiKey: "test-api-key",
    });

    const desktop = await sandbox.connectDesktop();
    const png = await desktop.screenshot();
    const socket = MockWebSocket.instances[0];

    expect(socket.url).toBe(
      "wss://sbx-1.sandbox.tensorlake.ai/api/v1/tunnels/tcp?port=5901",
    );
    expect(socket.options?.headers?.Authorization).toBe("Bearer test-api-key");
    expect(socket.sent[0].toString("ascii")).toBe("RFB 003.008\n");
    expect(parsePng(png).width).toBe(2);
  });
});
