import { deflateSync } from "node:zlib";
import { SandboxError } from "./errors.js";
import { TunnelByteStream, withTimeout } from "./tunnel.js";

const SECURITY_TYPE_NONE = 1;
const SECURITY_TYPE_VNC_AUTH = 2;
const ENCODING_RAW = 0;
const ENCODING_DESKTOP_SIZE = -223;

const BUTTON_LEFT_MASK = 1;
const BUTTON_MIDDLE_MASK = 1 << 1;
const BUTTON_RIGHT_MASK = 1 << 2;
const BUTTON_SCROLL_UP_MASK = 1 << 3;
const BUTTON_SCROLL_DOWN_MASK = 1 << 4;

const PNG_SIGNATURE = Buffer.from([
  0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
]);

const CRC32_TABLE = buildCrc32Table();

export type MouseButton = "left" | "middle" | "right";

export interface ConnectDesktopOptions {
  /** Remote VNC port. Defaults to `5901`. */
  port?: number;
  /** Optional VNC password for classic VNC authentication. */
  password?: string;
  /** Whether to request a shared VNC session. Defaults to `true`. */
  shared?: boolean;
  /** Seconds to wait while opening the desktop session. Defaults to `10`. */
  connectTimeout?: number;
}

export interface DesktopPointerOptions {
  button?: MouseButton;
  x?: number;
  y?: number;
}

export interface DesktopDoubleClickOptions extends DesktopPointerOptions {
  delayMs?: number;
}

interface DesktopConnectRequest extends ConnectDesktopOptions {
  baseUrl: string;
  wsHeaders: Record<string, string>;
}

interface FramebufferUpdateOutcome {
  kind: "framebufferUpdate";
  sawResize: boolean;
  sawRaw: boolean;
}

interface BellOutcome {
  kind: "bell";
}

interface ServerCutTextOutcome {
  kind: "serverCutText";
}

type ServerMessageOutcome =
  | FramebufferUpdateOutcome
  | BellOutcome
  | ServerCutTextOutcome;

export interface DesktopTransport {
  readExactly(length: number): Promise<Buffer>;
  writeAll(data: Uint8Array): Promise<void>;
  close(): Promise<void>;
}

export class DesktopSession<T extends DesktopTransport> {
  readonly transport: T;
  width: number;
  height: number;

  private pixelFormat: PixelFormat;
  private framebuffer: Uint8Array;
  private pointerX = 0;
  private pointerY = 0;
  private buttonMask = 0;
  private framebufferVersion = 0;
  private closed = false;
  private updateLoopError: Error | null = null;
  private readonly updateSignal = createDeferredSignal();
  private updateLoopPromise: Promise<void> | null = null;

  constructor(options: {
    transport: T;
    width: number;
    height: number;
    pixelFormat: PixelFormat;
    framebuffer: Uint8Array;
  }) {
    this.transport = options.transport;
    this.width = options.width;
    this.height = options.height;
    this.pixelFormat = options.pixelFormat;
    this.framebuffer = options.framebuffer;
  }

  static async connect<T extends DesktopTransport>(
    transport: T,
    password?: string,
    shared = true,
  ): Promise<DesktopSession<T>> {
    const serverVersion = await ProtocolVersion.read(transport);
    const clientVersion = serverVersion.negotiated();
    await transport.writeAll(Buffer.from(clientVersion.render(), "ascii"));

    await negotiateSecurity(transport, clientVersion, password);
    await transport.writeAll(Uint8Array.of(shared ? 1 : 0));

    const init = await ServerInit.read(transport);
    if (!init.pixelFormat.trueColor) {
      throw new SandboxError(
        "desktop sessions require a true-color VNC pixel format",
      );
    }

    const pixelFormat = PixelFormat.preferred();
    await sendSetPixelFormat(transport, pixelFormat);
    await sendSetEncodings(transport, [ENCODING_RAW, ENCODING_DESKTOP_SIZE]);

    const session = new DesktopSession({
      transport,
      width: init.width,
      height: init.height,
      pixelFormat,
      framebuffer: allocateFramebuffer(init.width, init.height),
    });
    session.startFramebufferUpdates();
    return session;
  }

  async close(): Promise<void> {
    this.closed = true;
    this.updateSignal.resolve();
    await this.transport.close();
    await this.updateLoopPromise;
  }

  async screenshot(timeoutSeconds = 5): Promise<Uint8Array> {
    await this.waitForFramebufferVersion(
      1,
      timeoutSeconds,
      `timed out waiting for initial desktop framebuffer after ${timeoutSeconds.toFixed(2)}s`,
    );
    return encodePng(this.width, this.height, this.framebuffer);
  }

  getFrameVersion(): number {
    return this.framebufferVersion;
  }

  async screenshotAfter(frameVersion: number, timeoutSeconds = 1): Promise<Uint8Array> {
    const minimumVersion = validateNonNegativeInteger(frameVersion, "frame version") + 1;

    if (this.framebufferVersion < minimumVersion) {
      await this.waitForFramebufferVersion(
        minimumVersion,
        timeoutSeconds,
        `timed out waiting for a fresher desktop framebuffer after ${timeoutSeconds.toFixed(2)}s`,
      );
    }

    return encodePng(this.width, this.height, this.framebuffer);
  }

  async moveMouse(x: number, y: number): Promise<void> {
    const nextX = validateCoordinate(x, "mouse x coordinate");
    const nextY = validateCoordinate(y, "mouse y coordinate");
    this.ensurePointerInBounds(nextX, nextY);
    this.pointerX = nextX;
    this.pointerY = nextY;
    await sendPointerEvent(this.transport, this.buttonMask, nextX, nextY);
  }

  async mousePress(options?: DesktopPointerOptions): Promise<void> {
    const button = options?.button ?? "left";
    await this.moveIfRequested(options?.x, options?.y);
    this.buttonMask |= buttonMask(button);
    await sendPointerEvent(
      this.transport,
      this.buttonMask,
      this.pointerX,
      this.pointerY,
    );
  }

  async mouseRelease(options?: DesktopPointerOptions): Promise<void> {
    const button = options?.button ?? "left";
    await this.moveIfRequested(options?.x, options?.y);
    this.buttonMask &= ~buttonMask(button);
    await sendPointerEvent(
      this.transport,
      this.buttonMask,
      this.pointerX,
      this.pointerY,
    );
  }

  async click(options?: DesktopPointerOptions): Promise<void> {
    await this.mousePress(options);
    await this.mouseRelease({ button: options?.button ?? "left" });
  }

  async doubleClick(options?: DesktopDoubleClickOptions): Promise<void> {
    const button = options?.button ?? "left";
    const delayMs = options?.delayMs ?? 50;
    validateNonNegativeInteger(delayMs, "double click delay");

    await this.click({ button, x: options?.x, y: options?.y });
    if (delayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
    await this.click({ button });
  }

  async scroll(steps: number, x?: number, y?: number): Promise<void> {
    const normalizedSteps = validateInteger(steps, "scroll steps");
    await this.moveIfRequested(x, y);

    if (normalizedSteps === 0) {
      return;
    }

    const wheelMask =
      normalizedSteps > 0 ? BUTTON_SCROLL_UP_MASK : BUTTON_SCROLL_DOWN_MASK;
    const stepCount = Math.abs(normalizedSteps);
    for (let index = 0; index < stepCount; index += 1) {
      await sendPointerEvent(
        this.transport,
        this.buttonMask | wheelMask,
        this.pointerX,
        this.pointerY,
      );
      await sendPointerEvent(
        this.transport,
        this.buttonMask,
        this.pointerX,
        this.pointerY,
      );
    }
  }

  async keyDown(key: string): Promise<void> {
    await sendKeyEvent(this.transport, true, keysymFromKeyName(key));
  }

  async keyUp(key: string): Promise<void> {
    await sendKeyEvent(this.transport, false, keysymFromKeyName(key));
  }

  async press(keys: string | string[]): Promise<void> {
    const parts = Array.isArray(keys) ? keys : [keys];
    if (parts.length === 0) {
      throw new SandboxError("desktop press requires at least one key");
    }

    const keysyms = parts.map((part) => keysymFromKeyName(part));
    if (keysyms.length === 1) {
      await sendKeyEvent(this.transport, true, keysyms[0]);
      await sendKeyEvent(this.transport, false, keysyms[0]);
      return;
    }

    for (const keysym of keysyms.slice(0, -1)) {
      await sendKeyEvent(this.transport, true, keysym);
    }

    const last = keysyms[keysyms.length - 1];
    await sendKeyEvent(this.transport, true, last);
    await sendKeyEvent(this.transport, false, last);

    for (const keysym of keysyms.slice(0, -1).reverse()) {
      await sendKeyEvent(this.transport, false, keysym);
    }
  }

  async typeText(text: string): Promise<void> {
    for (const char of text) {
      const keysym = keysymFromChar(char);
      await sendKeyEvent(this.transport, true, keysym);
      await sendKeyEvent(this.transport, false, keysym);
    }
  }

  private async moveIfRequested(x?: number, y?: number): Promise<void> {
    if (x == null && y == null) {
      return;
    }
    if (x == null || y == null) {
      throw new SandboxError(
        "desktop pointer actions require both x and y when specifying coordinates",
      );
    }
    await this.moveMouse(x, y);
  }

  private ensurePointerInBounds(x: number, y: number): void {
    if (this.width > 0 && x >= this.width) {
      throw new SandboxError(
        `mouse x coordinate ${x} is outside desktop width ${this.width}`,
      );
    }
    if (this.height > 0 && y >= this.height) {
      throw new SandboxError(
        `mouse y coordinate ${y} is outside desktop height ${this.height}`,
      );
    }
  }

  private startFramebufferUpdates(): void {
    if (this.updateLoopPromise) {
      return;
    }

    this.updateLoopPromise = this.runFramebufferUpdateLoop().catch((error: unknown) => {
      if (this.closed) {
        return;
      }
      this.updateLoopError = normalizeError(error);
      this.updateSignal.resolve();
    });
  }

  private async runFramebufferUpdateLoop(): Promise<void> {
    let incremental = false;

    while (!this.closed) {
      await sendFramebufferUpdateRequest(
        this.transport,
        incremental,
        0,
        0,
        this.width,
        this.height,
      );

      const outcome = await this.readUntilFramebufferUpdate();
      if (outcome.sawResize && !outcome.sawRaw) {
        incremental = false;
        continue;
      }

      if (outcome.sawRaw) {
        this.framebufferVersion += 1;
        this.updateSignal.resolve();
      }

      incremental = true;
    }
  }

  private async readUntilFramebufferUpdate(): Promise<FramebufferUpdateOutcome> {
    while (true) {
      const outcome = await this.readServerMessage();
      if (outcome.kind === "framebufferUpdate") {
        return outcome;
      }
    }
  }

  private async waitForFramebufferVersion(
    minimumVersion: number,
    timeoutSeconds: number,
    timeoutMessage: string,
  ): Promise<void> {
    const timeoutMs = secondsToMillis(timeoutSeconds);
    const deadline = Date.now() + timeoutMs;

    while (this.framebufferVersion < minimumVersion) {
      if (this.updateLoopError) {
        throw this.updateLoopError;
      }
      if (this.closed) {
        throw new SandboxError("desktop session is closed");
      }

      const observedVersion = this.framebufferVersion;
      const waitForUpdate = this.updateSignal.wait();
      if (this.framebufferVersion !== observedVersion) {
        continue;
      }

      const remainingMs = Math.max(0, deadline - Date.now());
      if (remainingMs === 0) {
        break;
      }

      await withTimeout(remainingMs, () => waitForUpdate, timeoutMessage);
    }

    if (this.framebufferVersion < minimumVersion) {
      throw new SandboxError(timeoutMessage);
    }
  }

  private async readServerMessage(): Promise<ServerMessageOutcome> {
    const messageType = await readU8(this.transport);
    if (messageType === 0) {
      return this.readFramebufferUpdate();
    }
    if (messageType === 1) {
      await this.readSetColorMapEntries();
      return { kind: "bell" };
    }
    if (messageType === 2) {
      return { kind: "bell" };
    }
    if (messageType === 3) {
      await this.readServerCutText();
      return { kind: "serverCutText" };
    }
    throw new SandboxError(`unsupported VNC server message type ${messageType}`);
  }

  private async readFramebufferUpdate(): Promise<FramebufferUpdateOutcome> {
    await readU8(this.transport);
    const rectangleCount = await readU16(this.transport);
    let sawResize = false;
    let sawRaw = false;

    for (let index = 0; index < rectangleCount; index += 1) {
      const x = await readU16(this.transport);
      const y = await readU16(this.transport);
      const width = await readU16(this.transport);
      const height = await readU16(this.transport);
      const encoding = await readI32(this.transport);

      if (encoding === ENCODING_RAW) {
        const bytesPerPixel = this.pixelFormat.bytesPerPixel();
        const length = width * height * bytesPerPixel;
        const data = await this.transport.readExactly(length);
        this.blitRawRectangle(x, y, width, height, data);
        sawRaw = true;
        continue;
      }

      if (encoding === ENCODING_DESKTOP_SIZE) {
        this.resizeFramebuffer(width, height);
        sawResize = true;
        continue;
      }

      throw new SandboxError(`unsupported VNC rectangle encoding ${encoding}`);
    }

    return { kind: "framebufferUpdate", sawResize, sawRaw };
  }

  private async readSetColorMapEntries(): Promise<void> {
    await readU8(this.transport);
    await readU16(this.transport);
    const colorCount = await readU16(this.transport);
    await this.transport.readExactly(colorCount * 6);
    throw new SandboxError(
      "desktop sessions do not support color-map VNC pixel formats",
    );
  }

  private async readServerCutText(): Promise<void> {
    await this.transport.readExactly(3);
    const length = await readU32(this.transport);
    await this.transport.readExactly(length);
  }

  private resizeFramebuffer(width: number, height: number): void {
    this.width = width;
    this.height = height;
    this.framebuffer = allocateFramebuffer(width, height);
    this.pointerX = width > 0 ? Math.min(this.pointerX, width - 1) : 0;
    this.pointerY = height > 0 ? Math.min(this.pointerY, height - 1) : 0;
  }

  private blitRawRectangle(
    x: number,
    y: number,
    width: number,
    height: number,
    data: Uint8Array,
  ): void {
    if (x + width > this.width || y + height > this.height) {
      throw new SandboxError("desktop raw rectangle exceeds framebuffer bounds");
    }

    const bytesPerPixel = this.pixelFormat.bytesPerPixel();
    for (let row = 0; row < height; row += 1) {
      for (let col = 0; col < width; col += 1) {
        const srcIndex = (row * width + col) * bytesPerPixel;
        const rgba = this.pixelFormat.decodePixel(
          data.subarray(srcIndex, srcIndex + bytesPerPixel),
        );
        const dstIndex = ((y + row) * this.width + x + col) * 4;
        this.framebuffer.set(rgba, dstIndex);
      }
    }
  }
}

export class Desktop {
  private session: DesktopSession<TunnelByteStream>;
  private readonly connectRequest: DesktopConnectRequest & {
    port: number;
    shared: boolean;
    connectTimeout: number;
  };
  private operationChain: Promise<void> = Promise.resolve();
  private reconnectPromise: Promise<void> | null = null;
  private closed = false;

  private constructor(
    session: DesktopSession<TunnelByteStream>,
    connectRequest: DesktopConnectRequest & {
      port: number;
      shared: boolean;
      connectTimeout: number;
    },
  ) {
    this.session = session;
    this.connectRequest = connectRequest;
  }

  static async connect(options: DesktopConnectRequest): Promise<Desktop> {
    const connectRequest = normalizeDesktopConnectRequest(options);
    const session = await openDesktopSession(connectRequest);
    return new Desktop(session, connectRequest);
  }

  get width(): number {
    return this.session.width;
  }

  get height(): number {
    return this.session.height;
  }

  async close(): Promise<void> {
    this.closed = true;
    await this.enqueue(() => this.session.close());
  }

  async screenshot(timeout = 5): Promise<Uint8Array> {
    return this.enqueue(() => this.captureScreenshot(timeout));
  }

  getFrameVersion(): number {
    return this.session.getFrameVersion();
  }

  async screenshotAfter(frameVersion: number, timeout = 1): Promise<Uint8Array> {
    return this.enqueue(() => this.captureScreenshotAfter(frameVersion, timeout));
  }

  async moveMouse(x: number, y: number): Promise<void> {
    await this.enqueue(() => this.session.moveMouse(x, y));
  }

  async mousePress(options?: DesktopPointerOptions): Promise<void> {
    await this.enqueue(() => this.session.mousePress(options));
  }

  async mouseRelease(options?: DesktopPointerOptions): Promise<void> {
    await this.enqueue(() => this.session.mouseRelease(options));
  }

  async click(options?: DesktopPointerOptions): Promise<void> {
    await this.enqueue(() => this.session.click(options));
  }

  async doubleClick(options?: DesktopDoubleClickOptions): Promise<void> {
    await this.enqueue(() => this.session.doubleClick(options));
  }

  async leftClick(x?: number, y?: number): Promise<void> {
    await this.click({ button: "left", x, y });
  }

  async middleClick(x?: number, y?: number): Promise<void> {
    await this.click({ button: "middle", x, y });
  }

  async rightClick(x?: number, y?: number): Promise<void> {
    await this.click({ button: "right", x, y });
  }

  async scroll(steps: number, x?: number, y?: number): Promise<void> {
    await this.enqueue(() => this.session.scroll(steps, x, y));
  }

  async scrollUp(steps = 1, x?: number, y?: number): Promise<void> {
    await this.scroll(Math.abs(steps), x, y);
  }

  async scrollDown(steps = 1, x?: number, y?: number): Promise<void> {
    await this.scroll(-Math.abs(steps), x, y);
  }

  async keyDown(key: string): Promise<void> {
    await this.enqueue(() => this.session.keyDown(key));
  }

  async keyUp(key: string): Promise<void> {
    await this.enqueue(() => this.session.keyUp(key));
  }

  async press(keys: string | string[]): Promise<void> {
    await this.enqueue(() => this.session.press(keys));
  }

  async typeText(text: string): Promise<void> {
    await this.enqueue(() => this.session.typeText(text));
  }

  private enqueue<T>(operation: () => Promise<T>): Promise<T> {
    const run = this.operationChain.catch(() => {}).then(operation);
    this.operationChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  private async captureScreenshot(timeout: number): Promise<Uint8Array> {
    try {
      return await this.session.screenshot(timeout);
    } catch (error) {
      if (!isReconnectableDesktopScreenshotError(error) || this.closed) {
        throw error;
      }

      await this.reconnect();
      return this.session.screenshot(timeout);
    }
  }

  private async captureScreenshotAfter(
    frameVersion: number,
    timeout: number,
  ): Promise<Uint8Array> {
    try {
      return await this.session.screenshotAfter(frameVersion, timeout);
    } catch (error) {
      if (!isReconnectableDesktopScreenshotError(error) || this.closed) {
        throw error;
      }

      await this.reconnect();
      return this.session.screenshot(timeout);
    }
  }

  private async reconnect(): Promise<void> {
    if (this.reconnectPromise) {
      return this.reconnectPromise;
    }

    this.reconnectPromise = this.performReconnect().finally(() => {
      this.reconnectPromise = null;
    });
    return this.reconnectPromise;
  }

  private async performReconnect(): Promise<void> {
    const previousSession = this.session;
    await previousSession.close().catch(() => {});
    this.session = await openDesktopSession(this.connectRequest);
  }
}

class ProtocolVersion {
  readonly major: number;
  readonly minor: number;

  constructor(major: number, minor: number) {
    this.major = major;
    this.minor = minor;
  }

  static async read(transport: DesktopTransport): Promise<ProtocolVersion> {
    const raw = await transport.readExactly(12);
    const text = raw.toString("ascii");
    const trimmed = text.endsWith("\n") ? text.slice(0, -1) : text;
    const match = /^RFB (\d{3})\.(\d{3})$/.exec(trimmed);
    if (!match) {
      throw new SandboxError(`invalid VNC protocol banner \`${text}\``);
    }
    return new ProtocolVersion(Number.parseInt(match[1], 10), Number.parseInt(match[2], 10));
  }

  negotiated(): ProtocolVersion {
    if (this.major !== 3 || this.minor >= 8) {
      return new ProtocolVersion(3, 8);
    }
    if (this.minor >= 7) {
      return new ProtocolVersion(3, 7);
    }
    return new ProtocolVersion(3, 3);
  }

  render(): string {
    return `RFB ${String(this.major).padStart(3, "0")}.${String(this.minor).padStart(3, "0")}\n`;
  }
}

class PixelFormat {
  readonly bitsPerPixel: number;
  readonly depth: number;
  readonly bigEndian: boolean;
  readonly trueColor: boolean;
  readonly redMax: number;
  readonly greenMax: number;
  readonly blueMax: number;
  readonly redShift: number;
  readonly greenShift: number;
  readonly blueShift: number;

  constructor(options: {
    bitsPerPixel: number;
    depth: number;
    bigEndian: boolean;
    trueColor: boolean;
    redMax: number;
    greenMax: number;
    blueMax: number;
    redShift: number;
    greenShift: number;
    blueShift: number;
  }) {
    this.bitsPerPixel = options.bitsPerPixel;
    this.depth = options.depth;
    this.bigEndian = options.bigEndian;
    this.trueColor = options.trueColor;
    this.redMax = options.redMax;
    this.greenMax = options.greenMax;
    this.blueMax = options.blueMax;
    this.redShift = options.redShift;
    this.greenShift = options.greenShift;
    this.blueShift = options.blueShift;
  }

  static preferred(): PixelFormat {
    return new PixelFormat({
      bitsPerPixel: 32,
      depth: 24,
      bigEndian: false,
      trueColor: true,
      redMax: 255,
      greenMax: 255,
      blueMax: 255,
      redShift: 16,
      greenShift: 8,
      blueShift: 0,
    });
  }

  static parse(bytes: Uint8Array): PixelFormat {
    if (bytes.length !== 16) {
      throw new SandboxError("invalid VNC pixel format payload length");
    }
    return new PixelFormat({
      bitsPerPixel: bytes[0],
      depth: bytes[1],
      bigEndian: bytes[2] !== 0,
      trueColor: bytes[3] !== 0,
      redMax: Buffer.from(bytes.subarray(4, 6)).readUInt16BE(0),
      greenMax: Buffer.from(bytes.subarray(6, 8)).readUInt16BE(0),
      blueMax: Buffer.from(bytes.subarray(8, 10)).readUInt16BE(0),
      redShift: bytes[10],
      greenShift: bytes[11],
      blueShift: bytes[12],
    });
  }

  bytesPerPixel(): number {
    return this.bitsPerPixel / 8;
  }

  encode(): Uint8Array {
    const bytes = Buffer.alloc(16);
    bytes[0] = this.bitsPerPixel;
    bytes[1] = this.depth;
    bytes[2] = this.bigEndian ? 1 : 0;
    bytes[3] = this.trueColor ? 1 : 0;
    bytes.writeUInt16BE(this.redMax, 4);
    bytes.writeUInt16BE(this.greenMax, 6);
    bytes.writeUInt16BE(this.blueMax, 8);
    bytes[10] = this.redShift;
    bytes[11] = this.greenShift;
    bytes[12] = this.blueShift;
    return bytes;
  }

  decodePixel(bytes: Uint8Array): Uint8Array {
    if (bytes.length !== this.bytesPerPixel()) {
      throw new SandboxError("desktop pixel buffer has an unexpected size");
    }

    let value = 0;
    if (this.bigEndian) {
      for (const byte of bytes) {
        value = (value << 8) | byte;
      }
    } else {
      for (let index = 0; index < bytes.length; index += 1) {
        value |= bytes[index] << (index * 8);
      }
    }

    const red = scaleChannel((value >> this.redShift) & this.redMax, this.redMax);
    const green = scaleChannel(
      (value >> this.greenShift) & this.greenMax,
      this.greenMax,
    );
    const blue = scaleChannel((value >> this.blueShift) & this.blueMax, this.blueMax);
    return Uint8Array.of(red, green, blue, 255);
  }
}

class ServerInit {
  readonly width: number;
  readonly height: number;
  readonly pixelFormat: PixelFormat;

  constructor(width: number, height: number, pixelFormat: PixelFormat) {
    this.width = width;
    this.height = height;
    this.pixelFormat = pixelFormat;
  }

  static async read(transport: DesktopTransport): Promise<ServerInit> {
    const width = await readU16(transport);
    const height = await readU16(transport);
    const pixelFormat = PixelFormat.parse(await transport.readExactly(16));
    const nameLength = await readU32(transport);
    await transport.readExactly(nameLength);
    return new ServerInit(width, height, pixelFormat);
  }
}

async function negotiateSecurity(
  transport: DesktopTransport,
  version: ProtocolVersion,
  password?: string,
): Promise<number> {
  let securityTypes: number[];

  if (version.minor === 3) {
    const securityType = await readU32(transport);
    if (securityType === 0) {
      const reasonLength = await readU32(transport);
      const reason = (await transport.readExactly(reasonLength)).toString("utf8");
      throw new SandboxError(`VNC security negotiation failed: ${reason}`);
    }
    securityTypes = [securityType];
  } else {
    const count = await readU8(transport);
    if (count === 0) {
      const reasonLength = await readU32(transport);
      const reason = (await transport.readExactly(reasonLength)).toString("utf8");
      throw new SandboxError(`VNC security negotiation failed: ${reason}`);
    }
    securityTypes = [...(await transport.readExactly(count))];
  }

  let selected: number;
  if (password != null && securityTypes.includes(SECURITY_TYPE_VNC_AUTH)) {
    selected = SECURITY_TYPE_VNC_AUTH;
  } else if (securityTypes.includes(SECURITY_TYPE_NONE)) {
    selected = SECURITY_TYPE_NONE;
  } else if (securityTypes.includes(SECURITY_TYPE_VNC_AUTH)) {
    throw new SandboxError(
      "VNC server requires password authentication but no password was provided",
    );
  } else {
    throw new SandboxError(
      `unsupported VNC security types advertised by server: [${securityTypes.join(", ")}]`,
    );
  }

  if (version.minor >= 7) {
    await transport.writeAll(Uint8Array.of(selected));
  }

  if (selected === SECURITY_TYPE_VNC_AUTH) {
    if (password == null) {
      throw new SandboxError(
        "VNC server requires password authentication but no password was provided",
      );
    }
    const challenge = await transport.readExactly(16);
    const response = encryptVncChallenge(Buffer.from(password, "utf8"), challenge);
    await transport.writeAll(response);
    await readSecurityResult(transport, version.minor >= 8);
  } else if (version.minor >= 8) {
    await readSecurityResult(transport, true);
  }

  return selected;
}

function encryptVncChallenge(password: Uint8Array, challenge: Uint8Array): Uint8Array {
  if (challenge.length !== 16) {
    throw new SandboxError("VNC authentication challenge must be 16 bytes");
  }

  const key = Buffer.alloc(8);
  for (let index = 0; index < Math.min(password.length, 8); index += 1) {
    key[index] = reverseBits(password[index]);
  }

  const roundKeys = buildDesRoundKeys(key);
  const output = Buffer.alloc(16);
  for (let blockIndex = 0; blockIndex < 2; blockIndex += 1) {
    const start = blockIndex * 8;
    const encrypted = encryptDesBlock(challenge.subarray(start, start + 8), roundKeys);
    output.set(encrypted, start);
  }
  return output;
}

function reverseBits(value: number): number {
  let reversed = 0;
  for (let bit = 0; bit < 8; bit += 1) {
    reversed |= ((value >> bit) & 1) << (7 - bit);
  }
  return reversed;
}

async function readSecurityResult(
  transport: DesktopTransport,
  hasReasonString: boolean,
): Promise<void> {
  const status = await readU32(transport);
  if (status === 0) {
    return;
  }

  let reason: string;
  if (hasReasonString) {
    const reasonLength = await readU32(transport);
    reason = (await transport.readExactly(reasonLength)).toString("utf8");
  } else if (status === 1) {
    reason = "authentication failed";
  } else {
    reason = `security handshake failed with status ${status}`;
  }

  throw new SandboxError(`VNC security negotiation failed: ${reason}`);
}

async function sendSetPixelFormat(
  transport: DesktopTransport,
  pixelFormat: PixelFormat,
): Promise<void> {
  const message = Buffer.alloc(20);
  message[0] = 0;
  message.set(pixelFormat.encode(), 4);
  await transport.writeAll(message);
}

async function sendSetEncodings(
  transport: DesktopTransport,
  encodings: number[],
): Promise<void> {
  const message = Buffer.alloc(4 + encodings.length * 4);
  message[0] = 2;
  message.writeUInt16BE(encodings.length, 2);
  for (let index = 0; index < encodings.length; index += 1) {
    message.writeInt32BE(encodings[index], 4 + index * 4);
  }
  await transport.writeAll(message);
}

async function sendFramebufferUpdateRequest(
  transport: DesktopTransport,
  incremental: boolean,
  x: number,
  y: number,
  width: number,
  height: number,
): Promise<void> {
  const message = Buffer.alloc(10);
  message[0] = 3;
  message[1] = incremental ? 1 : 0;
  message.writeUInt16BE(x, 2);
  message.writeUInt16BE(y, 4);
  message.writeUInt16BE(width, 6);
  message.writeUInt16BE(height, 8);
  await transport.writeAll(message);
}

async function sendPointerEvent(
  transport: DesktopTransport,
  buttonMask: number,
  x: number,
  y: number,
): Promise<void> {
  const message = Buffer.alloc(6);
  message[0] = 5;
  message[1] = buttonMask;
  message.writeUInt16BE(x, 2);
  message.writeUInt16BE(y, 4);
  await transport.writeAll(message);
}

async function sendKeyEvent(
  transport: DesktopTransport,
  down: boolean,
  keysym: number,
): Promise<void> {
  const message = Buffer.alloc(8);
  message[0] = 4;
  message[1] = down ? 1 : 0;
  message.writeUInt32BE(keysym, 4);
  await transport.writeAll(message);
}

async function readU8(transport: DesktopTransport): Promise<number> {
  return (await transport.readExactly(1))[0];
}

async function readU16(transport: DesktopTransport): Promise<number> {
  return (await transport.readExactly(2)).readUInt16BE(0);
}

async function readU32(transport: DesktopTransport): Promise<number> {
  return (await transport.readExactly(4)).readUInt32BE(0);
}

async function readI32(transport: DesktopTransport): Promise<number> {
  return (await transport.readExactly(4)).readInt32BE(0);
}

function scaleChannel(value: number, max: number): number {
  if (max === 0) {
    throw new SandboxError("invalid VNC pixel format with zero channel range");
  }
  return Math.trunc((value * 255) / max);
}

function allocateFramebuffer(width: number, height: number): Uint8Array {
  return new Uint8Array(width * height * 4);
}

function normalizeDesktopConnectRequest(
  options: DesktopConnectRequest,
): DesktopConnectRequest & {
  port: number;
  shared: boolean;
  connectTimeout: number;
} {
  return {
    ...options,
    port: validatePort(options.port ?? 5901, "desktop port"),
    shared: options.shared ?? true,
    connectTimeout: options.connectTimeout ?? 10,
  };
}

async function openDesktopSession(
  options: DesktopConnectRequest & {
    port: number;
    shared: boolean;
    connectTimeout: number;
  },
): Promise<DesktopSession<TunnelByteStream>> {
  const connectTimeoutMs = secondsToMillis(options.connectTimeout);
  const state: { transport?: TunnelByteStream } = {};

  try {
    return await withTimeout(
      connectTimeoutMs,
      async () => {
        state.transport = await TunnelByteStream.connect({
          baseUrl: options.baseUrl,
          wsHeaders: options.wsHeaders,
          remotePort: options.port,
          connectTimeoutMs,
        });
        return DesktopSession.connect(
          state.transport,
          options.password,
          options.shared,
        );
      },
      `timed out while connecting desktop session after ${options.connectTimeout.toFixed(2)}s`,
    );
  } catch (error) {
    if (state.transport) {
      await state.transport.close().catch(() => {});
    }
    throw error;
  }
}

function isReconnectableDesktopScreenshotError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }

  const message = error.message.toLowerCase();
  return (
    message.includes("desktop tunnel closed unexpectedly") ||
    message.includes("desktop tunnel is not connected") ||
    message.includes("connection closed") ||
    message.includes("econnreset") ||
    message.includes("timed out waiting for initial desktop framebuffer") ||
    message.includes("timed out while connecting tunnel websocket") ||
    message.includes("tunnel websocket closed before opening") ||
    message.includes("tunnel websocket handshake failed")
  );
}

function createDeferredSignal(): {
  resolve(): void;
  wait(): Promise<void>;
} {
  let resolveCurrent!: () => void;
  let promise = new Promise<void>((resolve) => {
    resolveCurrent = resolve;
  });

  return {
    resolve() {
      resolveCurrent();
      promise = new Promise<void>((resolve) => {
        resolveCurrent = resolve;
      });
    },
    wait() {
      return promise;
    },
  };
}

function normalizeError(error: unknown): Error {
  return error instanceof Error ? error : new SandboxError(String(error));
}

function buttonMask(button: MouseButton | string): number {
  const normalized = button.trim().toLowerCase();
  if (normalized === "left") return BUTTON_LEFT_MASK;
  if (normalized === "middle") return BUTTON_MIDDLE_MASK;
  if (normalized === "right") return BUTTON_RIGHT_MASK;
  throw new SandboxError(
    `unsupported mouse button \`${button}\`; expected left, middle, or right`,
  );
}

function keysymFromKeyName(key: string): number {
  const trimmed = key.trim();
  if (trimmed.length === 0) {
    throw new SandboxError("desktop key name cannot be empty");
  }

  if ([...trimmed].length === 1) {
    return keysymFromChar(trimmed);
  }

  const normalized = trimmed.toLowerCase();
  const special = SPECIAL_KEYSYMS.get(normalized);
  if (special != null) {
    return special;
  }

  const functionMatch = /^f([1-9]|1[0-2])$/.exec(normalized);
  if (functionMatch) {
    return 0xffbd + Number.parseInt(functionMatch[1], 10);
  }

  throw new SandboxError(`unsupported desktop key \`${trimmed}\``);
}

function keysymFromChar(char: string): number {
  const codePoint = char.codePointAt(0);
  if (codePoint == null) {
    throw new SandboxError("desktop key name cannot be empty");
  }
  if (char === "\n" || char === "\r") return 0xff0d;
  if (char === "\t") return 0xff09;
  if (char === "\b") return 0xff08;
  if (codePoint >= 0x20 && codePoint <= 0x7e) return codePoint;
  if (codePoint < 0x20 || (codePoint >= 0x7f && codePoint <= 0x9f)) {
    throw new SandboxError(
      `unsupported control character U+${codePoint.toString(16).toUpperCase().padStart(4, "0")} for desktop typing`,
    );
  }
  return 0x0100_0000 | codePoint;
}

function encodePng(width: number, height: number, rgba: Uint8Array): Uint8Array {
  const stride = width * 4;
  const raw = Buffer.alloc((stride + 1) * height);
  for (let row = 0; row < height; row += 1) {
    const srcOffset = row * stride;
    const dstOffset = row * (stride + 1);
    raw[dstOffset] = 0;
    raw.set(rgba.subarray(srcOffset, srcOffset + stride), dstOffset + 1);
  }

  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(width, 0);
  ihdr.writeUInt32BE(height, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  ihdr[10] = 0;
  ihdr[11] = 0;
  ihdr[12] = 0;

  const idat = deflateSync(raw);
  return Buffer.concat([
    PNG_SIGNATURE,
    pngChunk("IHDR", ihdr),
    pngChunk("IDAT", idat),
    pngChunk("IEND", Buffer.alloc(0)),
  ]);
}

function pngChunk(type: string, data: Buffer): Buffer {
  const chunkType = Buffer.from(type, "ascii");
  const length = Buffer.alloc(4);
  length.writeUInt32BE(data.length, 0);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(Buffer.concat([chunkType, data])), 0);
  return Buffer.concat([length, chunkType, data, crc]);
}

function crc32(data: Uint8Array): number {
  let crc = 0xffff_ffff;
  for (const byte of data) {
    crc = CRC32_TABLE[(crc ^ byte) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffff_ffff) >>> 0;
}

function buildCrc32Table(): Uint32Array {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index += 1) {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1) !== 0 ? 0xedb8_8320 ^ (value >>> 1) : value >>> 1;
    }
    table[index] = value >>> 0;
  }
  return table;
}

function validateCoordinate(value: number, label: string): number {
  return validateIntegerInRange(value, label, 0, 0xffff);
}

function validatePort(value: number, label: string): number {
  return validateIntegerInRange(value, label, 1, 65535);
}

function validateIntegerInRange(
  value: number,
  label: string,
  min: number,
  max: number,
): number {
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new SandboxError(`${label} must be an integer between ${min} and ${max}, got ${value}`);
  }
  return value;
}

function validateInteger(value: number, label: string): number {
  if (!Number.isInteger(value)) {
    throw new SandboxError(`${label} must be an integer, got ${value}`);
  }
  return value;
}

function validateNonNegativeInteger(value: number, label: string): number {
  if (!Number.isInteger(value) || value < 0) {
    throw new SandboxError(`${label} must be a non-negative integer, got ${value}`);
  }
  return value;
}

function secondsToMillis(seconds: number): number {
  if (!Number.isFinite(seconds) || seconds < 0) {
    throw new SandboxError(`timeout must be >= 0 seconds, got ${seconds}`);
  }
  return Math.round(seconds * 1000);
}

const SPECIAL_KEYSYMS = new Map<string, number>([
  ["enter", 0xff0d],
  ["tab", 0xff09],
  ["escape", 0xff1b],
  ["backspace", 0xff08],
  ["delete", 0xffff],
  ["space", 0x0020],
  ["up", 0xff52],
  ["down", 0xff54],
  ["left", 0xff51],
  ["right", 0xff53],
  ["home", 0xff50],
  ["end", 0xff57],
  ["pageup", 0xff55],
  ["pagedown", 0xff56],
  ["page_up", 0xff55],
  ["page_down", 0xff56],
  ["shift", 0xffe1],
  ["ctrl", 0xffe3],
  ["control", 0xffe3],
  ["alt", 0xffe9],
  ["meta", 0xffe7],
]);

const DES_INITIAL_PERMUTATION = [
  58, 50, 42, 34, 26, 18, 10, 2,
  60, 52, 44, 36, 28, 20, 12, 4,
  62, 54, 46, 38, 30, 22, 14, 6,
  64, 56, 48, 40, 32, 24, 16, 8,
  57, 49, 41, 33, 25, 17, 9, 1,
  59, 51, 43, 35, 27, 19, 11, 3,
  61, 53, 45, 37, 29, 21, 13, 5,
  63, 55, 47, 39, 31, 23, 15, 7,
];

const DES_FINAL_PERMUTATION = [
  40, 8, 48, 16, 56, 24, 64, 32,
  39, 7, 47, 15, 55, 23, 63, 31,
  38, 6, 46, 14, 54, 22, 62, 30,
  37, 5, 45, 13, 53, 21, 61, 29,
  36, 4, 44, 12, 52, 20, 60, 28,
  35, 3, 43, 11, 51, 19, 59, 27,
  34, 2, 42, 10, 50, 18, 58, 26,
  33, 1, 41, 9, 49, 17, 57, 25,
];

const DES_EXPANSION = [
  32, 1, 2, 3, 4, 5,
  4, 5, 6, 7, 8, 9,
  8, 9, 10, 11, 12, 13,
  12, 13, 14, 15, 16, 17,
  16, 17, 18, 19, 20, 21,
  20, 21, 22, 23, 24, 25,
  24, 25, 26, 27, 28, 29,
  28, 29, 30, 31, 32, 1,
];

const DES_P_PERMUTATION = [
  16, 7, 20, 21,
  29, 12, 28, 17,
  1, 15, 23, 26,
  5, 18, 31, 10,
  2, 8, 24, 14,
  32, 27, 3, 9,
  19, 13, 30, 6,
  22, 11, 4, 25,
];

const DES_PC1 = [
  57, 49, 41, 33, 25, 17, 9,
  1, 58, 50, 42, 34, 26, 18,
  10, 2, 59, 51, 43, 35, 27,
  19, 11, 3, 60, 52, 44, 36,
  63, 55, 47, 39, 31, 23, 15,
  7, 62, 54, 46, 38, 30, 22,
  14, 6, 61, 53, 45, 37, 29,
  21, 13, 5, 28, 20, 12, 4,
];

const DES_PC2 = [
  14, 17, 11, 24, 1, 5,
  3, 28, 15, 6, 21, 10,
  23, 19, 12, 4, 26, 8,
  16, 7, 27, 20, 13, 2,
  41, 52, 31, 37, 47, 55,
  30, 40, 51, 45, 33, 48,
  44, 49, 39, 56, 34, 53,
  46, 42, 50, 36, 29, 32,
];

const DES_ROTATIONS = [
  1, 1, 2, 2, 2, 2, 2, 2,
  1, 2, 2, 2, 2, 2, 2, 1,
];

const DES_SBOXES = [
  [
    14, 4, 13, 1, 2, 15, 11, 8, 3, 10, 6, 12, 5, 9, 0, 7,
    0, 15, 7, 4, 14, 2, 13, 1, 10, 6, 12, 11, 9, 5, 3, 8,
    4, 1, 14, 8, 13, 6, 2, 11, 15, 12, 9, 7, 3, 10, 5, 0,
    15, 12, 8, 2, 4, 9, 1, 7, 5, 11, 3, 14, 10, 0, 6, 13,
  ],
  [
    15, 1, 8, 14, 6, 11, 3, 4, 9, 7, 2, 13, 12, 0, 5, 10,
    3, 13, 4, 7, 15, 2, 8, 14, 12, 0, 1, 10, 6, 9, 11, 5,
    0, 14, 7, 11, 10, 4, 13, 1, 5, 8, 12, 6, 9, 3, 2, 15,
    13, 8, 10, 1, 3, 15, 4, 2, 11, 6, 7, 12, 0, 5, 14, 9,
  ],
  [
    10, 0, 9, 14, 6, 3, 15, 5, 1, 13, 12, 7, 11, 4, 2, 8,
    13, 7, 0, 9, 3, 4, 6, 10, 2, 8, 5, 14, 12, 11, 15, 1,
    13, 6, 4, 9, 8, 15, 3, 0, 11, 1, 2, 12, 5, 10, 14, 7,
    1, 10, 13, 0, 6, 9, 8, 7, 4, 15, 14, 3, 11, 5, 2, 12,
  ],
  [
    7, 13, 14, 3, 0, 6, 9, 10, 1, 2, 8, 5, 11, 12, 4, 15,
    13, 8, 11, 5, 6, 15, 0, 3, 4, 7, 2, 12, 1, 10, 14, 9,
    10, 6, 9, 0, 12, 11, 7, 13, 15, 1, 3, 14, 5, 2, 8, 4,
    3, 15, 0, 6, 10, 1, 13, 8, 9, 4, 5, 11, 12, 7, 2, 14,
  ],
  [
    2, 12, 4, 1, 7, 10, 11, 6, 8, 5, 3, 15, 13, 0, 14, 9,
    14, 11, 2, 12, 4, 7, 13, 1, 5, 0, 15, 10, 3, 9, 8, 6,
    4, 2, 1, 11, 10, 13, 7, 8, 15, 9, 12, 5, 6, 3, 0, 14,
    11, 8, 12, 7, 1, 14, 2, 13, 6, 15, 0, 9, 10, 4, 5, 3,
  ],
  [
    12, 1, 10, 15, 9, 2, 6, 8, 0, 13, 3, 4, 14, 7, 5, 11,
    10, 15, 4, 2, 7, 12, 9, 5, 6, 1, 13, 14, 0, 11, 3, 8,
    9, 14, 15, 5, 2, 8, 12, 3, 7, 0, 4, 10, 1, 13, 11, 6,
    4, 3, 2, 12, 9, 5, 15, 10, 11, 14, 1, 7, 6, 0, 8, 13,
  ],
  [
    4, 11, 2, 14, 15, 0, 8, 13, 3, 12, 9, 7, 5, 10, 6, 1,
    13, 0, 11, 7, 4, 9, 1, 10, 14, 3, 5, 12, 2, 15, 8, 6,
    1, 4, 11, 13, 12, 3, 7, 14, 10, 15, 6, 8, 0, 5, 9, 2,
    6, 11, 13, 8, 1, 4, 10, 7, 9, 5, 0, 15, 14, 2, 3, 12,
  ],
  [
    13, 2, 8, 4, 6, 15, 11, 1, 10, 9, 3, 14, 5, 0, 12, 7,
    1, 15, 13, 8, 10, 3, 7, 4, 12, 5, 6, 11, 0, 14, 9, 2,
    7, 11, 4, 1, 9, 12, 14, 2, 0, 6, 10, 13, 15, 3, 5, 8,
    2, 1, 14, 7, 4, 10, 8, 13, 15, 12, 9, 0, 3, 5, 6, 11,
  ],
];

function buildDesRoundKeys(key: Uint8Array): bigint[] {
  const keyBlock = bytesToBigInt(key);
  const permuted = permuteBits(keyBlock, DES_PC1, 64);
  let c = Number((permuted >> 28n) & 0x0fff_ffffn);
  let d = Number(permuted & 0x0fff_ffffn);

  const roundKeys: bigint[] = [];
  for (const rotation of DES_ROTATIONS) {
    c = rotateLeft28(c, rotation);
    d = rotateLeft28(d, rotation);
    const combined = (BigInt(c) << 28n) | BigInt(d);
    roundKeys.push(permuteBits(combined, DES_PC2, 56));
  }
  return roundKeys;
}

function encryptDesBlock(block: Uint8Array, roundKeys: bigint[]): Uint8Array {
  let value = permuteBits(bytesToBigInt(block), DES_INITIAL_PERMUTATION, 64);
  let left = Number((value >> 32n) & 0xffff_ffffn);
  let right = Number(value & 0xffff_ffffn);

  for (const roundKey of roundKeys) {
    const nextLeft = right;
    const nextRight = (left ^ feistel(right, roundKey)) >>> 0;
    left = nextLeft >>> 0;
    right = nextRight;
  }

  value = (BigInt(right) << 32n) | BigInt(left);
  return bigIntToBytes(permuteBits(value, DES_FINAL_PERMUTATION, 64), 8);
}

function feistel(right: number, roundKey: bigint): number {
  const expanded = permuteBits(BigInt(right >>> 0), DES_EXPANSION, 32) ^ roundKey;
  let output = 0;
  for (let index = 0; index < 8; index += 1) {
    const shift = BigInt((7 - index) * 6);
    const chunk = Number((expanded >> shift) & 0x3fn);
    const row = ((chunk & 0x20) >> 4) | (chunk & 0x01);
    const column = (chunk >> 1) & 0x0f;
    output = (output << 4) | DES_SBOXES[index][row * 16 + column];
  }
  return Number(permuteBits(BigInt(output >>> 0), DES_P_PERMUTATION, 32)) >>> 0;
}

function rotateLeft28(value: number, shift: number): number {
  const masked = value & 0x0fff_ffff;
  return ((masked << shift) | (masked >>> (28 - shift))) & 0x0fff_ffff;
}

function permuteBits(input: bigint, table: number[], inputBits: number): bigint {
  let output = 0n;
  for (const position of table) {
    const shift = BigInt(inputBits - position);
    output = (output << 1n) | ((input >> shift) & 1n);
  }
  return output;
}

function bytesToBigInt(bytes: Uint8Array): bigint {
  let value = 0n;
  for (const byte of bytes) {
    value = (value << 8n) | BigInt(byte);
  }
  return value;
}

function bigIntToBytes(value: bigint, length: number): Uint8Array {
  const out = Buffer.alloc(length);
  let remaining = value;
  for (let index = length - 1; index >= 0; index -= 1) {
    out[index] = Number(remaining & 0xffn);
    remaining >>= 8n;
  }
  return out;
}
