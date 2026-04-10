import * as net from "node:net";
import { SandboxConnectionError, SandboxError } from "./errors.js";
import WebSocket, { type RawData } from "ws";

const DEFAULT_TUNNEL_CONNECT_TIMEOUT_MS = 10_000;

export interface CreateTunnelOptions {
  /** Local host/interface to bind. Defaults to `127.0.0.1`. */
  localHost?: string;
  /** Local port to bind. Defaults to the remote port. Use `0` for an ephemeral port. */
  localPort?: number;
  /** Seconds to wait for each websocket tunnel connection. Defaults to `10`. */
  connectTimeout?: number;
}

export interface TunnelAddress {
  host: string;
  port: number;
}

interface TunnelWebSocketOptions {
  baseUrl: string;
  wsHeaders: Record<string, string>;
  remotePort: number;
  connectTimeoutMs?: number;
}

interface PendingRead {
  length: number;
  resolve: (chunk: Buffer) => void;
  reject: (error: Error) => void;
}

interface ActiveRelay {
  localSocket: net.Socket;
  websocket: WebSocket | null;
}

export async function openTunnelWebSocket(
  options: TunnelWebSocketOptions,
): Promise<WebSocket> {
  const wsUrl = buildTunnelWsUrl(options.baseUrl, options.remotePort);
  const timeoutMs = options.connectTimeoutMs ?? DEFAULT_TUNNEL_CONNECT_TIMEOUT_MS;

  return new Promise<WebSocket>((resolve, reject) => {
    const socket = new WebSocket(wsUrl, {
      headers: options.wsHeaders,
    });

    let settled = false;
    const timer = setTimeout(() => {
      fail(
        new SandboxError(
          `timed out while connecting tunnel websocket after ${(timeoutMs / 1000).toFixed(2)}s`,
        ),
      );
    }, timeoutMs);

    const cleanup = () => {
      clearTimeout(timer);
      socket.removeListener("open", onOpen);
      socket.removeListener("error", onError);
      socket.removeListener("close", onCloseBeforeOpen);
      socket.removeListener("unexpected-response", onUnexpectedResponse);
    };

    const settle = (callback: () => void) => {
      if (settled) return;
      settled = true;
      cleanup();
      callback();
    };

    const fail = (error: Error) => {
      settle(() => {
        reject(error);
      });
    };

    const onOpen = () => {
      settle(() => resolve(socket));
    };

    const onError = (error: Error) => {
      fail(new SandboxConnectionError(error.message));
    };

    const onCloseBeforeOpen = (code: number, reason: Buffer) => {
      const closeReason =
        reason.length > 0 ? reason.toString("utf8") : "no reason";
      fail(
        new SandboxError(
          `tunnel websocket closed before opening: ${code} ${closeReason}`,
        ),
      );
    };

    const onUnexpectedResponse = (
      _request: unknown,
      response: { statusCode?: number; statusMessage?: string },
    ) => {
      const status = response.statusCode ?? 0;
      const statusMessage = response.statusMessage?.trim();
      fail(
        new SandboxError(
          `tunnel websocket handshake failed with HTTP ${status}${statusMessage ? ` ${statusMessage}` : ""}`,
        ),
      );
    };

    socket.on("open", onOpen);
    socket.on("error", onError);
    socket.on("close", onCloseBeforeOpen);
    socket.on("unexpected-response", onUnexpectedResponse);
  });
}

export class TunnelByteStream {
  private readonly socket: WebSocket;
  private readBuffer: Buffer = Buffer.alloc(0);
  private readonly pendingReads: PendingRead[] = [];
  private closeError: Error | null = null;
  private closePromise: Promise<void> | null = null;

  constructor(socket: WebSocket) {
    this.socket = socket;

    socket.on("message", (message: RawData, isBinary: boolean) => {
      if (!isBinary) {
        this.fail(
          new SandboxError("desktop tunnel received unexpected text frame"),
        );
        return;
      }
      this.pushBytes(normalizeWebSocketData(message));
    });

    socket.on("ping", (data: Buffer) => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.pong(data, false, () => {});
      }
    });

    socket.on("close", (_code: number, reason: Buffer) => {
      const closeReason =
        reason.length > 0 ? reason.toString("utf8") : "desktop tunnel closed unexpectedly";
      this.fail(new SandboxError(closeReason));
    });

    socket.on("error", (error: Error) => {
      this.fail(new SandboxConnectionError(error.message));
    });
  }

  static async connect(options: TunnelWebSocketOptions): Promise<TunnelByteStream> {
    const socket = await openTunnelWebSocket(options);
    return new TunnelByteStream(socket);
  }

  async readExactly(length: number): Promise<Buffer> {
    if (length < 0) {
      throw new SandboxError(`read length must be >= 0, got ${length}`);
    }
    if (length === 0) {
      return Buffer.alloc(0);
    }
    if (this.readBuffer.length >= length) {
      const chunk = this.readBuffer.subarray(0, length);
      this.readBuffer = this.readBuffer.subarray(length);
      return chunk;
    }
    if (this.closeError) {
      throw this.closeError;
    }
    return new Promise<Buffer>((resolve, reject) => {
      this.pendingReads.push({ length, resolve, reject });
    });
  }

  async writeAll(data: Uint8Array): Promise<void> {
    if (this.closeError) {
      throw this.closeError;
    }
    if (this.socket.readyState !== WebSocket.OPEN) {
      throw new SandboxError("desktop tunnel is not connected");
    }
    const payload = Buffer.from(data);
    await new Promise<void>((resolve, reject) => {
      this.socket.send(payload, { binary: true }, (error?: Error) => {
        if (error) {
          reject(new SandboxConnectionError(error.message));
          return;
        }
        resolve();
      });
    });
  }

  async close(): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }
    if (
      this.socket.readyState === WebSocket.CLOSED ||
      this.socket.readyState === WebSocket.CLOSING
    ) {
      this.closePromise = Promise.resolve();
      return this.closePromise;
    }

    this.closePromise = new Promise<void>((resolve) => {
      const onClose = () => {
        this.socket.removeListener("close", onClose);
        resolve();
      };
      this.socket.on("close", onClose);
      this.socket.close();
    });
    return this.closePromise;
  }

  private pushBytes(chunk: Buffer): void {
    this.readBuffer =
      this.readBuffer.length === 0
        ? chunk
        : Buffer.from(Buffer.concat([this.readBuffer, chunk]));
    this.flushPendingReads();
  }

  private flushPendingReads(): void {
    while (this.pendingReads.length > 0) {
      const next = this.pendingReads[0];
      if (this.readBuffer.length < next.length) {
        break;
      }
      const chunk = this.readBuffer.subarray(0, next.length);
      this.readBuffer = this.readBuffer.subarray(next.length);
      this.pendingReads.shift();
      next.resolve(chunk);
    }
  }

  private fail(error: Error): void {
    if (this.closeError) {
      return;
    }
    this.closeError = error;
    while (this.pendingReads.length > 0) {
      const pending = this.pendingReads.shift();
      pending?.reject(error);
    }
  }
}

export class TcpTunnel {
  readonly remotePort: number;
  readonly localHost: string;
  readonly localPort: number;

  private readonly baseUrl: string;
  private readonly wsHeaders: Record<string, string>;
  private readonly server: net.Server;
  private readonly connectTimeoutMs: number;
  private readonly activeRelays = new Set<ActiveRelay>();
  private closePromise: Promise<void> | null = null;

  private constructor(options: {
    baseUrl: string;
    wsHeaders: Record<string, string>;
    remotePort: number;
    localHost: string;
    localPort: number;
    server: net.Server;
    connectTimeoutMs: number;
  }) {
    this.baseUrl = options.baseUrl;
    this.wsHeaders = options.wsHeaders;
    this.remotePort = options.remotePort;
    this.localHost = options.localHost;
    this.localPort = options.localPort;
    this.server = options.server;
    this.connectTimeoutMs = options.connectTimeoutMs;
  }

  static async listen(options: {
    baseUrl: string;
    wsHeaders: Record<string, string>;
    remotePort: number;
    localHost?: string;
    localPort?: number;
    connectTimeout?: number;
  }): Promise<TcpTunnel> {
    const remotePort = validatePort(options.remotePort, "remote port");
    const localHost = options.localHost ?? "127.0.0.1";
    const localPort = validatePort(
      options.localPort ?? remotePort,
      "local port",
      true,
    );
    const connectTimeoutMs = secondsToMillis(options.connectTimeout ?? 10);

    const server = net.createServer();
    await listenServer(server, localPort, localHost);

    const address = server.address();
    if (!address || typeof address === "string") {
      server.close();
      throw new SandboxError("failed to determine bound tunnel address");
    }

    const tunnel = new TcpTunnel({
      baseUrl: options.baseUrl,
      wsHeaders: options.wsHeaders,
      remotePort,
      localHost,
      localPort: address.port,
      server,
      connectTimeoutMs,
    });

    server.on("connection", (localSocket) => {
      void tunnel.handleConnection(localSocket);
    });

    return tunnel;
  }

  address(): TunnelAddress {
    return { host: this.localHost, port: this.localPort };
  }

  async close(): Promise<void> {
    if (this.closePromise) {
      return this.closePromise;
    }

    for (const relay of this.activeRelays) {
      relay.localSocket.destroy();
      relay.websocket?.close();
    }

    this.closePromise = new Promise<void>((resolve, reject) => {
      this.server.close((error) => {
        if (error) {
          reject(new SandboxError(`failed to close tunnel listener: ${error.message}`));
          return;
        }
        resolve();
      });
    });
    return this.closePromise;
  }

  private async handleConnection(localSocket: net.Socket): Promise<void> {
    localSocket.setNoDelay(true);

    const relay: ActiveRelay = { localSocket, websocket: null };
    this.activeRelays.add(relay);

    try {
      relay.websocket = await openTunnelWebSocket({
        baseUrl: this.baseUrl,
        wsHeaders: this.wsHeaders,
        remotePort: this.remotePort,
        connectTimeoutMs: this.connectTimeoutMs,
      });
      await relaySocket(localSocket, relay.websocket);
    } catch (error) {
      localSocket.destroy(
        error instanceof Error ? error : new Error(String(error)),
      );
    } finally {
      this.activeRelays.delete(relay);
    }
  }
}

export function buildTunnelWsUrl(baseUrl: string, remotePort: number): string {
  const url = new URL(baseUrl);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  url.pathname = "/api/v1/tunnels/tcp";
  url.search = `port=${encodeURIComponent(String(remotePort))}`;
  return url.toString();
}

function normalizeWebSocketData(message: RawData): Buffer {
  if (Buffer.isBuffer(message)) return message;
  if (Array.isArray(message)) {
    return Buffer.concat(message.map((part) => Buffer.from(part)));
  }
  return Buffer.from(message);
}

async function relaySocket(localSocket: net.Socket, websocket: WebSocket): Promise<void> {
  return new Promise<void>((resolve) => {
    let settled = false;

    const finish = () => {
      if (settled) return;
      settled = true;
      cleanup();
      resolve();
    };

    const cleanup = () => {
      localSocket.removeListener("data", onLocalData);
      localSocket.removeListener("end", onLocalEnd);
      localSocket.removeListener("close", onLocalClose);
      localSocket.removeListener("error", onLocalError);
      websocket.removeListener("message", onWsMessage);
      websocket.removeListener("close", onWsClose);
      websocket.removeListener("error", onWsError);
      websocket.removeListener("ping", onWsPing);
    };

    const onLocalData = (chunk: Buffer) => {
      if (websocket.readyState !== WebSocket.OPEN) {
        localSocket.destroy();
        return;
      }

      websocket.send(chunk, { binary: true }, (error?: Error) => {
        if (error) {
          localSocket.destroy(error);
        }
      });
    };

    const onLocalEnd = () => {
      if (websocket.readyState === WebSocket.OPEN) {
        websocket.close();
      }
    };

    const onLocalClose = () => {
      if (
        websocket.readyState === WebSocket.OPEN ||
        websocket.readyState === WebSocket.CONNECTING
      ) {
        websocket.close();
      }
      finish();
    };

    const onLocalError = () => {
      websocket.close();
      finish();
    };

    const onWsMessage = (message: RawData, isBinary: boolean) => {
      if (!isBinary) {
        localSocket.destroy(
          new SandboxError("received unexpected text frame from tunnel"),
        );
        websocket.close();
        return;
      }

      const payload = normalizeWebSocketData(message);
      if (!localSocket.destroyed) {
        localSocket.write(payload);
      }
    };

    const onWsClose = () => {
      if (!localSocket.destroyed) {
        localSocket.end();
      }
      finish();
    };

    const onWsError = (error: Error) => {
      localSocket.destroy(error);
      finish();
    };

    const onWsPing = (data: Buffer) => {
      if (websocket.readyState === WebSocket.OPEN) {
        websocket.pong(data, false, () => {});
      }
    };

    localSocket.on("data", onLocalData);
    localSocket.on("end", onLocalEnd);
    localSocket.on("close", onLocalClose);
    localSocket.on("error", onLocalError);
    websocket.on("message", onWsMessage);
    websocket.on("close", onWsClose);
    websocket.on("error", onWsError);
    websocket.on("ping", onWsPing);
  });
}

async function listenServer(
  server: net.Server,
  localPort: number,
  localHost: string,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.removeListener("listening", onListening);
      reject(
        new SandboxError(
          `failed to bind local tunnel listener on ${localHost}:${localPort}: ${error.message}`,
        ),
      );
    };

    const onListening = () => {
      server.removeListener("error", onError);
      resolve();
    };

    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(localPort, localHost);
  });
}

function validatePort(port: number, label: string, allowZero = false): number {
  if (!Number.isInteger(port)) {
    throw new SandboxError(`${label} must be an integer, got ${port}`);
  }
  if (allowZero && port === 0) {
    return port;
  }
  if (port < 1 || port > 65535) {
    throw new SandboxError(`${label} must be between 1 and 65535, got ${port}`);
  }
  return port;
}

function secondsToMillis(seconds: number): number {
  if (!Number.isFinite(seconds) || seconds < 0) {
    throw new SandboxError(`timeout must be >= 0 seconds, got ${seconds}`);
  }
  return Math.round(seconds * 1000);
}

export async function withTimeout<T>(
  timeoutMs: number,
  operation: () => Promise<T>,
  timeoutMessage: string,
): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new SandboxError(timeoutMessage));
    }, timeoutMs);

    void operation()
      .then((value) => {
        clearTimeout(timer);
        resolve(value);
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}
