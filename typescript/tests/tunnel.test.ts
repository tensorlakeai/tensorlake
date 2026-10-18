import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { TcpTunnel } from "../src/tunnel.js";
import { loadNativeSandboxBinding } from "../src/native-sandbox.js";
import { SandboxError } from "../src/errors.js";
import { installNativeStub, clearNativeStub } from "./native-stub.js";

describe("TCP tunnel Rust bridge", () => {
  let listen: ReturnType<typeof vi.fn>;
  let close: ReturnType<typeof vi.fn>;
  beforeEach(() => {
    installNativeStub();
    let stopped: () => void;
    const lifetime = new Promise<void>((resolve) => { stopped = resolve; });
    listen = vi.fn(async () => 40000);
    close = vi.fn(async () => stopped());
    loadNativeSandboxBinding().NativeTcpTunnel = class {
      listen = listen;
      close = close;
      waitClosed = () => lifetime;
    };
  });
  afterEach(clearNativeStub);

  it("passes sandbox routing and connection options to Rust", async () => {
    const sandbox = new Sandbox({ sandboxId: "sbx-local", proxyUrl: "http://localhost:9443", apiKey: "test-key" });
    const tunnel = await sandbox.createTunnel(5901, {localPort: 0, connectTimeout: 3});
    expect(listen).toHaveBeenCalledWith("http://localhost:9443",
      expect.objectContaining({ Host: "sbx-local.local", Authorization: "Bearer test-key" }),
      5901, "127.0.0.1", 0, 3);
    expect(tunnel.address()).toEqual({host: "127.0.0.1", port: 40000});
    expect(tunnel.remotePort).toBe(5901);
    await Promise.all([tunnel.close(), tunnel.close()]);
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("preserves the default local port", async () => {
    const tunnel = await TcpTunnel.listen({baseUrl: "https://example.test", wsHeaders: {}, remotePort: 9000});
    expect(listen).toHaveBeenCalledWith("https://example.test", {}, 9000, "127.0.0.1", 9000, 10);
    await tunnel.close();
  });

  it("translates native bind errors", async () => {
    listen.mockRejectedValue(new Error(JSON.stringify({category: "client", message: "failed to bind local tunnel listener"})));
    await expect(TcpTunnel.listen({baseUrl: "http://localhost", wsHeaders: {}, remotePort: 9000})).rejects.toBeInstanceOf(SandboxError);
  });

  it("rejects invalid public arguments before entering native code", async () => {
    for (const options of [{remotePort: 0}, {remotePort: 65536}, {remotePort: 1.5}, {localPort: -1}, {connectTimeout: -1}]) {
      await expect(TcpTunnel.listen({baseUrl: "http://localhost", wsHeaders: {}, remotePort: 9000, ...options})).rejects.toBeInstanceOf(SandboxError);
    }
    expect(listen).not.toHaveBeenCalled();
  });
});
