import { afterEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import {
  __setNativeSandboxBindingForTest,
  type NativeSandboxBinding,
  type NativeSandboxProxyClient,
} from "../src/native-sandbox.js";
import { SandboxNotFoundError, RemoteAPIError } from "../src/errors.js";

/**
 * Verifies the Rust-backed proxy path: the rewired Sandbox methods call the
 * native proxy client, transform its JSON/bytes/events correctly, surface the
 * trace id, and translate structured native errors into typed SDK errors.
 */

type ProxyOverrides = Partial<NativeSandboxProxyClient>;

function installFakeBinding(overrides: ProxyOverrides = {}): {
  proxy: Record<string, ReturnType<typeof vi.fn>>;
} {
  const proxy: Record<string, ReturnType<typeof vi.fn>> = {
    baseUrl: vi.fn(() => "http://localhost:9443"),
    startProcess: vi.fn(),
    listProcesses: vi.fn(),
    getProcess: vi.fn(),
    killProcess: vi.fn(async () => "trace-kill"),
    restartProcess: vi.fn(),
    sendSignal: vi.fn(),
    writeStdin: vi.fn(async () => "trace-stdin"),
    closeStdin: vi.fn(async () => "trace-close"),
    getStdout: vi.fn(),
    getStderr: vi.fn(),
    getOutput: vi.fn(),
    followStdout: vi.fn(),
    followStderr: vi.fn(),
    followOutput: vi.fn(),
    runProcess: vi.fn(),
    runProcessStreaming: vi.fn(),
    readFile: vi.fn(),
    writeFile: vi.fn(async () => "trace-write"),
    uploadFile: vi.fn(async () => "trace-upload"),
    deleteFile: vi.fn(async () => "trace-delete"),
    listDirectory: vi.fn(),
    createPtySession: vi.fn(),
    deletePtySession: vi.fn(async () => "trace-pty-delete"),
    health: vi.fn(),
    info: vi.fn(),
  };
  Object.assign(proxy, overrides);

  const binding: NativeSandboxBinding = {
    validateManagedName: vi.fn(),
    NativeSandboxClient: class {
      connectProxy() {
        return proxy as unknown as NativeSandboxProxyClient;
      }
    } as unknown as NativeSandboxBinding["NativeSandboxClient"],
    NativeSandboxProxyClient: class {
      constructor() {
        return proxy as unknown as NativeSandboxProxyClient;
      }
    } as unknown as NativeSandboxBinding["NativeSandboxProxyClient"],
  };
  __setNativeSandboxBindingForTest(binding);
  return { proxy };
}

function makeSandbox(): Sandbox {
  return new Sandbox({ sandboxId: "sbx-test", proxyUrl: "http://localhost:9443" });
}

describe("Sandbox native proxy path", () => {
  afterEach(() => {
    __setNativeSandboxBindingForTest(undefined);
    vi.restoreAllMocks();
  });

  it("reads a file as bytes and surfaces the trace id", async () => {
    const { proxy } = installFakeBinding({
      readFile: vi.fn(async (path: string) => {
        expect(path).toBe("/tmp/x.txt");
        return { traceId: "tr-read", data: Buffer.from("hello") };
      }),
    });
    const sbx = makeSandbox();
    const data = await sbx.readFile("/tmp/x.txt");
    expect(new TextDecoder().decode(data)).toBe("hello");
    expect(data.traceId).toBe("tr-read");
    expect(proxy.readFile).toHaveBeenCalledOnce();
    sbx.close();
  });

  it("writes a file by forwarding a Buffer to the native client", async () => {
    const { proxy } = installFakeBinding();
    const sbx = makeSandbox();
    await sbx.writeFile("/tmp/out.txt", new TextEncoder().encode("data"));
    const [path, content] = proxy.writeFile.mock.calls[0];
    expect(path).toBe("/tmp/out.txt");
    expect(Buffer.isBuffer(content)).toBe(true);
    expect((content as Buffer).toString()).toBe("data");
    sbx.close();
  });

  it("parses getProcess JSON from snake_case into camelCase", async () => {
    const { proxy } = installFakeBinding({
      getProcess: vi.fn(async (process: string) => ({
        traceId: "tr-proc",
        json: JSON.stringify({
          pid: Number(process),
          status: "running",
          command: "sleep",
          args: ["1"],
          started_at: 123,
          stdin_writable: true,
        }),
      })),
    });
    const sbx = makeSandbox();
    const proc = await sbx.getProcess(42);
    expect(proxy.getProcess).toHaveBeenCalledWith("42");
    expect(proc.pid).toBe(42);
    // fromSnakeKeys maps started_at -> startedAt (and coerces the timestamp,
    // same as the previous undici path).
    expect(proc.startedAt).toBeInstanceOf(Date);
    expect(proc.stdinWritable).toBe(true);
    expect(proc.traceId).toBe("tr-proc");
    sbx.close();
  });

  it("assembles run() output from buffered run_process events", async () => {
    installFakeBinding({
      runProcess: vi.fn(async () => ({
        traceId: "tr-run",
        events: [
          JSON.stringify({ pid: 7, started_at: 1 }),
          JSON.stringify({ line: "hello", timestamp: 2 }),
          JSON.stringify({ line: "oops", stream: "stderr", timestamp: 3 }),
          JSON.stringify({ exit_code: 0 }),
        ],
      })),
    });
    const sbx = makeSandbox();
    const result = await sbx.run("echo hello");
    expect(result.stdout).toBe("hello");
    expect(result.stderr).toBe("oops");
    expect(result.exitCode).toBe(0);
    expect(result.traceId).toBe("tr-run");
    sbx.close();
  });

  it("streams followStdout events live via the emit bridge", async () => {
    const { proxy } = installFakeBinding({
      followStdout: vi.fn(async (_process: string, emit: (e: string) => void) => {
        emit(JSON.stringify({ line: "a", timestamp: 1 }));
        emit(JSON.stringify({ line: "b", timestamp: 2 }));
        return "tr-follow";
      }),
    });
    const sbx = makeSandbox();
    const lines: string[] = [];
    for await (const event of sbx.followStdout(7)) {
      lines.push(event.line);
    }
    expect(proxy.followStdout).toHaveBeenCalledWith("7", expect.any(Function));
    expect(lines).toEqual(["a", "b"]);
    sbx.close();
  });

  it("translates a proxy 404 into RemoteAPIError (not SandboxNotFoundError)", async () => {
    // A 404 from a data-plane op means the file/process is missing, not the
    // sandbox — proxy ops omit the not-found context so they stay RemoteAPIError,
    // matching the pre-shim behavior. SandboxNotFoundError is reserved for the
    // lifecycle client's sandbox lookups.
    installFakeBinding({
      getProcess: vi.fn(async () => {
        throw new Error(
          JSON.stringify({ category: "remote_api", status: 404, message: "missing" }),
        );
      }),
    });
    const sbx = makeSandbox();
    await expect(sbx.getProcess(1)).rejects.toBeInstanceOf(RemoteAPIError);
    await expect(sbx.getProcess(1)).rejects.not.toBeInstanceOf(SandboxNotFoundError);
    sbx.close();
  });

  it("translates a non-404 native error into RemoteAPIError with status", async () => {
    installFakeBinding({
      health: vi.fn(async () => {
        throw new Error(
          JSON.stringify({ category: "remote_api", status: 503, message: "down" }),
        );
      }),
    });
    const sbx = makeSandbox();
    await expect(sbx.health()).rejects.toMatchObject({
      name: "RemoteAPIError",
      statusCode: 503,
    });
    expect(RemoteAPIError).toBeDefined();
    sbx.close();
  });
});
