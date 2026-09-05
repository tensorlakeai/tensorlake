import { build } from "esbuild";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  NativeWorkerClient,
  createWorkerBinding,
  releaseNativeHandle,
} from "../src/native-worker-client.js";
import {
  nativeEventStream,
  callNative,
  type NativeSandboxProxyClient,
} from "../src/native-sandbox.js";
import { RemoteAPIError } from "../src/errors.js";

let directory: string;
let workerPath: string;
beforeAll(async () => {
  directory = await mkdtemp(path.join(tmpdir(), "tensorlake-worker-test-"));
  workerPath = path.join(directory, "worker.cjs");
  await build({
    entryPoints: [path.resolve("src/native-worker.ts")],
    outfile: workerPath,
    bundle: true,
    platform: "node",
    format: "cjs",
    plugins: [
      {
        name: "native-fixture",
        setup(builder) {
          builder.onResolve({ filter: /native-binding\.js$/ }, () => ({
            path: path.resolve("tests/fixtures/native-worker-binding.ts"),
          }));
        },
      },
    ],
  });
});
afterAll(async () => {
  await rm(directory, { recursive: true, force: true });
});

const health = async (proxy: NativeSandboxProxyClient) =>
  JSON.parse((await proxy.health()).json);
const setup = () => {
  const resolve = vi.fn(() => workerPath);
  const binding = createWorkerBinding(new NativeWorkerClient(resolve));
  const client = new binding.NativeSandboxClient("http://localhost", "first");
  const proxy = client.connectProxy("http://localhost", "sandbox");
  return { binding, client, proxy, resolve };
};

describe("native worker boundary", () => {
  it("keeps constructors lazy and the event loop live during cold loading; shares clients across concurrent proxies", async () => {
    const { binding, client, proxy, resolve } = setup();
    const second = client.connectProxy("http://localhost", "second");
    expect(resolve).not.toHaveBeenCalled();
    let ticks = 0;
    const timer = setInterval(() => ticks++, 10);
    try {
      const [a, b] = await Promise.all([health(proxy), health(second)]);
      expect(ticks).toBeGreaterThan(10);
      expect(a.threadId).toBeGreaterThan(0);
      expect(a.clientId).toBe(b.clientId);
      expect(a.clients).toBe(1);
      const other = new binding.NativeSandboxClient(
        "http://localhost",
        "second",
      ).connectProxy("http://localhost", "third");
      expect((await health(other)).key).toBe("second");
      expect((await health(proxy)).key).toBe("first");
      expect(resolve).toHaveBeenCalledOnce();
      expect(await client.selectSandboxProxyUrl("id", "http://selected")).toBe(
        "http://selected",
      );
      await expect(binding.validateManagedName("")).rejects.toThrow(
        "empty name",
      );
    } finally {
      clearInterval(timer);
    }
  });

  it("preserves bytes, nested uploads, trace IDs and typed errors across structured clone", async () => {
    const { binding, proxy } = setup();
    expect(await proxy.readFile("/file")).toEqual({
      traceId: "bytes",
      data: new Uint8Array([0, 255, 128, 7]),
    });
    const data = Buffer.from([0, 255, 128, 7]);
    expect(await proxy.writeFile("/file", data)).toBe("00ff8007");
    expect(data.byteLength).toBe(4); // Caller buffers must never be detached.
    const repository = new binding.NativeRepositoryClient!("http://localhost");
    expect(
      await repository.pushFilesystemFiles(
        "fs",
        [{ path: "a", content: Buffer.from("nested") }],
        [],
        [],
        [],
        "m",
        "main",
      ),
    ).toEqual({ traceId: "repo", json: "nested" });
    await expect(
      callNative(() => proxy.getProcess("missing")),
    ).rejects.toBeInstanceOf(RemoteAPIError);
  });

  it("applies backpressure at the consumer and cancels on early return", async () => {
    const { proxy } = setup();
    const stream = nativeEventStream((emit) => proxy.followStdout("7", emit));
    expect((await stream.next()).value).toEqual({ line: "0" });
    await delay(50);
    expect((await health(proxy)).events).toBe(1);
    expect((await stream.next()).value).toEqual({ line: "1" });
    await stream.return(undefined);
    await delay(20);
    expect((await health(proxy)).cancelled).toBe(1);
  });

  it("aborts a quiet stream and does not start an already-aborted stream", async () => {
    const { proxy } = setup();
    const controller = new AbortController();
    const stream = nativeEventStream(
      (emit) => proxy.followStdout("quiet", emit),
      undefined,
      controller.signal,
    );
    const next = stream.next();
    await health(proxy);
    controller.abort();
    expect((await next).done).toBe(true);
    await delay(20);
    expect((await health(proxy)).cancelled).toBe(1);
    const start = vi.fn();
    expect(
      (await nativeEventStream(start, undefined, controller.signal).next())
        .done,
    ).toBe(true);
    expect(start).not.toHaveBeenCalled();
  });

  it("drains a finite stream in order and cancels when a callback fails", async () => {
    const { proxy } = setup();
    const lines = [];
    for await (const event of nativeEventStream((emit) =>
      proxy.followStdout("7", emit),
    ))
      lines.push(event.line);
    expect(lines).toEqual(Array.from({ length: 100 }, (_, i) => String(i)));
    await expect(
      proxy.followStdout("7", () => {
        throw new Error("consumer failed");
      }),
    ).rejects.toThrow("consumer failed");
    await delay(20);
    expect((await health(proxy)).cancelled).toBe(1);
  });

  it("releases a proxy without invalidating siblings and rejects calls after close", async () => {
    const { client, proxy } = setup();
    const sibling = client.connectProxy("http://localhost", "sibling");
    const stream = nativeEventStream((emit) =>
      proxy.followStdout("quiet", emit),
    );
    const next = stream.next();
    await health(sibling);
    releaseNativeHandle(proxy);
    expect((await next).done).toBe(true);
    await expect(proxy.health()).rejects.toThrow("closed");
    expect((await health(sibling)).clientId).toBe(1);
    releaseNativeHandle(client);
    expect((await health(sibling)).clientId).toBe(1);
  });

  it("rejects every in-flight call on worker failure and recreates handles only for new calls", async () => {
    const { proxy, resolve } = setup();
    await health(proxy);
    const results = await Promise.allSettled([
      proxy.getProcess("pending"),
      proxy.getProcess("crash"),
    ]);
    for (const result of results) {
      expect(result.status).toBe("rejected");
      if (result.status === "rejected")
        expect(result.reason.message).toContain("not retried");
    }
    expect((await health(proxy)).clients).toBe(1);
    expect(resolve).toHaveBeenCalledTimes(2);
  });
});
