import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { once } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  ImageBuildInProgressError,
  ImageBuildInterruptedError,
  ImageBuildNotFoundError,
  cancelImageBuild,
  getImageBuild,
  startImageImport,
  waitImageBuild,
} from "../src/image-builds.js";
import { deleteSandboxImage, importSandboxImage } from "../src/sandbox-image.js";

// Run against the built addon with `npm run test:native-cloud`. Ordinary unit
// tests stub the binding and do not require native build artifacts.
//
// A scripted Image Service records every request so the tests can assert on
// the admission key and on the cancel selector an AbortSignal produces.
describe.skipIf(process.env.TENSORLAKE_TEST_NATIVE_CLOUD !== "1")("native image build operations", () => {
  let server: Server;
  let url: string;
  let requests: Array<{ method: string; url: string; headers: IncomingMessage["headers"]; body: string }>;
  let script: Map<string, Array<(body: string) => [number, unknown]>>;

  function build(status: string, extra: Record<string, unknown> = {}) {
    return {
      build_id: "build-1",
      client_request_id: null,
      kind: "import",
      name: "img",
      status,
      phase: null,
      attempt_no: 1,
      builder_sandbox_id: null,
      result: null,
      operation_deadline_ms: null,
      ...extra,
    };
  }

  /** The Image Service's trimmed tenant record (what the service sends). */
  function result() {
    return {
      id: "image_ok",
      name: "img",
      snapshot_id: "snap-cas",
      global: false,
      physical_size_bytes: 42,
      logical_size_bytes: 10737418240,
      created_at: "2026-10-15T00:00:00Z",
      updated_at: "2026-10-15T00:00:00Z",
    };
  }

  function on(method: string, path: string, ...replies: Array<(body: string) => [number, unknown]>) {
    const key = `${method} ${path}`;
    script.set(key, [...(script.get(key) ?? []), ...replies]);
  }

  async function waitForRequest(method: string, path: string) {
    await vi.waitFor(
      () => expect(requests.some((r) => r.method === method && r.url === path)).toBe(true),
      { timeout: 20_000 },
    );
  }

  beforeEach(async () => {
    requests = [];
    script = new Map();
    server = createServer((request: IncomingMessage, response: ServerResponse) => {
      const chunks: Buffer[] = [];
      request.on("data", (chunk: Buffer) => chunks.push(chunk));
      request.on("end", () => {
        const body = Buffer.concat(chunks).toString("utf8");
        requests.push({ method: request.method ?? "", url: request.url ?? "", headers: request.headers, body });
        const key = `${request.method} ${request.url}`;
        const queue = script.get(key);
        const reply = queue?.length ? (queue.length > 1 ? queue.shift()! : queue[0]!) : undefined;
        const [status, payload] = reply ? reply(body) : [404, { error: `unscripted ${key}` }];
        response.statusCode = status;
        response.setHeader("content-type", "application/json");
        response.end(JSON.stringify(payload));
      });
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    url = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
    vi.stubEnv("TENSORLAKE_API_URL", url);
    vi.stubEnv("TENSORLAKE_API_KEY", "test-key");
    vi.stubEnv("TENSORLAKE_IMAGE_SERVICE_URL", "");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
  });

  afterEach(async () => {
    vi.unstubAllEnvs();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("admits under one Idempotency-Key across a transient retry and waits to the result", async () => {
    on("POST", "/images/v4/builds", () => [503, { error: "deploying" }], () => [201, build("pending")]);
    on("GET", "/images/v4/builds/build-1", () => [200, build("running")], () => [
      200,
      build("succeeded", { result: result() }),
    ]);
    const handle = await startImageImport("ubuntu:24.04", { registeredName: "img", dockerConfigJson: "{}" });
    expect(handle.buildId).toBe("build-1");
    const admissions = requests.filter((r) => r.url === "/images/v4/builds");
    expect(admissions).toHaveLength(2);
    const keys = new Set(admissions.map((r) => r.headers["idempotency-key"]));
    expect(keys.size).toBe(1);
    expect([...keys][0]).toBe(handle.clientRequestId);
    const published = await handle.wait();
    expect(published.id).toBe("image_ok");
    expect(published.snapshotFormatVersion).toBe("content_addressed_streaming_v1");
  });

  it("an AbortSignal reaches the Rust cancellation handshake by build id", async () => {
    on("GET", "/images/v4/builds/build-1", () => [200, build("running")]);
    on("POST", "/images/v4/builds/build-1/cancel", () => [200, { outcome: "cancelled", build: build("cancelled") }]);
    const controller = new AbortController();
    const events: Array<Record<string, unknown>> = [];
    const pending = waitImageBuild("build-1", { signal: controller.signal, emit: (e) => events.push(e) });
    await waitForRequest("GET", "/images/v4/builds/build-1");
    controller.abort();
    const error = await pending.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ImageBuildInterruptedError);
    expect((error as ImageBuildInterruptedError).outcome).toBe("cancelled");
    expect((error as ImageBuildInterruptedError).confirmed).toBe(true);
    const cancel = requests.find((r) => r.url === "/images/v4/builds/build-1/cancel");
    expect(cancel).toBeDefined();
    // The build id rides in the path; the by-id cancel carries no body.
    expect(cancel!.body).toBe("");
    expect(events.some((e) => e.type === "interrupted" && e.outcome === "cancelled")).toBe(true);
  });

  it("an AbortSignal on a CAS import cancels while a timeout only stops waiting", async () => {
    on("POST", "/images/v4/builds", () => [201, build("pending")]);
    on("GET", "/images/v4/builds/build-1", () => [200, build("running")]);
    on("POST", "/images/v4/builds/build-1/cancel", () => [
      200,
      { outcome: "already_completed", build: build("succeeded", { result: result() }) },
    ]);
    const controller = new AbortController();
    const pending = importSandboxImage("ubuntu:24.04", {
      cas: true,
      registeredName: "img",
      dockerConfigJson: "{}",
      signal: controller.signal,
    });
    await waitForRequest("GET", "/images/v4/builds/build-1");
    controller.abort();
    const error = await pending.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ImageBuildInterruptedError);
    expect((error as ImageBuildInterruptedError).outcome).toBe("already_completed");
    expect((error as ImageBuildInterruptedError).result?.id).toBe("image_ok");
    expect(requests.filter((r) => r.url === "/images/v4/builds")).toHaveLength(1);

    requests = [];
    const timeout = await waitImageBuild("build-1", { timeoutMs: 300 }).catch((e: unknown) => e);
    expect((timeout as Error).name).toBe("ImageBuildTimeoutError");
    expect(requests.some((r) => r.url === "/images/v4/builds/build-1/cancel")).toBe(false);
  });

  it("surfaces typed conflicts, not-found and not_admitted through the binding", async () => {
    on("POST", "/images/v4/builds", () => [
      409,
      {
        error: "Image 'img' already has an unfinished import 'build-3'. Cancel it before starting another.",
        code: "image_build_in_progress",
        image_name: "img",
        active_build_id: "build-3",
        active_kind: "import",
        active_status: "pending",
      },
    ]);
    on("GET", "/images/v4/builds/build-x", () => [404, { error: "not found" }]);
    on("POST", "/images/v4/builds/cancel", () => [200, { outcome: "not_admitted", build: null }]);
    const conflict = await startImageImport("ubuntu:24.04", { registeredName: "img", dockerConfigJson: "{}" }).catch(
      (e: unknown) => e,
    );
    expect(conflict).toBeInstanceOf(ImageBuildInProgressError);
    expect((conflict as ImageBuildInProgressError).activeBuildId).toBe("build-3");
    await expect(getImageBuild("build-x")).rejects.toBeInstanceOf(ImageBuildNotFoundError);
    const outcome = await cancelImageBuild({ clientRequestId: "rid-lost" });
    expect(outcome.outcome).toBe("not_admitted");
    expect(outcome.confirmed).toBe(false);
    const cancel = requests.find((r) => r.url === "/images/v4/builds/cancel");
    expect(JSON.parse(cancel!.body)).toEqual({ client_request_id: "rid-lost" });
  });

  it("a legacy import keeps its error path and makes no Image Service request", async () => {
    on("POST", "/platform/v1/sandbox-template-builds", () => [500, { message: "baseline failure" }]);
    const error = await importSandboxImage("ubuntu:24.04", {
      cas: false,
      registeredName: "img",
      dockerConfigJson: "{}",
    }).catch((e: unknown) => e);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain("failed to prepare legacy image build");
    expect(error).not.toBeInstanceOf(ImageBuildInProgressError);
    expect(requests.every((r) => !r.url.startsWith("/images/v4/"))).toBe(true);
    expect(requests.some((r) => r.url === "/images/v4/builds/cancel")).toBe(false);
  });

  it("a name delete warns in the CAS spelling when the legacy cleanup fails", async () => {
    // `MyProject` deletes the CAS image `myproject`; the legacy image of that
    // name cannot be deleted, so the call succeeds and the warning, built from
    // the binding's real return value, names `myproject` for recovery.
    on("DELETE", "/images/v4/names/myproject", () => [202, { deleting: true }]);
    on("GET", "/platform/v1/sandbox-templates/by-name/myproject", () => [500, { message: "internal error" }]);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    try {
      await expect(deleteSandboxImage("MyProject")).resolves.toBeUndefined();
      expect(warn).toHaveBeenCalledTimes(1);
      const message = String(warn.mock.calls[0]![0]);
      expect(message).toContain("Deleted CAS image 'myproject'");
      expect(message).toContain("internal error");
      expect(message).toContain('deleteSandboxImage("myproject")');
      expect(message).not.toContain("MyProject");
      expect(requests.map((r) => `${r.method} ${r.url}`)).toEqual([
        "DELETE /images/v4/names/myproject",
        "GET /platform/v1/sandbox-templates/by-name/myproject",
      ]);
    } finally {
      warn.mockRestore();
    }
  });
});
