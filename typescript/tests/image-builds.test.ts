import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { isDeepStrictEqual } from "node:util";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __setNativeBindingForTest,
  createSandboxImage,
  importSandboxImage,
} from "../src/sandbox-image.js";
import {
  ImageBuildStatus,
  ImageBuildPhase,
  ImageBuildKind,
  ImageBuildErrorCode,
  ImageBuildCancellationOutcome,
  ImageBuildCancelledError,
  ImageBuildError,
  ImageBuildFailedError,
  ImageBuildInProgressError,
  ImageBuildInterruptedError,
  ImageBuildNotFoundError,
  ImageBuildTimeoutError,
  cancelImageBuild,
  getImageBuild,
  startImageBuild,
  startImageImport,
  toImageBuildError,
  waitImageBuild,
} from "../src/image-builds.js";
import { Image } from "../src/image.js";

type Emit = (event: Record<string, unknown>) => void;

function buildJson(status = "pending", extra: Record<string, unknown> = {}) {
  return {
    build_id: "build-1",
    kind: "dockerfile",
    name: "img",
    status,
    phase: null,
    attempt_no: 1,
    builder_sandbox_id: null,
    result: null,
    operation_deadline_ms: null,
    future_field: { ignored: true },
    ...extra,
  };
}

/** A build result as the core hands it over: the template shape derived from the trimmed record. */
function resultJson() {
  return {
    id: "image_ok",
    name: "img",
    snapshotId: "snap-cas",
    snapshotSandboxId: null,
    snapshotUri: "snapshot://snap-cas",
    snapshotFormatVersion: "content_addressed_streaming_v1",
    snapshotSizeBytes: 42,
    rootfsDiskBytes: 10737418240,
    rootfsNodeKind: "base",
    public: false,
    dockerfile: "",
    createdAt: "2026-10-15T00:00:00Z",
    updatedAt: "2026-10-15T00:00:00Z",
  };
}

/** The `{category: "image_build", ...}` rejection the native binding produces. */
function nativeError(payload: Record<string, unknown>): Error {
  return new Error(JSON.stringify({ category: "image_build", status: null, ...payload }));
}

interface FakeBindingOptions {
  waitRejectsWith?: Error;
  /** Make `waitImageBuild` wait until the interrupt is triggered, then reject. */
  waitUntilInterrupted?: boolean;
}

function makeFakeBinding(opts: FakeBindingOptions = {}) {
  const triggered = new Set<number>();
  const waiters = new Map<number, () => void>();
  let nextInterrupt = 7;
  const binding = {
    loadDockerConfigJson: vi.fn(() => "{}"),
    buildSandboxImage: vi.fn(async () => JSON.stringify({ id: "tpl-1" })),
    importSandboxImage: vi.fn(async () => JSON.stringify({ id: "tpl-1" })),
    listSandboxImages: vi.fn(async () => "[]"),
    findSandboxImage: vi.fn(async () => null),
    deleteSandboxImage: vi.fn(async () => {}),
    createImageBuildInterrupt: vi.fn(() => nextInterrupt++),
    triggerImageBuildInterrupt: vi.fn((id: number) => {
      triggered.add(id);
      waiters.get(id)?.();
    }),
    releaseImageBuildInterrupt: vi.fn(),
    startImageBuild: vi.fn(async (_options: unknown, emit?: Emit | null) => {
      emit?.({ eventType: "build_accepted", message: "Build build-1 accepted", buildId: "build-1", clientRequestId: "rid-gen" } as never);
      return JSON.stringify({ build: buildJson(), client_request_id: "rid-gen" });
    }),
    startImageImport: vi.fn(async () =>
      JSON.stringify({ build: buildJson("pending", { kind: "import" }), client_request_id: "rid-gen" }),
    ),
    getImageBuild: vi.fn(async () => JSON.stringify(buildJson("running"))),
    waitImageBuild: vi.fn(
      async (
        _options: unknown,
        _buildId: string,
        _wait: unknown,
        emit?: Emit | null,
        interruptId?: number | null,
      ) => {
        emit?.({ eventType: "build_progress", message: "Build build-1: running (attempt 1)", buildId: "build-1", status: "running", phase: "executing", attempt: 1 } as never);
        if (opts.waitUntilInterrupted) {
          if (interruptId == null) throw new Error("expected an interrupt id");
          if (!triggered.has(interruptId)) {
            await new Promise<void>((resolve) => waiters.set(interruptId, resolve));
          }
          emit?.({ eventType: "interrupted", message: "Build build-1 cancelled", buildId: "build-1", outcome: "cancelled", confirmed: true } as never);
          throw nativeError({ code: "interrupted", message: "Build build-1 cancelled", build_id: "build-1", outcome: "cancelled", confirmed: true });
        }
        if (opts.waitRejectsWith) throw opts.waitRejectsWith;
        return JSON.stringify(resultJson());
      },
    ),
    cancelImageBuild: vi.fn(async (_options: unknown, selector: { buildId?: string; clientRequestId?: string }) =>
      JSON.stringify(
        selector.clientRequestId
          ? { outcome: "not_admitted", build: null }
          : { outcome: "cancelled", build: buildJson("cancelled") },
      ),
    ),
  };
  __setNativeBindingForTest(binding as never);
  return binding;
}

describe("image build operations", () => {
  let tempDir: string;

  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    tempDir = mkdtempSync(path.join(os.tmpdir(), "tl-image-builds-"));
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("startImageBuild admits a Dockerfile build and returns a handle", async () => {
    const binding = makeFakeBinding();
    const dockerfile = path.join(tempDir, "Dockerfile");
    writeFileSync(dockerfile, "FROM scratch\n");
    const events: Record<string, unknown>[] = [];
    const handle = await startImageBuild(dockerfile, {
      registeredName: "img",
      cpus: 1.5,
      memoryMb: 2048,
      diskMb: 20480,
      buildArgs: { A: "1" },
      dockerConfigJson: "{}",
      emit: (event) => events.push(event),
    });
    expect(handle.buildId).toBe("build-1");
    expect(handle.clientRequestId).toBe("rid-gen");
    expect(handle.info.status).toBe("pending");
    expect(handle.info.raw.future_field).toEqual({ ignored: true });
    const [options, , interruptId] = binding.startImageBuild.mock.calls[0] as unknown as [
      Record<string, unknown>,
      unknown,
      number | undefined,
    ];
    expect(options).toMatchObject({
      apiUrl: "https://api.tensorlake.test",
      bearerToken: "tl_key_test",
      dockerfilePath: dockerfile,
      registeredName: "img",
      cpus: 1.5,
      memoryMb: 2048,
      diskMb: 20480,
      isPublic: false,
      namespace: "default",
      useScopeHeaders: false,
      dockerConfigJson: "{}",
      cas: true,
      buildArgs: { A: "1" },
    });
    expect(interruptId).toBeUndefined();
    expect(events).toContainEqual({ type: "status", message: "Building image 'img'..." });
    expect(events).toContainEqual({
      type: "build_accepted",
      message: "Build build-1 accepted",
      build_id: "build-1",
      client_request_id: "rid-gen",
    });
  });

  it.each([
    {}, { build: null }, { build: [] }, { build: {} },
    ...[null, "", " ", 123].map((build_id) => ({ build: buildJson("pending", { build_id }) })),
  ])("rejects admission without a build identity: %j", async (payload) => {
    const binding = makeFakeBinding();
    for (const [native, invoke] of [
      [binding.startImageBuild, () => startImageBuild(new Image({ name: "img", baseImage: "ubuntu" }))],
      [binding.startImageImport, () => startImageImport("ubuntu")],
    ] as const) {
      native.mockResolvedValueOnce(JSON.stringify(payload));
      await expect(invoke()).rejects.toMatchObject({
        code: ImageBuildErrorCode.SERVICE, raw: { response: payload },
      });
    }
  });

  it("keeps a null build id empty", async () => {
    const binding = makeFakeBinding();
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(buildJson("pending", { build_id: null })));
    expect((await getImageBuild("build-1")).info.buildId).toBe("");
  });

  it.each([
    [null, 0], [0, 0], [2, 2], ["2", 2], ["2.0", 2], [Number.MAX_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
  ])("decodes attempt_no %j as %j", async (attempt_no, expected) => {
    const binding = makeFakeBinding();
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(buildJson("pending", { attempt_no })));
    expect((await getImageBuild("build-1")).info.attemptNo).toBe(expected);
  });

  it.each([true, false, 1.5, "1.5", 2 ** 53, -(2 ** 53), "bad", "", [], {}])(
    "rejects invalid attempt_no %j with SERVICE", async (attempt_no) => {
      const binding = makeFakeBinding();
      binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(buildJson("pending", { attempt_no })));
      await expect(getImageBuild("build-1")).rejects.toMatchObject({ code: ImageBuildErrorCode.SERVICE });
    },
  );

  it("decodes known and future build enums at the response boundary", async () => {
    const binding = makeFakeBinding();
    for (const [field, values] of [["status", ImageBuildStatus], ["kind", ImageBuildKind], ["phase", ImageBuildPhase]] as const) {
      for (const value of Object.values(values)) {
        binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(buildJson("pending", { [field]: value })));
        expect((await getImageBuild("build-1")).info[field]).toBe(value);
      }
    }
    const raw = buildJson("future_status", { kind: "future_kind", phase: "future_phase" });
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(raw));
    const { info } = await getImageBuild("build-1");
    expect(info).toMatchObject({ status: ImageBuildStatus.UNKNOWN, kind: ImageBuildKind.UNKNOWN, phase: ImageBuildPhase.UNKNOWN });
    expect(info.raw).toEqual(raw);
    for (const value of [null, "", undefined]) {
      binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(buildJson("pending", { kind: value, phase: value })));
      const { info } = await getImageBuild("build-1");
      expect(info.kind).toBeNull();
      expect(info.phase).toBeNull();
    }
  });

  it("keeps diagnostic raw out of equality and serialization and updates it on wait", async () => {
    const binding = makeFakeBinding();
    const raw = buildJson("pending");
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(raw));
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify({ ...raw, future_field: "different" }));
    const a = await getImageBuild("build-1");
    const b = await getImageBuild("build-1");
    expect(isDeepStrictEqual(a.info, b.info)).toBe(true);
    expect(a.info.raw).not.toEqual(b.info.raw);
    expect(JSON.parse(JSON.stringify(a.info))).not.toHaveProperty("raw");
    await a.wait();
    expect(a.info.raw).toEqual({ ...raw, status: "succeeded", result: resultJson() });
    expect(a.info.raw).not.toHaveProperty("raw");
    expect(a.info.buildId).toBe("build-1");
    const cancellation = await a.cancel();
    const dumped = JSON.parse(JSON.stringify(cancellation));
    expect(dumped).not.toHaveProperty("raw");
    expect(dumped.build).not.toHaveProperty("raw");
  });

  it("decodes structured errors without reparsing them in constructors", () => {
    for (const code of Object.values(ImageBuildErrorCode)) {
      expect((toImageBuildError(nativeError({ code, message: "m" })) as ImageBuildError).code).toBe(code);
    }
    const payload = { code: "future_code", message: "m", active_kind: "future_kind", active_status: "future_status" };
    const error = toImageBuildError(nativeError(payload)) as ImageBuildError;
    expect(error.code).toBe(ImageBuildErrorCode.UNKNOWN);
    expect(error.details.activeKind).toBe(ImageBuildKind.UNKNOWN);
    expect(error.details.activeStatus).toBe(ImageBuildStatus.UNKNOWN);
    expect(error.raw).toMatchObject(payload);
    expect(JSON.parse(JSON.stringify(error))).not.toHaveProperty("raw");
    for (const code of ["failed", "cancelled"]) {
      const error = toImageBuildError(nativeError({ code, message: "m", info: buildJson("pending", { status: null }) })) as ImageBuildFailedError;
      expect(error.info?.status).toBe(ImageBuildStatus.UNKNOWN);
      expect(error.info?.raw.status).toBeNull();
    }
    for (const info of ["bad", [], { attempt_no: "bad" }]) {
      const error = toImageBuildError(nativeError({ code: "failed", message: "m", info })) as ImageBuildFailedError;
      expect(error).toBeInstanceOf(ImageBuildFailedError);
      expect(error.info).toBeUndefined();
    }
  });

  it("never confirms an unfamiliar cancellation outcome", async () => {
    const binding = makeFakeBinding();
    for (const outcome of Object.values(ImageBuildCancellationOutcome)) {
      binding.cancelImageBuild.mockResolvedValueOnce(JSON.stringify({ outcome }));
      expect((await cancelImageBuild({ buildId: "build-1" })).outcome).toBe(outcome);
    }
    binding.cancelImageBuild.mockResolvedValueOnce(JSON.stringify({ outcome: "future_outcome" }));
    const outcome = await cancelImageBuild({ buildId: "build-1" });
    expect(outcome.outcome).toBe(ImageBuildCancellationOutcome.UNKNOWN);
    expect(outcome.confirmed).toBe(false);
    for (const fields of [{}, { outcome: null }, { outcome: "future_outcome" }]) {
      const error = toImageBuildError(nativeError({ code: "interrupted", message: "m", confirmed: true, ...fields })) as ImageBuildInterruptedError;
      expect(error.confirmed).toBe(false);
      expect(error.details.confirmed).toBe(false);
      expect(error.raw.confirmed).toBe(true);
    }
  });

  it.each([null, [], { attempt_no: "bad" }])("keeps malformed top-level build records in the typed hierarchy: %j", async (raw) => {
    const binding = makeFakeBinding();
    binding.getImageBuild.mockResolvedValueOnce(JSON.stringify(raw));
    await expect(getImageBuild("build-1")).rejects.toBeInstanceOf(ImageBuildError);
  });

  it("keeps callback events as plain strings with the native spelling", async () => {
    const binding = makeFakeBinding();
    binding.waitImageBuild.mockImplementationOnce(async (_opts, _id, _wait, emit) => {
      emit?.({ eventType: "future_event", message: "m", status: "future_status", outcome: "future_outcome", confirmed: true });
      return JSON.stringify(resultJson());
    });
    const events: Record<string, unknown>[] = [];
    await waitImageBuild("build-1", { emit: (e) => events.push(e) });
    expect(events).toEqual([{ type: "future_event", message: "m", status: "future_status", outcome: "future_outcome", confirmed: true }]);
  });

  it("keeps malformed native bodies typed across every build response boundary", async () => {
    const binding = makeFakeBinding();
    const handle = await getImageBuild("build-1");
    for (const [native, invoke] of [
      [binding.getImageBuild, () => getImageBuild("build-1")],
      [binding.getImageBuild, () => handle.refresh()],
      [binding.startImageBuild, () => startImageBuild(new Image({ name: "img", baseImage: "ubuntu" }))],
      [binding.startImageImport, () => startImageImport("ubuntu")],
      [binding.waitImageBuild, () => handle.wait()],
      [binding.waitImageBuild, () => waitImageBuild("build-1")],
      [binding.cancelImageBuild, () => cancelImageBuild({ buildId: "build-1" })],
    ] as const) {
      native.mockResolvedValueOnce("null");
      await expect(invoke()).rejects.toBeInstanceOf(ImageBuildError);
    }
  });

  it("startImageBuild renders an Image into an empty context and cleans it up", async () => {
    const binding = makeFakeBinding();
    const handle = await startImageBuild(new Image({ name: "cas-img", baseImage: "python:3.12-slim" }));
    expect(handle.buildId).toBe("build-1");
    const options = binding.startImageBuild.mock.calls[0]![0] as Record<string, unknown>;
    expect(options.registeredName).toBe("cas-img");
    expect(String(options.dockerfileText)).toContain("FROM python:3.12-slim");
    expect(typeof options.contextDir).toBe("string");
  });

  it("startImageImport admits an import", async () => {
    const binding = makeFakeBinding();
    const handle = await startImageImport("ghcr.io/org/app:v1");
    expect(handle.info.kind).toBe("import");
    const options = binding.startImageImport.mock.calls[0]![0] as Record<string, unknown>;
    expect(options).toMatchObject({ imageReference: "ghcr.io/org/app:v1", registeredName: "app", cas: true });
  });

  it("waitImageBuild runs the lookup inside the interruptible native wait, not as a separate call", async () => {
    const binding = makeFakeBinding();
    const result = await waitImageBuild("build-1", { timeoutMs: 1234 });
    expect(result.id).toBe(resultJson().id);
    expect(binding.getImageBuild).not.toHaveBeenCalled();
    expect(binding.waitImageBuild).toHaveBeenCalledTimes(1);
    expect(binding.waitImageBuild.mock.calls[0][1]).toBe("build-1");
    expect(binding.waitImageBuild.mock.calls[0][2]).toEqual({ timeoutMs: 1234, followLogs: false });
  });

  it("getImageBuild returns the record and refresh re-reads it", async () => {
    const binding = makeFakeBinding();
    const handle = await getImageBuild("build-1");
    expect(handle.info.status).toBe("running");
    binding.getImageBuild.mockResolvedValueOnce(
      JSON.stringify(buildJson("succeeded", { result: resultJson() })),
    );
    const info = await handle.refresh();
    expect(info.status).toBe("succeeded");
    expect(info.result?.id).toBe("image_ok");
    expect(info.result?.snapshotFormatVersion).toBe("content_addressed_streaming_v1");
    expect(binding.getImageBuild).toHaveBeenCalledWith(
      { apiUrl: "https://api.tensorlake.test", bearerToken: "tl_key_test", namespace: "default", useScopeHeaders: false },
      "build-1",
    );
  });

  it("refuses legacy and image references before any native call", async () => {
    const binding = makeFakeBinding();
    for (const reference of ["sandbox_template_1", "image_abc123", " "]) {
      await expect(getImageBuild(reference)).rejects.toBeInstanceOf(ImageBuildError);
      await expect(waitImageBuild(reference)).rejects.toBeInstanceOf(ImageBuildError);
      await expect(cancelImageBuild({ buildId: reference })).rejects.toBeInstanceOf(ImageBuildError);
    }
    expect(binding.getImageBuild).not.toHaveBeenCalled();
    expect(binding.waitImageBuild).not.toHaveBeenCalled();
    expect(binding.cancelImageBuild).not.toHaveBeenCalled();
  });

  it("wait returns the operation's own result and forwards options", async () => {
    const binding = makeFakeBinding();
    const events: Record<string, unknown>[] = [];
    const handle = await getImageBuild("build-1");
    const result = await handle.wait({ timeoutMs: 1500, followLogs: true, emit: (e) => events.push(e) });
    expect(result.id).toBe("image_ok");
    // One image representation: the result is the template-shaped record
    // the catalog answers, with no digest or build provenance extras.
    expect(result.snapshotFormatVersion).toBe("content_addressed_streaming_v1");
    expect(result.snapshotUri).toBe("snapshot://snap-cas");
    expect(result).toEqual(resultJson());
    expect("buildId" in result).toBe(false);
    expect("imageId" in result).toBe(false);
    expect(handle.info.status).toBe("succeeded");
    const [, buildId, waitOptions, , interruptId] = binding.waitImageBuild.mock.calls[0] as unknown as [
      unknown,
      string,
      Record<string, unknown>,
      unknown,
      number | undefined,
    ];
    expect(buildId).toBe("build-1");
    expect(waitOptions).toEqual({ timeoutMs: 1500, followLogs: true });
    expect(interruptId).toBeUndefined();
    expect(events).toContainEqual({
      type: "build_progress",
      message: "Build build-1: running (attempt 1)",
      build_id: "build-1",
      status: "running",
      phase: "executing",
      attempt: 1,
    });
  });

  it("maps typed outcomes to error classes", async () => {
    const cases: Array<[Record<string, unknown>, unknown]> = [
      [{ code: "failed", message: "build build-1 failed: RUN exited 1", build_id: "build-1", error: "RUN exited 1", info: buildJson("failed") }, ImageBuildFailedError],
      [{ code: "cancelled", message: "build build-1 was cancelled", build_id: "build-1", info: buildJson("cancelled") }, ImageBuildCancelledError],
      [{ code: "timeout", message: "build build-1 did not finish", build_id: "build-1", waited_secs: 7 }, ImageBuildTimeoutError],
      [{ code: "not_found", message: "build build-1 not found", build_id: "build-1" }, ImageBuildNotFoundError],
    ];
    for (const [payload, expected] of cases) {
      makeFakeBinding({ waitRejectsWith: nativeError(payload) });
      const error = await waitImageBuild("build-1").catch((e: unknown) => e);
      expect(error).toBeInstanceOf(expected);
      expect((error as ImageBuildError).buildId).toBe("build-1");
      expect((error as ImageBuildError).code).toBe(payload.code);
    }
    const failed = toImageBuildError(
      nativeError({ code: "failed", message: "m", build_id: "b", error: "RUN exited 1", info: buildJson("failed") }),
    ) as ImageBuildFailedError;
    expect(failed.error).toBe("RUN exited 1");
    expect(failed.info?.status).toBe("failed");
    const conflict = toImageBuildError(
      nativeError({
        code: "image_build_in_progress",
        message: "Image 'img' already has an unfinished build 'build-9'. Cancel it before starting another.",
        build_id: "build-9",
        image_name: "img",
        active_build_id: "build-9",
        active_kind: "dockerfile",
        active_status: "running",
      }),
    ) as ImageBuildInProgressError;
    expect(conflict).toBeInstanceOf(ImageBuildInProgressError);
    expect(conflict.activeBuildId).toBe("build-9");
    expect(conflict.activeKind).toBe("dockerfile");
    expect(conflict.activeStatus).toBe("running");
    // Other errors pass through untouched.
    const plain = new Error("boom");
    expect(toImageBuildError(plain)).toBe(plain);
  });

  it("an AbortSignal reaches the native interrupt and rejects with the outcome", async () => {
    const binding = makeFakeBinding({ waitUntilInterrupted: true });
    const controller = new AbortController();
    const events: Record<string, unknown>[] = [];
    const pending = waitImageBuild("build-1", { signal: controller.signal, emit: (e) => events.push(e) });
    await vi.waitFor(() => expect(binding.waitImageBuild).toHaveBeenCalled());
    controller.abort();
    const error = await pending.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ImageBuildInterruptedError);
    expect((error as ImageBuildInterruptedError).outcome).toBe("cancelled");
    expect((error as ImageBuildInterruptedError).confirmed).toBe(true);
    expect((error as ImageBuildInterruptedError).buildId).toBe("build-1");
    expect(binding.createImageBuildInterrupt).toHaveBeenCalledTimes(1);
    expect(binding.triggerImageBuildInterrupt).toHaveBeenCalledWith(7);
    expect(binding.releaseImageBuildInterrupt).toHaveBeenCalledWith(7);
    const waitInterrupt = binding.waitImageBuild.mock.calls[0]![4];
    expect(waitInterrupt).toBe(7);
    expect(events).toContainEqual({
      type: "interrupted",
      message: "Build build-1 cancelled",
      build_id: "build-1",
      outcome: "cancelled",
      confirmed: true,
    });
  });

  it("an already aborted signal triggers the interrupt immediately", async () => {
    const binding = makeFakeBinding({ waitUntilInterrupted: true });
    const controller = new AbortController();
    controller.abort();
    await expect(waitImageBuild("build-1", { signal: controller.signal })).rejects.toBeInstanceOf(
      ImageBuildInterruptedError,
    );
    expect(binding.triggerImageBuildInterrupt).toHaveBeenCalledWith(7);
  });

  it("cancels by build id alone and reports not_admitted as an observation", async () => {
    const binding = makeFakeBinding();
    const cancelled = await cancelImageBuild({ buildId: "build-1" });
    expect(cancelled.outcome).toBe("cancelled");
    expect(cancelled.confirmed).toBe(true);
    expect(cancelled.build?.status).toBe("cancelled");
    expect(binding.cancelImageBuild).toHaveBeenLastCalledWith(
      expect.objectContaining({ bearerToken: "tl_key_test" }),
      { buildId: "build-1", clientRequestId: undefined },
    );
    const notAdmitted = await cancelImageBuild({ clientRequestId: "rid-lost" });
    expect(notAdmitted.outcome).toBe("not_admitted");
    expect(notAdmitted.confirmed).toBe(false);
    expect(notAdmitted.build).toBeNull();
    const handle = await getImageBuild("build-1");
    await handle.cancel();
    expect(binding.cancelImageBuild).toHaveBeenLastCalledWith(expect.anything(), {
      buildId: "build-1",
      clientRequestId: undefined,
    });
    await expect(cancelImageBuild({})).rejects.toBeInstanceOf(ImageBuildError);
    await expect(cancelImageBuild({ buildId: "b", clientRequestId: "r" })).rejects.toBeInstanceOf(
      ImageBuildError,
    );
  });

  it("requires an API key", async () => {
    makeFakeBinding();
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "pat");
    const error = await startImageImport("ubuntu:24.04").catch((e: unknown) => e);
    expect(error).toBeInstanceOf(ImageBuildError);
    expect((error as ImageBuildError).code).toBe("auth");
  });
});

describe("CAS convenience paths and the legacy boundary", () => {
  let tempDir: string;

  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    tempDir = mkdtempSync(path.join(os.tmpdir(), "tl-image-builds-"));
    writeFileSync(path.join(tempDir, "Dockerfile"), "FROM scratch\n");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
    rmSync(tempDir, { recursive: true, force: true });
  });

  it("createSandboxImage with cas wires the signal into the native interrupt", async () => {
    const binding = makeFakeBinding();
    const controller = new AbortController();
    await createSandboxImage(path.join(tempDir, "Dockerfile"), { cas: true, signal: controller.signal });
    expect(binding.createImageBuildInterrupt).toHaveBeenCalledTimes(1);
    expect(binding.buildSandboxImage.mock.calls[0]![2]).toBe(7);
    expect(binding.releaseImageBuildInterrupt).toHaveBeenCalledWith(7);
  });

  it("createSandboxImage with cas decodes typed outcomes", async () => {
    const binding = makeFakeBinding();
    binding.buildSandboxImage.mockRejectedValueOnce(
      nativeError({
        code: "image_build_in_progress",
        message: "Image 'img' already has an unfinished build 'build-9'. Cancel it before starting another.",
        active_build_id: "build-9",
      }),
    );
    const error = await createSandboxImage(path.join(tempDir, "Dockerfile"), { cas: true }).catch(
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ImageBuildInProgressError);
  });

  it("legacy createSandboxImage never receives an interrupt and keeps its error path", async () => {
    const binding = makeFakeBinding();
    await createSandboxImage(path.join(tempDir, "Dockerfile"));
    expect(binding.createImageBuildInterrupt).not.toHaveBeenCalled();
    expect(binding.buildSandboxImage.mock.calls[0]!.length).toBe(2);
    expect(binding.buildSandboxImage.mock.calls[0]![0]).toMatchObject({ cas: false });

    // A legacy failure is the plain native error, not a typed build error.
    const plain = new Error("rootfs build preparation failed (HTTP 500)");
    binding.buildSandboxImage.mockRejectedValueOnce(plain);
    const error = await createSandboxImage(path.join(tempDir, "Dockerfile")).catch((e: unknown) => e);
    expect(error).toBe(plain);
    expect(binding.cancelImageBuild).not.toHaveBeenCalled();
    expect(binding.triggerImageBuildInterrupt).not.toHaveBeenCalled();
  });

  it("rejects a signal on a legacy build or import before any native call", async () => {
    const binding = makeFakeBinding();
    const controller = new AbortController();
    await expect(
      createSandboxImage(path.join(tempDir, "Dockerfile"), { signal: controller.signal }),
    ).rejects.toThrow(/only supported for CAS image builds/);
    await expect(
      importSandboxImage("ubuntu:24.04", { signal: controller.signal }),
    ).rejects.toThrow(/only supported for CAS image imports/);
    expect(binding.buildSandboxImage).not.toHaveBeenCalled();
    expect(binding.importSandboxImage).not.toHaveBeenCalled();
    expect(binding.createImageBuildInterrupt).not.toHaveBeenCalled();
  });

  it("importSandboxImage with cas wires the signal and legacy imports do not", async () => {
    const binding = makeFakeBinding();
    const controller = new AbortController();
    await importSandboxImage("ubuntu:24.04", { cas: true, signal: controller.signal });
    expect(binding.importSandboxImage.mock.calls[0]![2]).toBe(7);
    await importSandboxImage("ubuntu:24.04");
    expect(binding.importSandboxImage.mock.calls[1]!.length).toBe(2);
    expect(binding.createImageBuildInterrupt).toHaveBeenCalledTimes(1);
  });
});
