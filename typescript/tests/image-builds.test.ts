import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __setNativeBindingForTest,
  createSandboxImage,
  importSandboxImage,
} from "../src/sandbox-image.js";
import {
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
    client_request_id: "rid-1",
    kind: "dockerfile",
    name: "img",
    status,
    phase: null,
    attempt_no: 1,
    builder_sandbox_id: null,
    result: null,
    cleanup_pending: false,
    operation_deadline_ms: null,
    future_field: { ignored: true },
    ...extra,
  };
}

function resultJson() {
  return {
    id: "image_ok",
    name: "img",
    snapshot_id: "snap-cas",
    snapshot_uri: "snapshot://snap-cas",
    snapshot_sandbox_id: null,
    snapshot_format_version: "content_addressed_streaming_v1",
    snapshot_size_bytes: 42,
    rootfs_disk_bytes: 10737418240,
    rootfs_node_kind: "base",
    public: false,
    dockerfile: "",
    created_by_user_id: null,
    created_at: "2026-10-15T00:00:00Z",
    updated_at: "2026-10-15T00:00:00Z",
    image_id: "a".repeat(64),
    build_id: "build-1",
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
    expect(result.buildId).toBe("build-1");
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
