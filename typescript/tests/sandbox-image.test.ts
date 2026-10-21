import { existsSync, readdirSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __setNativeBindingForTest,
  createSandboxImage,
  deleteSandboxImage,
  findSandboxImageByName,
  importSandboxImage,
  listSandboxImages,
  runCreateSandboxImageCli,
  runImportSandboxImageCli,
} from "../src/sandbox-image.js";
import { Image, dockerfileContent } from "../src/image.js";
import { CloudClient } from "../src/cloud-client.js";
import { API_URL } from "../src/defaults.js";

interface CapturedCall {
  options: Record<string, unknown>;
  emit?: ((event: { eventType: string; stream?: string | null; message: string }) => void) | null;
  // Files staged into the build context, relative + posix-normalized, sorted.
  // Snapshotted at call time since the temp context is removed once build returns.
  contextFiles?: string[] | null;
}

function makeFakeBinding(opts: {
  resultJson?: string;
  events?: Array<{ eventType: string; stream?: string | null; message: string }>;
  listJson?: string;
  findJson?: string | null;
} = {}) {
  const captured: CapturedCall = { options: {} };
  const handler = async (
    options: Record<string, unknown>,
    emit?:
      | ((event: { eventType: string; stream?: string | null; message: string }) => void)
      | null,
  ) => {
    captured.options = options;
    captured.emit = emit;
    const contextDir = options.contextDir as string | undefined;
    captured.contextFiles =
      contextDir != null
        ? readdirSync(contextDir, { recursive: true, withFileTypes: true })
            .filter((d) => d.isFile())
            .map((d) =>
              path
                .relative(contextDir, path.join(d.parentPath, d.name))
                .split(path.sep)
                .join("/"),
            )
            .sort()
        : null;
    if (emit && opts.events) {
      for (const event of opts.events) emit(event);
    }
    return opts.resultJson ?? '{"id":"tpl-1","snapshot_id":"snap-1"}';
  };
  const binding = {
    loadDockerConfigJson: vi.fn(() => "{}"),
    buildSandboxImage: vi.fn(handler),
    importSandboxImage: vi.fn(handler),
    listSandboxImages: vi.fn(async () => opts.listJson ?? "[]"),
    findSandboxImage: vi.fn(async () => opts.findJson ?? null),
    deleteSandboxImage: vi.fn(async () => {}),
  };
  __setNativeBindingForTest(binding);
  return { binding, captured };
}

describe("createSandboxImage", () => {
  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
    vi.restoreAllMocks();
  });

  it("delegates a Dockerfile-path source to the native binding", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-path`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "sandbox-image.Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim\nWORKDIR /app\nRUN echo hi\n",
      "utf8",
    );

    const { binding, captured } = makeFakeBinding();
    const result = await createSandboxImage(dockerfilePath, { isPublic: true });

    expect(binding.buildSandboxImage).toHaveBeenCalledOnce();
    expect(captured.options).toMatchObject({
      apiUrl: "https://api.tensorlake.test",
      bearerToken: "tl_key_test",
      dockerfilePath: path.resolve(dockerfilePath),
      registeredName: "sandbox-image",
      isPublic: true,
      dockerCompat: false,
      dockerfileText: undefined,
      contextDir: undefined,
    });
    expect(result).toEqual({ id: "tpl-1", snapshot_id: "snap-1" });
  });

  it("renders an Image DSL definition to Dockerfile text and forwards context_dir", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-dsl`),
      { recursive: true },
    );
    await writeFile(path.join(tempDir, "hello.txt"), "hi", "utf8");

    const image = new Image({
      name: "dsl-image",
      baseImage: "python:3.12-slim",
    })
      .run("apt-get update")
      .workdir("/app")
      .env("APP_ENV", "prod")
      .copy("./src", "/app/src");

    const { captured } = makeFakeBinding();
    await createSandboxImage(image, { contextDir: tempDir });

    expect(captured.options.contextDir).toBe(path.resolve(tempDir));
    expect(captured.options.registeredName).toBe("dsl-image");
    // Dockerfile text must match the renderer in image.ts (the napi binding
    // is responsible for parsing/validating it).
    const expectedDockerfile = `${dockerfileContent(image)}\n`;
    expect(captured.options.dockerfileText).toBe(expectedDockerfile);
  });

  it("uses an empty build context (not cwd) when contextDir is omitted", async () => {
    // Without an explicit contextDir, an Image build must NOT upload the
    // current working directory (which has no Dockerfile in it). It uses a
    // throwaway empty temp dir instead, so only the generated Dockerfile text
    // is built — nothing from cwd is archived — and that temp dir is cleaned
    // up once the build returns.
    const image = new Image({
      name: "no-context-image",
      baseImage: "python:3.12-slim",
    }).run("pip install numpy");

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    const contextDir = captured.options.contextDir as string;
    expect(typeof contextDir).toBe("string");
    expect(path.resolve(contextDir)).not.toBe(path.resolve(process.cwd()));
    // The temp dir is removed once the build returns.
    expect(existsSync(contextDir)).toBe(false);
  });

  it("uses an empty context for an image with no COPY/ADD ops", async () => {
    const image = new Image({
      name: "no-copy-image",
      baseImage: "python:3.12-slim",
    }).run("pip install numpy");

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    expect(captured.contextFiles).toEqual([]);
  });

  it("throws when COPY reads host files without a contextDir", async () => {
    // COPY/ADD that reads host files needs a context. Without contextDir the
    // build must fail fast with a clear, actionable message rather than
    // guessing or uploading the whole cwd.
    const image = new Image({
      name: "copy-image",
      baseImage: "python:3.12-slim",
    }).copy("requirements.txt", "/tmp/requirements.txt");

    makeFakeBinding();
    await expect(createSandboxImage(image)).rejects.toThrow(/contextDir/);
  });

  it("throws when ADD reads host files without a contextDir", async () => {
    const image = new Image({
      name: "add-image",
      baseImage: "python:3.12-slim",
    }).add("./data", "/app/data");

    makeFakeBinding();
    await expect(createSandboxImage(image)).rejects.toThrow(/contextDir/);
  });

  it("does not require a context for a remote ADD <url>", async () => {
    // A remote ADD <url> reads nothing from the host, so it builds with an
    // empty context.
    const image = new Image({
      name: "url-image",
      baseImage: "python:3.12-slim",
    }).add("https://example.com/data.tar.gz", "/app/data.tar.gz");

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    expect(captured.contextFiles).toEqual([]);
  });

  it("does not require a context for COPY --from=<stage>", async () => {
    // COPY --from reads from another build stage, not the host.
    const image = new Image({
      name: "stage-image",
      baseImage: "python:3.12-slim",
    }).copy("/build/app", "/app", { from: "builder" });

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    expect(captured.contextFiles).toEqual([]);
  });

  it("throws when a RUN bind mount reads the context without a contextDir", async () => {
    // A RUN bind mount reads the build context, so it needs a context.
    // `type=bind` is the default, so omitting it must also be detected.
    for (const mount of ["type=bind,source=.,target=/src", "target=/src"]) {
      const image = new Image({
        name: "mount-image",
        baseImage: "python:3.12-slim",
      }).run("make -C /src", { mount });

      makeFakeBinding();
      await expect(createSandboxImage(image)).rejects.toThrow(/contextDir/);
    }
  });

  it("does not require a context for a RUN mount with from=<stage>", async () => {
    const image = new Image({
      name: "mount-stage",
      baseImage: "python:3.12-slim",
    }).run("make", { mount: "type=bind,from=builder,target=/src" });

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    expect(captured.contextFiles).toEqual([]);
  });

  it("does not require a context for a RUN cache mount", async () => {
    const image = new Image({
      name: "mount-cache",
      baseImage: "python:3.12-slim",
    }).run("pip install -r req.txt", { mount: "type=cache,target=/root/.cache" });

    const { captured } = makeFakeBinding();
    await createSandboxImage(image);

    expect(captured.contextFiles).toEqual([]);
  });

  it("uploads the given directory as-is when contextDir is set with COPY ops", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-ctxcopy`),
      { recursive: true },
    );
    await writeFile(path.join(tempDir, "requirements.txt"), "flask\n", "utf8");

    const image = new Image({
      name: "copy-image",
      baseImage: "python:3.12-slim",
    }).copy("requirements.txt", "/tmp/requirements.txt");

    const { captured } = makeFakeBinding();
    await createSandboxImage(image, { contextDir: tempDir as string });

    expect(captured.options.contextDir).toBe(path.resolve(tempDir as string));
  });

  it("forwards dockerCompat to the native build binding", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-compat`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath, { dockerCompat: true });

    expect(captured.options.dockerCompat).toBe(true);
  });

  it("forwards cas and buildArgs to the native build binding, defaulting to a CAS image", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-cas`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(
      dockerfilePath,
      "ARG PY_TAG=3.12-slim\nFROM python:${PY_TAG}\n",
      "utf8",
    );

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath, {
      cas: true,
      buildArgs: { PY_TAG: "3.13" },
    });
    expect(captured.options.cas).toBe(true);
    expect(captured.options.buildArgs).toEqual({ PY_TAG: "3.13" });

    await createSandboxImage(dockerfilePath);
    expect(captured.options.cas).toBe(true);
    expect(captured.options.buildArgs).toBeUndefined();

    await createSandboxImage(dockerfilePath, { cas: false });
    expect(captured.options.cas).toBe(false);
  });

  it("forwards native events back through the user emit callback", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-emit`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "my-img.Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim\nRUN echo hi\n",
      "utf8",
    );

    makeFakeBinding({
      events: [
        { eventType: "status", message: "Preparing rootfs build..." },
        { eventType: "build_log", stream: "stdout", message: "build line" },
        { eventType: "warning", message: "Skipping LABEL" },
      ],
    });

    const events: Array<Record<string, unknown>> = [];
    await createSandboxImage(dockerfilePath, {}, { emit: (e) => events.push(e) });

    expect(events).toEqual([
      { type: "status", message: "Building CAS image 'my-img'..." },
      { type: "status", message: "Preparing rootfs build..." },
      { type: "build_log", stream: "stdout", message: "build line" },
      { type: "warning", message: "Skipping LABEL" },
      { type: "image_registered", name: "my-img", image_id: "tpl-1" },
      { type: "done" },
    ]);
  });

  it("rejects with a user emit error after native event forwarding without emitting completion", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-emit-error`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "my-img.Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim\nRUN echo hi\n",
      "utf8",
    );

    makeFakeBinding({
      events: [
        { eventType: "status", message: "Preparing rootfs build..." },
        { eventType: "build_log", stream: "stdout", message: "build line" },
      ],
    });

    const emitError = new Error("emit failed");
    const events: Array<Record<string, unknown>> = [];
    await expect(
      createSandboxImage(dockerfilePath, {}, {
        emit: (event) => {
          events.push(event);
          if (event.type === "status" && event.message === "Preparing rootfs build...") {
            throw emitError;
          }
        },
      }),
    ).rejects.toBe(emitError);

    expect(events).toEqual([
      { type: "status", message: "Building CAS image 'my-img'..." },
      { type: "status", message: "Preparing rootfs build..." },
      { type: "build_log", stream: "stdout", message: "build line" },
    ]);
  });

  it("derives the registered name from the parent dir when the file is named Dockerfile", async () => {
    const baseDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-parent`),
      { recursive: true },
    );
    const appDir = path.join(baseDir, "weather-app");
    await mkdir(appDir, { recursive: true });
    const dockerfilePath = path.join(appDir, "Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim\nRUN echo hi\n",
      "utf8",
    );

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath, {});

    expect(captured.options.registeredName).toBe("weather-app");
  });

  it("uses the explicit registeredName when provided", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-name`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim\nRUN echo hi\n",
      "utf8",
    );

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath, { registeredName: "override" });
    expect(captured.options.registeredName).toBe("override");
  });

  it("throws when the Dockerfile path does not exist", async () => {
    makeFakeBinding();
    await expect(createSandboxImage("/nonexistent/Dockerfile")).rejects.toThrow(
      /Dockerfile not found/,
    );
  });

  it("validates a missing Dockerfile before checking credentials", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "");
    makeFakeBinding();

    await expect(createSandboxImage("/nonexistent/Dockerfile")).rejects.toThrow(
      /Dockerfile not found/,
    );
  });

  it("throws when no credentials are configured", async () => {
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-noauth`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "");
    makeFakeBinding();

    await expect(createSandboxImage(dockerfilePath)).rejects.toThrow(
      /Missing TENSORLAKE_API_KEY/,
    );
  });

  it("rejects PAT-only SDK auth", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "tl_pat_xyz");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_1");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_1");

    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-pat`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    makeFakeBinding();
    await expect(createSandboxImage(dockerfilePath)).rejects.toThrow(
      /Personal access tokens are CLI-only/,
    );
  });

  it("allows PAT auth from the standalone CLI", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "tl_pat_xyz");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_1");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_1");
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);

    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-cli-pat`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    const { captured } = makeFakeBinding();
    await runCreateSandboxImageCli([dockerfilePath]);

    expect(captured.options).toMatchObject({
      bearerToken: "tl_pat_xyz",
      organizationId: "org_1",
      projectId: "proj_1",
      useScopeHeaders: true,
    });
  });

  it("prefers API key auth without PAT scope headers when both are set", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_apiKey_test");
    vi.stubEnv("TENSORLAKE_PAT", "tl_pat_test");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_stale");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "project_stale");

    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-api-key-precedence`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath);
    expect(captured.options.bearerToken).toBe("tl_apiKey_test");
    expect(captured.options.useScopeHeaders).toBe(false);
  });

  it("rejects an unknown source type", async () => {
    makeFakeBinding();
    await expect(
      // @ts-expect-error - intentionally invalid input
      createSandboxImage(12345),
    ).rejects.toThrow(TypeError);
  });

  it("validates an unknown source type before checking credentials", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "");
    makeFakeBinding();

    await expect(
      // @ts-expect-error - intentionally invalid input
      createSandboxImage(12345),
    ).rejects.toThrow(TypeError);
  });

  it("warns once for default-named Image.build", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const image = new Image({ baseImage: "python:3.12-slim" });
    makeFakeBinding();

    await image.build();

    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[0]).toMatch(/default name/);
  });

  it("warns once for direct createSandboxImage with a default-named Image", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const image = new Image({ baseImage: "python:3.12-slim" });
    makeFakeBinding();

    await createSandboxImage(image);

    expect(warn).toHaveBeenCalledOnce();
    expect(warn.mock.calls[0]?.[0]).toMatch(/default name/);
  });
});

describe("importSandboxImage", () => {
  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
    vi.restoreAllMocks();
  });

  it("delegates an image reference to the native import binding", async () => {
    const { binding, captured } = makeFakeBinding();
    const result = await importSandboxImage(
      "pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
      { isPublic: true },
    );

    expect(binding.importSandboxImage).toHaveBeenCalledOnce();
    expect(binding.buildSandboxImage).not.toHaveBeenCalled();
    expect(captured.options).toMatchObject({
      apiUrl: "https://api.tensorlake.test",
      bearerToken: "tl_key_test",
      imageReference: "pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
      registeredName: "pytorch",
      isPublic: true,
      dockerCompat: false,
    });
    // The import option shape carries no Dockerfile fields.
    expect(captured.options).not.toHaveProperty("dockerfilePath");
    expect(captured.options).not.toHaveProperty("importImageReference");
    expect(result).toEqual({ id: "tpl-1", snapshot_id: "snap-1" });
  });

  it("forwards cas to the native import binding, defaulting to a CAS image", async () => {
    const { captured } = makeFakeBinding();
    await importSandboxImage("ubuntu:24.04", { cas: true });
    expect(captured.options.cas).toBe(true);
    await importSandboxImage("ubuntu:24.04");
    expect(captured.options.cas).toBe(true);
    await importSandboxImage("ubuntu:24.04", { cas: false });
    expect(captured.options.cas).toBe(false);
  });

  it("derives the registered name from the reference, stripping tag and registry path", async () => {
    const { captured } = makeFakeBinding();
    await importSandboxImage("ghcr.io/org/app@sha256:abc123");
    expect(captured.options.registeredName).toBe("app");
  });

  it("uses the explicit registeredName when provided", async () => {
    const { captured } = makeFakeBinding();
    await importSandboxImage("pytorch/pytorch:2.4.1", {
      registeredName: "override",
    });
    expect(captured.options.registeredName).toBe("override");
  });

  it("forwards dockerCompat to the native import binding", async () => {
    const { captured } = makeFakeBinding();
    await importSandboxImage("pytorch/pytorch:2.4.1", {
      dockerCompat: true,
    });
    expect(captured.options.dockerCompat).toBe(true);
  });

  it("forwards native events back through the user emit callback", async () => {
    makeFakeBinding({
      events: [
        { eventType: "status", message: "Pulling layers..." },
        { eventType: "build_log", stream: "stdout", message: "applied layer" },
      ],
    });

    const events: Array<Record<string, unknown>> = [];
    await importSandboxImage(
      "pytorch/pytorch:2.4.1",
      {},
      { emit: (e) => events.push(e) },
    );

    expect(events).toEqual([
      {
        type: "status",
        message: "Importing 'pytorch/pytorch:2.4.1' as CAS image 'pytorch'...",
      },
      { type: "status", message: "Pulling layers..." },
      { type: "build_log", stream: "stdout", message: "applied layer" },
      { type: "image_registered", name: "pytorch", image_id: "tpl-1" },
      { type: "done" },
    ]);
  });

  it("throws on an empty image reference", async () => {
    makeFakeBinding();
    await expect(importSandboxImage("   ")).rejects.toThrow(
      /image reference to import must not be empty/,
    );
  });

  it("throws when no credentials are configured", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "");
    makeFakeBinding();

    await expect(importSandboxImage("pytorch/pytorch:2.4.1")).rejects.toThrow(
      /Missing TENSORLAKE_API_KEY/,
    );
  });

  it("allows PAT auth from the standalone import CLI", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "tl_pat_xyz");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_1");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_1");
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);

    const { captured } = makeFakeBinding();
    await runImportSandboxImageCli(["pytorch/pytorch:2.4.1"]);

    expect(captured.options).toMatchObject({
      bearerToken: "tl_pat_xyz",
      organizationId: "org_1",
      projectId: "proj_1",
      useScopeHeaders: true,
    });
  });

  it("builds and imports CAS images from the standalone CLIs unless --no-cas is passed", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_apiKey_test");
    vi.spyOn(process.stdout, "write").mockImplementation(() => true);
    const tempDir = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-cli-cas`),
      { recursive: true },
    );
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\n", "utf8");

    const { captured } = makeFakeBinding();
    await runCreateSandboxImageCli([dockerfilePath]);
    expect(captured.options.cas).toBe(true);
    await runCreateSandboxImageCli([dockerfilePath, "--no-cas"]);
    expect(captured.options.cas).toBe(false);
    await runImportSandboxImageCli(["ubuntu:24.04"]);
    expect(captured.options.cas).toBe(true);
    await runImportSandboxImageCli(["ubuntu:24.04", "--no-cas"]);
    expect(captured.options.cas).toBe(false);
  });
});

describe("sandbox image catalog", () => {
  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
    vi.restoreAllMocks();
  });

  const record = {
    id: "image_abc",
    name: "team/app",
    snapshotId: "snap-1",
    snapshotFormatVersion: "content_addressed_streaming_v1",
    rootfsDiskBytes: 1024,
    public: false,
    createdAt: "2026-10-15T00:00:00.000Z",
  };

  it("lists both families through the native catalog", async () => {
    const { binding } = makeFakeBinding({ listJson: JSON.stringify([record]) });
    const images = await listSandboxImages();
    expect(images).toEqual([record]);
    expect(binding.listSandboxImages).toHaveBeenCalledWith({
      apiUrl: "https://api.tensorlake.test",
      bearerToken: "tl_key_test",
      useScopeHeaders: false,
    });
  });

  it("finds one image by name or id and returns null when unknown", async () => {
    const { binding } = makeFakeBinding({ findJson: JSON.stringify(record) });
    expect(await findSandboxImageByName("team/app")).toEqual(record);
    expect(binding.findSandboxImage).toHaveBeenCalledWith(
      expect.objectContaining({ bearerToken: "tl_key_test" }),
      "team/app",
    );
    makeFakeBinding({ findJson: null });
    expect(await findSandboxImageByName("missing")).toBeNull();
    await expect(findSandboxImageByName("")).rejects.toThrow(TypeError);
  });

  it("deletes through the native catalog and surfaces its errors", async () => {
    const { binding } = makeFakeBinding();
    await deleteSandboxImage("team/app");
    expect(binding.deleteSandboxImage).toHaveBeenCalledWith(
      expect.objectContaining({ apiUrl: "https://api.tensorlake.test" }),
      "team/app",
    );
    binding.deleteSandboxImage.mockRejectedValueOnce(
      new Error('{"category":"remote_api","status":404,"message":"image \'missing\' not found"}'),
    );
    await expect(deleteSandboxImage("missing")).rejects.toThrow("not found");
  });

  it("warns, without rejecting, when a name's same-named legacy image could not be deleted", async () => {
    const { binding } = makeFakeBinding();
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    await deleteSandboxImage("app");
    expect(warn).not.toHaveBeenCalled();

    binding.deleteSandboxImage.mockResolvedValueOnce({
      legacyImageId: "sandbox_template_1",
      error: "Server error: 409 Conflict - retry the delete",
    });
    await expect(deleteSandboxImage("app")).resolves.toBeUndefined();
    expect(warn).toHaveBeenCalledTimes(1);
    const message = String(warn.mock.calls[0]![0]);
    expect(message).toContain("Deleted CAS image 'app'");
    expect(message).toContain("legacy image of the same name (sandbox_template_1)");
    expect(message).toContain("409 Conflict - retry the delete");
    expect(message).toContain('deleteSandboxImage("sandbox_template_1")');

    binding.deleteSandboxImage.mockResolvedValueOnce({
      legacyImageId: null,
      error: "Server error: 500 - internal error",
    });
    await deleteSandboxImage("app");
    const unnamed = String(warn.mock.calls[1]![0]);
    expect(unnamed).toContain("legacy image of the same name, which");
    expect(unnamed).toContain('deleteSandboxImage("app")');
  });

  it("requires an API key", async () => {
    vi.stubEnv("TENSORLAKE_API_KEY", "");
    vi.stubEnv("TENSORLAKE_PAT", "tl_pat_test");
    makeFakeBinding();
    await expect(listSandboxImages()).rejects.toThrow("Personal access tokens are CLI-only");
    vi.stubEnv("TENSORLAKE_PAT", "");
    await expect(findSandboxImageByName("x")).rejects.toThrow("Missing TENSORLAKE_API_KEY");
  });
});

describe("dockerfileContent", () => {
  it("renders an Image DSL definition", () => {
    const image = new Image({
      name: "weather-image",
      baseImage: "python:3.12-slim",
    })
      .run("apt-get update")
      .workdir("/app")
      .env("APP_ENV", "prod")
      .copy("./src", "/app/src");

    expect(dockerfileContent(image)).toBe(
      [
        "FROM python:3.12-slim",
        "RUN apt-get update",
        "WORKDIR /app",
        'ENV APP_ENV="prod"',
        "COPY ./src /app/src",
      ].join("\n"),
    );
  });

  it("omits FROM when no base image is set", () => {
    const image = new Image({ name: "no-base" }).run("echo hi");
    // baseImage is undefined → dockerfileContent emits no FROM line.
    expect(dockerfileContent(image).split("\n")[0]).toBe("RUN echo hi");
  });
});

describe("CloudClient sandbox image methods", () => {
  beforeEach(() => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://env.tensorlake.test");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_env");
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    __setNativeBindingForTest(undefined);
  });

  const record = { id: "image_1", name: "team/app", snapshotId: "snap-1" };

  it("keeps deleteSandboxImage, findSandboxImageByName and listSandboxImages as merged-catalog wrappers with the client's credentials", async () => {
    const { binding } = makeFakeBinding({
      listJson: JSON.stringify([record]),
      findJson: JSON.stringify(record),
    });
    const client = new CloudClient({
      apiUrl: "https://client.tensorlake.test",
      apiKey: "tl_key_client",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    // The client's explicit scope addresses the project's routes directly,
    // as the previous implementation did.
    const credentials = {
      apiUrl: "https://client.tensorlake.test",
      bearerToken: "tl_key_client",
      organizationId: "org-1",
      projectId: "proj-1",
      useScopeHeaders: true,
    };

    expect(await client.listSandboxImages()).toEqual([record]);
    expect(binding.listSandboxImages).toHaveBeenCalledWith(credentials);

    expect(await client.findSandboxImageByName("team/app")).toEqual(record);
    expect(binding.findSandboxImage).toHaveBeenCalledWith(credentials, "team/app");
    binding.findSandboxImage.mockResolvedValueOnce(null);
    expect(await client.findSandboxImageByName("missing")).toBeNull();
    await expect(client.findSandboxImageByName("")).rejects.toThrow(TypeError);

    await client.deleteSandboxImage("team/app");
    expect(binding.deleteSandboxImage).toHaveBeenCalledWith(credentials, "team/app");
    await expect(client.deleteSandboxImage("")).rejects.toThrow(TypeError);

    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    binding.deleteSandboxImage.mockResolvedValueOnce({
      legacyImageId: "sandbox_template_1",
      error: "Server error: 409 Conflict",
    });
    await client.deleteSandboxImage("team/app");
    expect(String(warn.mock.calls[0]![0])).toContain("Deleted CAS image 'team/app'");
    warn.mockRestore();
    client.close();
  });

  it("lets a call override the client's organization and project scope", async () => {
    const { binding } = makeFakeBinding({ listJson: "[]" });
    const client = new CloudClient({
      apiUrl: "https://client.tensorlake.test",
      apiKey: "tl_key_client",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    await client.listSandboxImages({ organizationId: "org-2", projectId: "proj-2" });
    expect(binding.listSandboxImages).toHaveBeenLastCalledWith({
      apiUrl: "https://client.tensorlake.test",
      bearerToken: "tl_key_client",
      organizationId: "org-2",
      projectId: "proj-2",
      useScopeHeaders: true,
    });
    await client.findSandboxImageByName("team/app", { projectId: "proj-3" });
    expect(binding.findSandboxImage).toHaveBeenLastCalledWith(
      {
        apiUrl: "https://client.tensorlake.test",
        bearerToken: "tl_key_client",
        organizationId: "org-1",
        projectId: "proj-3",
        useScopeHeaders: true,
      },
      "team/app",
    );
    client.close();
  });

  it("scopes an explicit pair given only per call and refuses half a pair", async () => {
    const { binding } = makeFakeBinding({ listJson: "[]" });
    const unscoped = new CloudClient({
      apiUrl: "https://client.tensorlake.test",
      apiKey: "tl_key_client",
    });
    await unscoped.listSandboxImages();
    expect(binding.listSandboxImages).toHaveBeenLastCalledWith({
      apiUrl: "https://client.tensorlake.test",
      bearerToken: "tl_key_client",
      organizationId: undefined,
      projectId: undefined,
      useScopeHeaders: false,
    });
    await unscoped.listSandboxImages({ organizationId: "org-9", projectId: "proj-9" });
    expect(binding.listSandboxImages).toHaveBeenLastCalledWith({
      apiUrl: "https://client.tensorlake.test",
      bearerToken: "tl_key_client",
      organizationId: "org-9",
      projectId: "proj-9",
      useScopeHeaders: true,
    });
    await expect(unscoped.listSandboxImages({ projectId: "proj-9" })).rejects.toThrow(
      "organizationId and projectId must be given together",
    );
    expect(binding.listSandboxImages).toHaveBeenCalledTimes(2);
    unscoped.close();
  });

  it("falls back to the default URL and the environment's key when the client sets none", async () => {
    const { binding } = makeFakeBinding({ listJson: "[]" });
    const client = new CloudClient();
    await client.listSandboxImages();
    // The URL default is read when the module loads; the key falls back to
    // the environment through the catalog's credential resolution.
    expect(binding.listSandboxImages).toHaveBeenCalledWith({
      apiUrl: API_URL,
      bearerToken: "tl_key_env",
      organizationId: undefined,
      projectId: undefined,
      useScopeHeaders: false,
    });
    client.close();
  });
});
