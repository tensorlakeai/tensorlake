import { existsSync, readdirSync } from "node:fs";
import { mkdir, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __setNativeBindingForTest,
  createSandboxImage,
  importSandboxImage,
} from "../src/sandbox-image.js";
import { Image, dockerfileContent } from "../src/image.js";

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
    buildSandboxImage: vi.fn(handler),
    importSandboxImage: vi.fn(handler),
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

  it("stages only referenced files into the minimal context (no contextDir)", async () => {
    // Without contextDir, the build context is assembled from just the
    // COPY/ADD sources (resolved against cwd) — unrelated files in cwd must
    // NOT be uploaded.
    const tempCwd = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-mincopy`),
      { recursive: true },
    );
    await writeFile(path.join(tempCwd, "requirements.txt"), "flask\n", "utf8");
    await writeFile(path.join(tempCwd, "secret.env"), "nope", "utf8");

    const image = new Image({
      name: "copy-image",
      baseImage: "python:3.12-slim",
    }).copy("requirements.txt", "/tmp/requirements.txt");

    const { captured } = makeFakeBinding();
    const spy = vi.spyOn(process, "cwd").mockReturnValue(tempCwd as string);
    try {
      await createSandboxImage(image);
    } finally {
      spy.mockRestore();
    }

    expect(captured.contextFiles).toEqual(["requirements.txt"]);
  });

  it("expands globs and copies directories into the minimal context", async () => {
    const tempCwd = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-globdir`),
      { recursive: true },
    );
    await writeFile(path.join(tempCwd, "a.txt"), "a", "utf8");
    await writeFile(path.join(tempCwd, "b.txt"), "b", "utf8");
    await writeFile(path.join(tempCwd, "skip.md"), "m", "utf8");
    await mkdir(path.join(tempCwd, "src", "pkg"), { recursive: true });
    await writeFile(path.join(tempCwd, "src", "main.py"), "print()", "utf8");
    await writeFile(path.join(tempCwd, "src", "pkg", "util.py"), "x=1", "utf8");
    await writeFile(path.join(tempCwd, "ignored.py"), "nope", "utf8");

    const image = new Image({
      name: "glob-image",
      baseImage: "python:3.12-slim",
    })
      .copy("*.txt", "/app/")
      .copy("./src", "/app/src");

    const { captured } = makeFakeBinding();
    const spy = vi.spyOn(process, "cwd").mockReturnValue(tempCwd as string);
    try {
      await createSandboxImage(image);
    } finally {
      spy.mockRestore();
    }

    expect(captured.contextFiles).toEqual([
      "a.txt",
      "b.txt",
      "src/main.py",
      "src/pkg/util.py",
    ]);
  });

  it("stages a symlinked source at the path the Dockerfile names", async () => {
    // A COPY source that is a symlink must be staged at the link path the
    // Dockerfile references (with the target's contents), not left as a
    // dangling link in the throwaway context.
    const tempCwd = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-symlink`),
      { recursive: true },
    );
    await writeFile(path.join(tempCwd, "target.txt"), "payload", "utf8");
    await symlink(
      path.join(tempCwd, "target.txt"),
      path.join(tempCwd, "link.txt"),
    );

    const image = new Image({
      name: "link-image",
      baseImage: "python:3.12-slim",
    }).copy("link.txt", "/app/link.txt");

    const { captured } = makeFakeBinding();
    const spy = vi.spyOn(process, "cwd").mockReturnValue(tempCwd as string);
    try {
      await createSandboxImage(image);
    } finally {
      spy.mockRestore();
    }

    expect(captured.contextFiles).toEqual(["link.txt"]);
  });

  it("carries cwd's .dockerignore into the minimal context", async () => {
    // Without contextDir the staged context must include cwd's .dockerignore
    // so the native archiver applies the same exclusions it would for a
    // full-cwd context.
    const tempCwd = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-dockerignore`),
      { recursive: true },
    );
    await mkdir(path.join(tempCwd, "src"), { recursive: true });
    await writeFile(path.join(tempCwd, "src", "main.py"), "print()", "utf8");
    await writeFile(path.join(tempCwd, "src", "secret.env"), "nope", "utf8");
    await writeFile(path.join(tempCwd, ".dockerignore"), "src/secret.env\n", "utf8");

    const image = new Image({
      name: "ignore-image",
      baseImage: "python:3.12-slim",
    }).copy("./src", "/app/src");

    const { captured } = makeFakeBinding();
    const spy = vi.spyOn(process, "cwd").mockReturnValue(tempCwd as string);
    try {
      await createSandboxImage(image);
    } finally {
      spy.mockRestore();
    }

    // The .dockerignore is staged at the context root; the actual exclusion is
    // enforced by the native archiver, which reads it from there.
    expect(captured.contextFiles).toContain(".dockerignore");
  });

  it("throws when a COPY source does not exist", async () => {
    const tempCwd = await mkdir(
      path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-missing`),
      { recursive: true },
    );
    const image = new Image({
      name: "missing-image",
      baseImage: "python:3.12-slim",
    }).copy("does-not-exist.txt", "/tmp/x");

    makeFakeBinding();
    const spy = vi.spyOn(process, "cwd").mockReturnValue(tempCwd as string);
    try {
      await expect(createSandboxImage(image)).rejects.toThrow(
        /does-not-exist\.txt/,
      );
    } finally {
      spy.mockRestore();
    }
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
      { type: "status", message: "Building image 'my-img'..." },
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
      { type: "status", message: "Building image 'my-img'..." },
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
      /Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT/,
    );
  });

  it("uses PAT auth with scope headers when only TENSORLAKE_PAT is set", async () => {
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

    const { captured } = makeFakeBinding();
    await createSandboxImage(dockerfilePath);
    expect(captured.options.bearerToken).toBe("tl_pat_xyz");
    expect(captured.options.useScopeHeaders).toBe(true);
    expect(captured.options.organizationId).toBe("org_1");
    expect(captured.options.projectId).toBe("proj_1");
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
        message: "Importing image 'pytorch/pytorch:2.4.1' as 'pytorch'...",
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
      /Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT/,
    );
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
