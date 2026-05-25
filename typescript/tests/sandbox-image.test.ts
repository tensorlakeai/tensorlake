import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  __setNativeBindingForTest,
  createSandboxImage,
} from "../src/sandbox-image.js";
import { Image, dockerfileContent } from "../src/image.js";

interface CapturedCall {
  options: Record<string, unknown>;
  emit?: ((event: { eventType: string; stream?: string | null; message: string }) => void) | null;
}

function makeFakeBinding(opts: {
  resultJson?: string;
  events?: Array<{ eventType: string; stream?: string | null; message: string }>;
} = {}) {
  const captured: CapturedCall = { options: {} };
  const binding = {
    buildSandboxImage: vi.fn(
      async (
        options: Record<string, unknown>,
        emit?:
          | ((event: { eventType: string; stream?: string | null; message: string }) => void)
          | null,
      ) => {
        captured.options = options;
        captured.emit = emit;
        if (emit && opts.events) {
          for (const event of opts.events) emit(event);
        }
        return opts.resultJson ?? '{"id":"tpl-1","snapshot_id":"snap-1"}';
      },
    ),
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
