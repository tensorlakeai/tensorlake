import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createSandboxImage,
  loadImagePlan,
  loadDockerfilePlan,
  logicalDockerfileLines,
} from "../src/sandbox-image.js";
import { Image, dockerfileContent } from "../src/image.js";

describe("sandbox image helpers", () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.restoreAllMocks();
  });

  it("logicalDockerfileLines merges continuations", () => {
    const dockerfile = `
      # comment
      FROM python:3.12-slim
      RUN apt-get update \\
          && apt-get install -y curl

      ENV A=1 B=two
    `.trim();

    expect(logicalDockerfileLines(dockerfile)).toEqual([
      { lineNumber: 2, line: "FROM python:3.12-slim" },
      { lineNumber: 3, line: "RUN apt-get update && apt-get install -y curl" },
      { lineNumber: 6, line: "ENV A=1 B=two" },
    ]);
  });

  it("loadDockerfilePlan defaults name from parent directory", async () => {
    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}`), {
      recursive: true,
    });
    const appDir = path.join(tempDir, "weather-app");
    await mkdir(appDir, { recursive: true });
    const dockerfilePath = path.join(appDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\nRUN echo hi\n", "utf8");

    const plan = await loadDockerfilePlan(dockerfilePath);
    expect(plan.baseImage).toBe("python:3.12-slim");
    expect(plan.registeredName).toBe("weather-app");
    expect(plan.instructions).toEqual([
      { keyword: "RUN", value: "echo hi", lineNumber: 2 },
    ]);
  });

  it("loadDockerfilePlan rejects multistage Dockerfiles", async () => {
    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-multi`), {
      recursive: true,
    });
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(
      dockerfilePath,
      "FROM python:3.12-slim AS build\nFROM debian:bookworm-slim\n",
      "utf8",
    );

    await expect(loadDockerfilePlan(dockerfilePath)).rejects.toThrow(
      /multi-stage Dockerfiles are not supported/,
    );
  });

  it("renders a Dockerfile from the Image DSL", () => {
    const image = new Image({
      name: "data-tools",
      baseImage: "ubuntu-systemd",
    })
      .workdir("/workspace")
      .env("APP_ENV", "prod")
      .copy("requirements.txt", "/tmp/requirements.txt")
      .run("python3 -m pip install --break-system-packages -r /tmp/requirements.txt");

    expect(dockerfileContent(image)).toBe([
      "FROM ubuntu-systemd",
      "WORKDIR /workspace",
      'ENV APP_ENV="prod"',
      "COPY requirements.txt /tmp/requirements.txt",
      "RUN python3 -m pip install --break-system-packages -r /tmp/requirements.txt",
    ].join("\n"));
  });

  it("loadImagePlan derives build plan from the Image DSL", () => {
    const image = new Image({
      name: "data-tools",
      baseImage: "ubuntu-systemd",
    })
      .workdir("/workspace")
      .copy("requirements.txt", "/tmp/requirements.txt")
      .run("echo ready");

    const plan = loadImagePlan(image, {
      contextDir: "/tmp/project",
    });

    expect(plan.baseImage).toBe("ubuntu-systemd");
    expect(plan.registeredName).toBe("data-tools");
    expect(plan.contextDir).toBe("/tmp/project");
    expect(plan.instructions).toEqual([
      { keyword: "WORKDIR", value: "/workspace", lineNumber: 2 },
      {
        keyword: "COPY",
        value: "requirements.txt /tmp/requirements.txt",
        lineNumber: 3,
      },
      { keyword: "RUN", value: "echo ready", lineNumber: 4 },
    ]);
  });

  it("createSandboxImage registers snapshot from Dockerfile", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_123");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_123");

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-create`), {
      recursive: true,
    });
    const dockerfilePath = path.join(tempDir, "sandbox-image.Dockerfile");
    const dockerfileText = [
      "FROM python:3.12-slim",
      "WORKDIR /app",
      "COPY . /app",
      "RUN python -c \"print('hello')\"",
    ].join("\n");
    await writeFile(dockerfilePath, `${dockerfileText}\n`, "utf8");
    await writeFile(path.join(tempDir, "hello.txt"), "hi", "utf8");

    const writeFileMock = vi.fn(async () => {});
    const sandbox = {
      sandboxId: "sbx-1",
      run: vi.fn(async () => ({ exitCode: 0, stdout: "", stderr: "" })),
      startProcess: vi.fn(async () => ({ pid: 1 })),
      getStdout: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getStderr: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getProcess: vi.fn(async () => ({
        pid: 1,
        status: "exited",
        stdinWritable: false,
        command: "sh",
        args: [],
        startedAt: new Date(),
        exitCode: 0,
      })),
      writeFile: writeFileMock,
      terminate: vi.fn(async () => {}),
    };

    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      snapshotAndWait: vi.fn(async () => ({
        snapshotId: "snap-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
      })),
      close: vi.fn(() => {}),
    };

    const registerImage = vi.fn(async () => ({ id: "tpl-1" }));

    await createSandboxImage(
      dockerfilePath,
      {},
      {
        emit: () => {},
        createClient: () => client as never,
        registerImage,
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "python:3.12-slim",
      cpus: 2.0,
      memoryMb: 4096,
    });
    expect(client.snapshotAndWait).toHaveBeenCalledWith("sbx-1");
    expect(registerImage).toHaveBeenCalledWith(
      expect.objectContaining({
        organizationId: "org_123",
        projectId: "proj_123",
      }),
      "sandbox-image",
      `${dockerfileText}\n`,
      "snap-1",
      "s3://snapshots/snap-1.tar.zst",
      false,
    );
    expect(sandbox.terminate).toHaveBeenCalled();
    expect(writeFileMock).toHaveBeenCalled();
  });

  it("createSandboxImage accepts an Image DSL definition", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_123");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_123");

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-dsl`), {
      recursive: true,
    });
    await writeFile(path.join(tempDir, "hello.txt"), "hi", "utf8");

    const image = new Image({
      name: "dsl-image",
      baseImage: "ubuntu-systemd",
    })
      .workdir("/workspace")
      .copy("hello.txt", "/workspace/hello.txt")
      .run("cat /workspace/hello.txt");

    const writeFileMock = vi.fn(async () => {});
    const sandbox = {
      sandboxId: "sbx-1",
      run: vi.fn(async () => ({ exitCode: 0, stdout: "", stderr: "" })),
      startProcess: vi.fn(async () => ({ pid: 1 })),
      getStdout: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getStderr: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getProcess: vi.fn(async () => ({
        pid: 1,
        status: "exited",
        stdinWritable: false,
        command: "sh",
        args: [],
        startedAt: new Date(),
        exitCode: 0,
      })),
      writeFile: writeFileMock,
      terminate: vi.fn(async () => {}),
    };

    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      snapshotAndWait: vi.fn(async () => ({
        snapshotId: "snap-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
      })),
      close: vi.fn(() => {}),
    };

    const registerImage = vi.fn(async () => ({ id: "tpl-1" }));

    await createSandboxImage(
      image,
      { contextDir: tempDir },
      {
        emit: () => {},
        createClient: () => client as never,
        registerImage,
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "ubuntu-systemd",
      cpus: 2.0,
      memoryMb: 4096,
    });
    expect(writeFileMock).toHaveBeenCalledWith(
      "/workspace/hello.txt",
      expect.any(Uint8Array),
    );
    expect(registerImage).toHaveBeenCalledWith(
      expect.objectContaining({
        organizationId: "org_123",
        projectId: "proj_123",
      }),
      "dsl-image",
      [
        "FROM ubuntu-systemd",
        "WORKDIR /workspace",
        "COPY hello.txt /workspace/hello.txt",
        "RUN cat /workspace/hello.txt",
      ].join("\n"),
      "snap-1",
      "s3://snapshots/snap-1.tar.zst",
      false,
    );
    expect(sandbox.terminate).toHaveBeenCalled();
  });
});
