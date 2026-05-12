import { mkdir, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
  createSandboxImage,
  loadImagePlan,
  loadDockerfilePlan,
  logicalDockerfileLines,
  registerImage,
  type BuildContext,
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
      baseImage: "tensorlake/ubuntu-systemd",
    })
      .workdir("/workspace")
      .env("APP_ENV", "prod")
      .copy("requirements.txt", "/tmp/requirements.txt")
      .run("python3 -m pip install --break-system-packages -r /tmp/requirements.txt");

    expect(dockerfileContent(image)).toBe([
      "FROM tensorlake/ubuntu-systemd",
      "WORKDIR /workspace",
      'ENV APP_ENV="prod"',
      "COPY requirements.txt /tmp/requirements.txt",
      "RUN python3 -m pip install --break-system-packages -r /tmp/requirements.txt",
    ].join("\n"));
  });

  it("omits FROM when the Image DSL defers base-image selection", () => {
    const image = new Image({ name: "default-build" })
      .workdir("/workspace")
      .run("echo ready");

    expect(dockerfileContent(image)).toBe([
      "WORKDIR /workspace",
      "RUN echo ready",
    ].join("\n"));
  });

  it("loadImagePlan derives build plan from the Image DSL", () => {
    const image = new Image({
      name: "data-tools",
      baseImage: "tensorlake/ubuntu-systemd",
    })
      .workdir("/workspace")
      .copy("requirements.txt", "/tmp/requirements.txt")
      .run("echo ready");

    const plan = loadImagePlan(image, {
      contextDir: "/tmp/project",
    });

    expect(plan.baseImage).toBe("tensorlake/ubuntu-systemd");
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

  it("loadImagePlan supports Image DSL definitions without an explicit base image", () => {
    const image = new Image({ name: "default-build" })
      .workdir("/workspace")
      .run("echo ready");

    const plan = loadImagePlan(image, {
      contextDir: "/tmp/project",
    });

    expect(plan.baseImage).toBeUndefined();
    expect(plan.instructions).toEqual([
      { keyword: "WORKDIR", value: "/workspace", lineNumber: 1 },
      { keyword: "RUN", value: "echo ready", lineNumber: 2 },
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
        sandboxId: "sbx-source-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
        snapshotFormatVersion: "durable_archive_v1",
        sizeBytes: 123,
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
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
    // Regression: sandbox image builds MUST request a filesystem-only
    // snapshot so restored sandboxes cold-boot (see PR #583).
    expect(client.snapshotAndWait).toHaveBeenCalledWith(
      "sbx-1",
      expect.objectContaining({ snapshotType: "filesystem" }),
    );
    expect(registerImage).toHaveBeenCalledWith(
      expect.objectContaining({
        organizationId: "org_123",
        projectId: "proj_123",
      }),
      "sandbox-image",
      `${dockerfileText}\n`,
      "snap-1",
      "sbx-source-1",
      "s3://snapshots/snap-1.tar.zst",
      123,
      10 * 1024 * 1024 * 1024,
      false,
      "durable_archive_v1",
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
      baseImage: "tensorlake/ubuntu-systemd",
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
        sandboxId: "sbx-source-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
        snapshotFormatVersion: "durable_archive_v1",
        sizeBytes: 123,
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
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
      image: "tensorlake/ubuntu-systemd",
      cpus: 2.0,
      memoryMb: 4096,
    });
    expect(client.snapshotAndWait).toHaveBeenCalledWith(
      "sbx-1",
      expect.objectContaining({ snapshotType: "filesystem" }),
    );
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
        "FROM tensorlake/ubuntu-systemd",
        "WORKDIR /workspace",
        "COPY hello.txt /workspace/hello.txt",
        "RUN cat /workspace/hello.txt",
      ].join("\n"),
      "snap-1",
      "sbx-source-1",
      "s3://snapshots/snap-1.tar.zst",
      123,
      10 * 1024 * 1024 * 1024,
      false,
      "durable_archive_v1",
    );
    expect(sandbox.terminate).toHaveBeenCalled();
  });

  it("createSandboxImage forwards custom disk size to the build sandbox", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_123");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_123");

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-disk`), {
      recursive: true,
    });
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\nRUN echo hi\n", "utf8");

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
      writeFile: vi.fn(async () => {}),
      terminate: vi.fn(async () => {}),
    };

    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      snapshotAndWait: vi.fn(async () => ({
        snapshotId: "snap-1",
        sandboxId: "sbx-source-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
        snapshotFormatVersion: "durable_archive_v1",
        sizeBytes: 123,
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
      })),
      close: vi.fn(() => {}),
    };

    await createSandboxImage(
      dockerfilePath,
      { diskMb: 25 * 1024 },
      {
        emit: () => {},
        createClient: () => client as never,
        registerImage: async () => ({ id: "tpl-1" }),
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "python:3.12-slim",
      cpus: 2.0,
      memoryMb: 4096,
      diskMb: 25 * 1024,
    });
  });

  it("createSandboxImage lets the server choose the base image when the Image DSL omits one", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_123");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_123");

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-default`), {
      recursive: true,
    });

    const image = new Image({ name: "default-build" })
      .workdir("/workspace")
      .run("echo ready");

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
      writeFile: vi.fn(async () => {}),
      terminate: vi.fn(async () => {}),
    };

    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      snapshotAndWait: vi.fn(async () => ({
        snapshotId: "snap-1",
        sandboxId: "sbx-source-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
        snapshotFormatVersion: "durable_archive_v1",
        sizeBytes: 123,
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
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
      cpus: 2.0,
      memoryMb: 4096,
    });
    expect(client.snapshotAndWait).toHaveBeenCalledWith(
      "sbx-1",
      expect.objectContaining({ snapshotType: "filesystem" }),
    );
    expect(registerImage).toHaveBeenCalledWith(
      expect.anything(),
      "default-build",
      [
        "WORKDIR /workspace",
        "RUN echo ready",
      ].join("\n"),
      "snap-1",
      "sbx-source-1",
      "s3://snapshots/snap-1.tar.zst",
      123,
      10 * 1024 * 1024 * 1024,
      false,
      "durable_archive_v1",
    );
  });

  it("rejects COPY paths that escape the declared build context", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    vi.stubEnv("TENSORLAKE_ORGANIZATION_ID", "org_123");
    vi.stubEnv("TENSORLAKE_PROJECT_ID", "proj_123");

    const rootDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-escape`), {
      recursive: true,
    });
    const contextDir = path.join(rootDir, "context");
    await mkdir(contextDir, { recursive: true });
    await writeFile(path.join(rootDir, "secret.txt"), "top-secret", "utf8");

    const image = new Image({ name: "escape-build", baseImage: "tensorlake/ubuntu-systemd" })
      .copy("../secret.txt", "/workspace/secret.txt");

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
      writeFile: vi.fn(async () => {}),
      terminate: vi.fn(async () => {}),
    };

    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      snapshotAndWait: vi.fn(async () => ({
        snapshotId: "snap-1",
        sandboxId: "sbx-source-1",
        snapshotUri: "s3://snapshots/snap-1.tar.zst",
        sizeBytes: 123,
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
      })),
      close: vi.fn(() => {}),
    };

    const registerImage = vi.fn(async () => ({ id: "tpl-1" }));

    await expect(
      createSandboxImage(
        image,
        { contextDir },
        {
          emit: () => {},
          createClient: () => client as never,
          registerImage,
          sleep: async () => {},
        },
      ),
    ).rejects.toThrow(/escapes the build context/);

    expect(sandbox.writeFile).not.toHaveBeenCalled();
    expect(registerImage).not.toHaveBeenCalled();
    expect(sandbox.terminate).toHaveBeenCalled();
  });
});

describe("registerImage url selection", () => {
  const baseContext: BuildContext = {
    apiUrl: "https://api.tensorlake.test",
    apiKey: undefined,
    personalAccessToken: undefined,
    namespace: "default",
    organizationId: undefined,
    projectId: undefined,
    debug: false,
  };

  function stubFetch() {
    const fetchMock = vi.fn(async () =>
      Promise.resolve(
        new Response(JSON.stringify({ id: "tpl-1" }), {
          status: 200,
          headers: { "content-type": "application/json" },
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);
    return fetchMock;
  }

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("uses the scope-less URL when authenticating with an API key", async () => {
    const fetchMock = stubFetch();
    await registerImage(
      { ...baseContext, apiKey: "tl_apiKey_abc" },
      "img",
      "FROM python",
      "snap-1",
      "sbx-1",
      "s3://x",
      100,
      200,
      false,
    );

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("https://api.tensorlake.test/platform/v1/sandbox-templates");
    const headers = (init as RequestInit).headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer tl_apiKey_abc");
    expect(headers["X-Forwarded-Organization-Id"]).toBeUndefined();
    expect(headers["X-Forwarded-Project-Id"]).toBeUndefined();
  });

  it("ignores env org/project values when an API key is present", async () => {
    const fetchMock = stubFetch();
    await registerImage(
      {
        ...baseContext,
        apiKey: "tl_apiKey_abc",
        organizationId: "org_env",
        projectId: "proj_env",
      },
      "img",
      "FROM python",
      "snap-1",
      "sbx-1",
      "s3://x",
      100,
      200,
      false,
    );

    const [url] = fetchMock.mock.calls[0];
    expect(url).not.toContain("/organizations/");
    expect(url).not.toContain("/projects/");
  });

  it("keeps the scoped URL and X-Forwarded headers for PAT auth", async () => {
    const fetchMock = stubFetch();
    await registerImage(
      {
        ...baseContext,
        personalAccessToken: "tl_pat_xyz",
        organizationId: "org_1",
        projectId: "proj_1",
      },
      "img",
      "FROM python",
      "snap-1",
      "sbx-1",
      "s3://x",
      100,
      200,
      false,
    );

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(
      "https://api.tensorlake.test/platform/v1/organizations/org_1/projects/proj_1/sandbox-templates",
    );
    const headers = (init as RequestInit).headers as Record<string, string>;
    expect(headers["X-Forwarded-Organization-Id"]).toBe("org_1");
    expect(headers["X-Forwarded-Project-Id"]).toBe("proj_1");
    expect(headers.Authorization).toBe("Bearer tl_pat_xyz");
  });

  it("throws when authenticating with PAT but no org/project", async () => {
    stubFetch();
    await expect(
      registerImage(
        { ...baseContext, personalAccessToken: "tl_pat_xyz" },
        "img",
        "FROM python",
        "snap-1",
        "sbx-1",
        "s3://x",
        100,
        200,
        false,
      ),
    ).rejects.toThrow(/Personal Access Token/);
  });

  it("throws when no credentials are configured at all", async () => {
    stubFetch();
    await expect(
      registerImage(
        { ...baseContext },
        "img",
        "FROM python",
        "snap-1",
        "sbx-1",
        "s3://x",
        100,
        200,
        false,
      ),
    ).rejects.toThrow(/Missing TENSORLAKE_API_KEY/);
  });
});
