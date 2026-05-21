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
    vi.unstubAllGlobals();
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

  function makeSandbox(metadata: Record<string, unknown>) {
    return {
      sandboxId: "sbx-1",
      run: vi.fn(async () => ({ exitCode: 0, stdout: "", stderr: "" })),
      startProcess: vi.fn(async () => ({ pid: 1 })),
      getStdout: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getStderr: vi.fn(async () => ({ pid: 1, lines: [], lineCount: 0 })),
      getProcess: vi.fn(async () => ({
        pid: 1,
        status: "exited",
        stdinWritable: false,
        command: "tl-rootfs-build",
        args: [],
        startedAt: new Date(),
        exitCode: 0,
      })),
      writeFile: vi.fn(async () => {}),
      readFile: vi.fn(async () =>
        new TextEncoder().encode(JSON.stringify(metadata)),
      ),
      terminate: vi.fn(async () => {}),
    };
  }

  function stubRootfsBuildFetch(
    preparedOverrides: Record<string, unknown> = {},
  ) {
    const prepared = {
      buildId: "build-1",
      snapshotId: "snap-1",
      snapshotUri: "s3://snapshots/snap-1.tlsnap",
      rootfsNodeKind: "base",
      builder: {
        image: "tensorlake/rootfs-builder",
        command: "tl-rootfs-build",
        cpus: 2,
        memoryMb: 4096,
        diskMb: 8192,
      },
      ...preparedOverrides,
    };
    const completeBodies: Record<string, unknown>[] = [];
    const fetchMock = vi.fn(async (url: string | URL, init?: RequestInit) => {
      const urlString = String(url);
      if (urlString.endsWith("/platform/v1/keys/introspect")) {
        return new Response(
          JSON.stringify({
            organizationId: "org_introspected",
            projectId: "proj_introspected",
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      if (urlString.endsWith("/sandbox-template-builds")) {
        return new Response(JSON.stringify(prepared), {
          status: 200,
          headers: { "content-type": "application/json" },
        });
      }
      if (urlString.endsWith("/sandbox-template-builds/build-1/complete")) {
        completeBodies.push(
          JSON.parse(String(init?.body ?? "{}")) as Record<string, unknown>,
        );
        return new Response(
          JSON.stringify({ id: "tpl-1", snapshot_id: "snap-1" }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      return new Response(`unexpected URL ${urlString}`, { status: 500 });
    });
    vi.stubGlobal("fetch", fetchMock);
    return { fetchMock, completeBodies, prepared };
  }

  const rootfsMetadata = {
    snapshot_id: "snap-1",
    snapshot_uri: "s3://snapshots/snap-1.tlsnap",
    snapshot_format_version: "durable_archive_v1",
    snapshot_size_bytes: 123,
    rootfs_disk_bytes: 10 * 1024 * 1024 * 1024,
    rootfs_node_kind: "base",
  };

  it("createSandboxImage builds with the rootfs-builder path from Dockerfile", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    const { fetchMock, completeBodies } = stubRootfsBuildFetch();

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

    const sandbox = makeSandbox(rootfsMetadata);
    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      close: vi.fn(() => {}),
    };

    await createSandboxImage(
      dockerfilePath,
      {},
      {
        emit: () => {},
        createClient: () => client as never,
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "tensorlake/rootfs-builder",
      cpus: 2,
      memoryMb: 4096,
      diskMb: 10 * 1024,
    });
    expect(sandbox.run).toHaveBeenCalledWith(
      "mkdir",
      expect.objectContaining({
        args: ["-p", "/var/lib/tensorlake/rootfs-builder/build"],
        user: "root",
      }),
    );
    expect(sandbox.run).toHaveBeenCalledWith(
      "chmod",
      expect.objectContaining({
        args: ["0777", "/var/lib/tensorlake/rootfs-builder/build"],
        user: "root",
      }),
    );
    const contextMkdirCall = sandbox.run.mock.calls.find(
      ([command, options]) =>
        command === "mkdir" &&
        options?.args?.[1] ===
          "/var/lib/tensorlake/rootfs-builder/build/context",
    );
    expect(contextMkdirCall?.[1]).toMatchObject({
      args: ["-p", "/var/lib/tensorlake/rootfs-builder/build/context"],
    });
    expect(contextMkdirCall?.[1]?.user).toBeUndefined();
    expect(fetchMock).toHaveBeenCalledWith(
      "https://api.tensorlake.ai/platform/v1/organizations/org_introspected/projects/proj_introspected/sandbox-template-builds",
      expect.objectContaining({ method: "POST" }),
    );
    expect(sandbox.startProcess).toHaveBeenCalledWith(
      "/usr/local/bin/tl-rootfs-build",
      expect.objectContaining({
        args: [
          "--spec",
          "/var/lib/tensorlake/rootfs-builder/build/spec.json",
          "--metadata-out",
          "/var/lib/tensorlake/rootfs-builder/build/metadata.json",
        ],
        env: { PATH: "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" },
        workingDir: "/var/lib/tensorlake/rootfs-builder/build",
        user: "root",
      }),
    );
    const specCall = sandbox.writeFile.mock.calls.find(
      ([remotePath]) =>
        remotePath === "/var/lib/tensorlake/rootfs-builder/build/spec.json",
    );
    expect(specCall).toBeDefined();
    const spec = JSON.parse(new TextDecoder().decode(specCall?.[1] as Uint8Array));
    expect(spec).toMatchObject({
      dockerfile: `${dockerfileText}\n`,
      contextDir: "/var/lib/tensorlake/rootfs-builder/build/context",
      baseImage: "python:3.12-slim",
      rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
    });
    expect(completeBodies[0]).toEqual({
      snapshotId: "snap-1",
      snapshotUri: "s3://snapshots/snap-1.tlsnap",
      snapshotFormatVersion: "durable_archive_v1",
      snapshotSizeBytes: 123,
      rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
      rootfsNodeKind: "base",
    });
    expect(sandbox.terminate).toHaveBeenCalled();
  });

  it("createSandboxImage accepts an Image DSL definition", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    stubRootfsBuildFetch();

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

    const sandbox = makeSandbox(rootfsMetadata);
    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      close: vi.fn(() => {}),
    };

    await createSandboxImage(
      image,
      { contextDir: tempDir },
      {
        emit: () => {},
        createClient: () => client as never,
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "tensorlake/rootfs-builder",
      cpus: 2,
      memoryMb: 4096,
      diskMb: 10 * 1024,
    });
    const specCall = sandbox.writeFile.mock.calls.find(
      ([remotePath]) =>
        remotePath === "/var/lib/tensorlake/rootfs-builder/build/spec.json",
    );
    const spec = JSON.parse(new TextDecoder().decode(specCall?.[1] as Uint8Array));
    expect(spec).toMatchObject({
      dockerfile: [
        "FROM tensorlake/ubuntu-systemd",
        "WORKDIR /workspace",
        "COPY hello.txt /workspace/hello.txt",
        "RUN cat /workspace/hello.txt",
      ].join("\n"),
      baseImage: "tensorlake/ubuntu-systemd",
    });
    expect(sandbox.terminate).toHaveBeenCalled();
  });

  it("createSandboxImage uses builderDiskMb separately from generated rootfs disk size", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    const { completeBodies } = stubRootfsBuildFetch();

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-disk`), {
      recursive: true,
    });
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM python:3.12-slim\nRUN echo hi\n", "utf8");

    const sandbox = makeSandbox({
      ...rootfsMetadata,
      rootfs_disk_bytes: 25 * 1024 * 1024 * 1024,
    });
    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      close: vi.fn(() => {}),
    };

    await createSandboxImage(
      dockerfilePath,
      { diskMb: 25 * 1024, builderDiskMb: 32 * 1024 },
      {
        emit: () => {},
        createClient: () => client as never,
        sleep: async () => {},
      },
    );

    expect(client.createAndConnect).toHaveBeenCalledWith({
      image: "tensorlake/rootfs-builder",
      cpus: 2,
      memoryMb: 4096,
      diskMb: 32 * 1024,
    });
    expect(completeBodies[0].rootfsDiskBytes).toBe(25 * 1024 * 1024 * 1024);
  });

  it("createSandboxImage rejects Image DSL definitions without a base image", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    stubRootfsBuildFetch();

    const contextDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-default`), {
      recursive: true,
    });
    const image = new Image({ name: "default-build" })
      .workdir("/workspace")
      .run("echo ready");

    await expect(
      createSandboxImage(image, { contextDir }, { emit: () => {} }),
    ).rejects.toThrow(/FROM image or Image baseImage/);
  });

  it("createSandboxImage completes diff builds with prepared parent lineage", async () => {
    vi.stubEnv("TENSORLAKE_API_URL", "https://api.tensorlake.ai");
    vi.stubEnv("TENSORLAKE_API_KEY", "tl_key_test");
    vi.stubEnv("INDEXIFY_NAMESPACE", "default");
    const { completeBodies } = stubRootfsBuildFetch({
      rootfsNodeKind: "diff",
      parent: {
        parentManifestUri: "s3://snapshots/parent.tlsnap",
        rootfsDiskBytes: 10 * 1024 * 1024 * 1024,
      },
    });

    const tempDir = await mkdir(path.join(os.tmpdir(), `tensorlake-images-${Date.now()}-diff`), {
      recursive: true,
    });
    const dockerfilePath = path.join(tempDir, "Dockerfile");
    await writeFile(dockerfilePath, "FROM tensorlake/ubuntu-minimal\nRUN echo hi\n", "utf8");
    const sandbox = makeSandbox({
      ...rootfsMetadata,
      rootfs_node_kind: "diff",
    });
    const client = {
      createAndConnect: vi.fn(async () => sandbox),
      close: vi.fn(() => {}),
    };

    await createSandboxImage(dockerfilePath, {}, {
      emit: () => {},
      createClient: () => client as never,
      sleep: async () => {},
    });

    expect(completeBodies[0]).toMatchObject({
      rootfsNodeKind: "diff",
      parentManifestUri: "s3://snapshots/parent.tlsnap",
    });
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
