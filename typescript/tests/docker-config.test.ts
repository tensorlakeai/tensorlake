import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Image } from "../src/image.js";
import {
  __setNativeBindingForTest,
  createSandboxImage,
  importSandboxImage,
  runCreateSandboxImageCli,
  runImportSandboxImageCli,
} from "../src/sandbox-image.js";

describe("image registry configuration", () => {
  const config = (token: string) => JSON.stringify({
    auths: { "registry.example.com": { identitytoken: token } },
  });
  let directory: string;
  let dockerfile: string;
  const calls: Array<Record<string, unknown>> = [];
  const invoke = vi.fn(async (options: Record<string, unknown>) => {
    calls.push(options);
    await Promise.resolve();
    return '{"id":"image-1"}';
  });
  const load = vi.fn(() => config("captured"));

  beforeEach(() => {
    directory = mkdtempSync(path.join(os.tmpdir(), "tensorlake-docker-config-"));
    dockerfile = path.join(directory, "Dockerfile");
    writeFileSync(dockerfile, "FROM registry.example.com/base:latest\n");
    calls.length = 0;
    vi.stubEnv("TENSORLAKE_API_KEY", "test-key");
    vi.stubEnv("DOCKER_CONFIG", "/unused/ambient/config");
    __setNativeBindingForTest({
      loadDockerConfigJson: load,
      buildSandboxImage: invoke,
      importSandboxImage: invoke,
    });
  });

  afterEach(() => {
    __setNativeBindingForTest(undefined);
    vi.unstubAllEnvs();
    vi.clearAllMocks();
    rmSync(directory, { recursive: true, force: true });
  });

  it("keeps independent contents across concurrent builds and imports", async () => {
    await Promise.all([
      importSandboxImage("registry.example.com/a", { dockerConfigJson: config("a") }),
      createSandboxImage(dockerfile, { dockerConfigJson: config("b") }),
      new Image({ name: "c", baseImage: "registry.example.com/c" })
        .build({ dockerConfigJson: config("c") }),
    ]);
    expect(calls.map((call) => call.dockerConfigJson).sort()).toEqual(
      [config("a"), config("b"), config("c")].sort(),
    );
    expect(load).not.toHaveBeenCalled();
    expect(process.env.DOCKER_CONFIG).toBe("/unused/ambient/config");
  });

  it("distinguishes omitted configuration from explicit empty contents", async () => {
    for (const dockerConfigJson of [undefined, "{}", ""]) {
      await importSandboxImage("registry.example.com/a", { dockerConfigJson });
    }
    // Validation belongs to Rust; even an invalid empty string must reach it
    // unchanged, rather than accidentally enabling ambient discovery.
    expect(calls.map((call) => call.dockerConfigJson)).toEqual([undefined, "{}", ""]);
  });

  it.each(["build", "import"])("the %s CLI captures config before invoking the SDK", async (kind) => {
    if (kind === "build") {
      await runCreateSandboxImageCli([dockerfile]);
    } else {
      await runImportSandboxImageCli(["registry.example.com/a"]);
    }
    expect(load).toHaveBeenCalledOnce();
    expect(load.mock.invocationCallOrder[0]).toBeLessThan(invoke.mock.invocationCallOrder[0]!);
    expect(calls[0]?.dockerConfigJson).toBe(config("captured"));
  });
});
