import { afterEach, describe, expect, it, vi } from "vitest";
import { APIClient } from "../src/api-client.js";
import { createFileSystem } from "../src/file-system.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

describe("APIClient", () => {

  afterEach(() => {
    clearNativeStub();
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
  });

  it("creates a file system through Artifact Storage with only an API key", async () => {
    const stub = installNativeStub();
    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      apiKey: "api-key",
    });
    const fs = await client.createFileSystem("skills");
    expect(fs.id).toBe("skills");
    expect(stub.repository.createFilesystem).toHaveBeenCalledWith("skills");
    expect(stub.repositoryCtorArgs.slice(0, 4)).toEqual([
      "http://localhost:8900",
      "api-key",
      null,
      null,
    ]);
    client.close();
  });

  it("preserves an explicit region through compatibility creation helpers", async () => {
    const stub = installNativeStub();
    const client = new APIClient({ apiKey: "api-key" });
    await client.createFileSystem("regional", undefined, { region: "eu-central-1" });
    expect(stub.repository.createFilesystem).toHaveBeenLastCalledWith("regional", "eu-central-1");
    client.close();
    vi.stubEnv("TENSORLAKE_API_KEY", "api-key");
    await createFileSystem("helper", undefined, { region: "eu-central-1" });
    expect(stub.repository.createFilesystem).toHaveBeenLastCalledWith("helper", "eu-central-1");
  });

  it("deletes a file system through Artifact Storage", async () => {
    const stub = installNativeStub();
    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      apiKey: "api-key",
    });
    await client.deleteFileSystem("skills");
    expect(stub.repository.deleteFilesystem).toHaveBeenCalledWith("skills");
    client.close();
  });
});
