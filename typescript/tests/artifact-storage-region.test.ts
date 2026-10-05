import { afterEach, describe, expect, it } from "vitest";
import { resolveArtifactStorageApiUrl, type ArtifactStorageRegion } from "../src/artifact-storage-region.js";
import { FilesystemClient } from "../src/filesystem.js";
import { RepositoryClient } from "../src/repositories.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

describe("Artifact Storage regions", () => {
  afterEach(clearNativeStub);

  it("preserves custom endpoints when no region is selected", () => {
    expect(resolveArtifactStorageApiUrl("http://localhost:8080")).toBe("http://localhost:8080");
  });

  it("rejects unknown regions and conflicting custom endpoints", () => {
    for (const region of ["", "eu-west-1", "EU-CENTRAL-1", "../us-east-1", "toString"]) {
      expect(() => resolveArtifactStorageApiUrl("https://api.tensorlake.ai", region as ArtifactStorageRegion)).toThrow("Unsupported");
    }
    expect(() => resolveArtifactStorageApiUrl("https://api.tensorlake.dev", "eu-central-1")).toThrow("custom or development");
  });

  it("routes both SDK clients to the selected region without changing defaults", () => {
    const stub = installNativeStub();
    for (const Client of [FilesystemClient, RepositoryClient]) {
      new Client({ apiKey: "test", apiUrl: "https://api.tensorlake.ai", region: "eu-central-1" });
      expect(stub.repositoryCtorArgs?.[0]).toBe("https://api.eu-central-1.tensorlake.ai");
      new Client({ apiKey: "test", apiUrl: "https://api.tensorlake.ai" });
      expect(stub.repositoryCtorArgs?.[0]).toBe("https://api.tensorlake.ai");
    }
  });
});
