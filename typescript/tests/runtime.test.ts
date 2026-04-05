import path from "node:path";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const { binaryPath, binaryTargetId } = require("../lib/runtime.cjs");

describe("runtime binary selection", () => {
  it("maps supported platform and arch pairs into package target ids", () => {
    expect(binaryTargetId("linux", "x64")).toBe("linux-x64");
    expect(binaryTargetId("darwin", "arm64")).toBe("darwin-arm64");
    expect(binaryTargetId("win32", "x64")).toBe("win32-x64");
  });

  it("builds platform-specific binary paths", () => {
    const root = path.join(path.sep, "tmp", "tensorlake");

    expect(binaryPath("tl", { packageRoot: root, platform: "linux", arch: "x64" })).toBe(
      path.join(root, "dist", "bin", "linux-x64", "tl"),
    );
    expect(
      binaryPath("tensorlake", { packageRoot: root, platform: "darwin", arch: "arm64" }),
    ).toBe(path.join(root, "dist", "bin", "darwin-arm64", "tensorlake"));
    expect(
      binaryPath("tl", { packageRoot: root, platform: "win32", arch: "x64" }),
    ).toBe(path.join(root, "dist", "bin", "win32-x64", "tl.exe"));
  });
});
