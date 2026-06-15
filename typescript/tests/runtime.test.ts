import path from "node:path";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const { binaryPath, binaryTargetId, nativeBindingPath, nativeTargetId } =
  require("../lib/runtime.cjs");

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
    expect(
      binaryPath("tl", { packageRoot: root, platform: "linux", arch: "x64", libc: "musl" }),
    ).toBe(path.join(root, "dist", "bin", "linux-x64-musl", "tl"));
  });

  it("selects libc-specific native binding targets on Linux", () => {
    expect(nativeTargetId("linux", "x64", { libc: "gnu" })).toBe("linux-x64");
    expect(nativeTargetId("linux", "x64", { libc: "musl" })).toBe("linux-x64-musl");
    expect(
      nativeTargetId("linux", "x64", {
        report: { header: { glibcVersionRuntime: "2.35" } },
      }),
    ).toBe("linux-x64");
    expect(nativeTargetId("linux", "x64", { report: { header: {} } })).toBe(
      "linux-x64-musl",
    );
    expect(nativeTargetId("darwin", "arm64", { libc: "musl" })).toBe("darwin-arm64");
  });

  it("builds libc-specific native binding paths", () => {
    const root = path.join(path.sep, "tmp", "tensorlake");

    expect(
      nativeBindingPath({ packageRoot: root, platform: "linux", arch: "x64", libc: "gnu" }),
    ).toBe(path.join(root, "dist", "native", "linux-x64", "tensorlake-node.node"));
    expect(
      nativeBindingPath({ packageRoot: root, platform: "linux", arch: "x64", libc: "musl" }),
    ).toBe(path.join(root, "dist", "native", "linux-x64-musl", "tensorlake-node.node"));
  });
});
