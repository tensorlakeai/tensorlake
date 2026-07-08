import path from "node:path";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";

const require = createRequire(import.meta.url);
const { nativeBindingPath, nativeTargetId } = require("../lib/runtime.cjs");

describe("runtime native binding selection", () => {
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
