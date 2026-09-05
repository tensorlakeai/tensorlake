import { createRequire } from "node:module";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  nativeBindingPath,
  nativePackageName,
  nativeTargetId,
} from "../src/native-binding.js";

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

  it("selects the platform-specific optional package", () => {
    expect(nativePackageName("linux", "x64", { libc: "gnu" })).toBe(
      "tensorlake-native-linux-x64-gnu",
    );
    expect(nativePackageName("linux", "arm64", { libc: "musl" })).toBe(
      "tensorlake-native-linux-arm64-musl",
    );
    expect(nativePackageName("darwin", "arm64")).toBe(
      "tensorlake-native-darwin-arm64",
    );
    expect(nativePackageName("win32", "x64")).toBe(
      "tensorlake-native-win32-x64",
    );
    expect(nativePackageName("darwin", "x64")).toBeUndefined();
  });
});

const require = createRequire(import.meta.url);
const runtimePath = require.resolve("../lib/runtime.cjs");

describe.each(["SDK", "CommonJS launcher"])("%s libc detection", (loader) => {
  let runtime: typeof import("../src/native-binding.js");

  beforeEach(async () => {
    vi.resetModules();
    delete require.cache[runtimePath];
    runtime = loader === "SDK"
      ? await import("../src/native-binding.js")
      : require(runtimePath);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    delete require.cache[runtimePath];
  });

  it.runIf(process.platform === "linux").each(["gnu", "musl"] as const)(
    "detects and caches %s without diagnostic reports",
    (libc) => {
      vi.stubEnv("TENSORLAKE_NODE_LIBC", libc);
      const getReport = vi.spyOn(process.report, "getReport").mockImplementation(() => {
        throw new Error("SDK must not generate diagnostic reports");
      });
      const target = libc === "musl" ? "linux-x64-musl" : "linux-x64";
      expect(runtime.nativeTargetId("linux", "x64")).toBe(target);
      vi.stubEnv("TENSORLAKE_NODE_LIBC", "invalid-after-detection");
      expect(runtime.nativeBindingPath({ platform: "linux", arch: "x64" })).toContain(
        path.join("native", target, "tensorlake-node.node"),
      );
      expect(runtime.nativePackageName("linux", "x64")).toBe(
        libc === "musl" ? "tensorlake-native-linux-x64-musl" : "tensorlake-native-linux-x64-gnu",
      );
      expect(runtime.nativeTargetId("linux", "x64", { libc: "musl" })).toBe("linux-x64-musl");
      expect(runtime.nativeTargetId("linux", "x64", { report: { header: {} } })).toBe("linux-x64-musl");
      expect(runtime.nativeTargetId("linux", "x64")).toBe(target);
      expect(getReport).not.toHaveBeenCalled();
    },
  );

  it("does not detect the host libc for explicit inputs or non-Linux targets", () => {
    vi.stubEnv("TENSORLAKE_NODE_LIBC", "invalid");
    expect(runtime.nativeTargetId("linux", "x64", { libc: "gnu" })).toBe("linux-x64");
    expect(runtime.nativeTargetId("linux", "x64", { report: { header: {} } })).toBe("linux-x64-musl");
    expect(runtime.nativeTargetId("darwin", "arm64")).toBe("darwin-arm64");
  });

  it.runIf(process.platform === "linux")("does not cache failed detection", () => {
    vi.stubEnv("TENSORLAKE_NODE_LIBC", "invalid");
    expect(() => runtime.nativeTargetId()).toThrow("TENSORLAKE_NODE_LIBC");
    vi.stubEnv("TENSORLAKE_NODE_LIBC", "gnu");
    expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
  });
});
