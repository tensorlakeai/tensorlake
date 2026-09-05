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
      "@tensorlake/native-linux-x64-gnu",
    );
    expect(nativePackageName("linux", "arm64", { libc: "musl" })).toBe(
      "@tensorlake/native-linux-arm64-musl",
    );
    expect(nativePackageName("darwin", "arm64")).toBe(
      "@tensorlake/native-darwin-arm64",
    );
    expect(nativePackageName("win32", "x64")).toBe(
      "@tensorlake/native-win32-x64",
    );
    expect(nativePackageName("darwin", "x64")).toBeUndefined();
  });
});

const require = createRequire(import.meta.url);
const runtimePath = require.resolve("../lib/runtime.cjs");
const processReport = process.report as NodeJS.ProcessReport & { excludeNetwork: boolean };

describe.each(["SDK", "CommonJS launcher"])("%s libc detection", (loader) => {
  let runtime: typeof import("../src/native-binding.js");
  let originalExcludeNetwork: boolean;

  beforeEach(async () => {
    originalExcludeNetwork = processReport.excludeNetwork;
    vi.resetModules();
    delete require.cache[runtimePath];
    runtime = loader === "SDK"
      ? await import("../src/native-binding.js")
      : require(runtimePath);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    processReport.excludeNetwork = originalExcludeNetwork;
    delete require.cache[runtimePath];
  });

  it.runIf(process.platform === "linux").each([
    { header: { glibcVersionRuntime: "2.35" }, suffix: "" },
    { header: {}, suffix: "-musl" },
  ])("probes only once across target, path and package lookup ($suffix)", ({ header, suffix }) => {
    processReport.excludeNetwork = false;
    const reportOptions: boolean[] = [];
    const getReport = vi.spyOn(process.report, "getReport").mockImplementation(() => {
      reportOptions.push(processReport.excludeNetwork);
      return { header } as ReturnType<typeof process.report.getReport>;
    });

    for (let i = 0; i < 3; i++) {
      expect(runtime.nativeTargetId("linux", "x64")).toBe(`linux-x64${suffix}`);
      expect(runtime.nativeBindingPath({ platform: "linux", arch: "x64" })).toContain(
        path.join("native", `linux-x64${suffix}`, "tensorlake-node.node"),
      );
      expect(runtime.nativePackageName("linux", "x64")).toBe(
        suffix ? "@tensorlake/native-linux-x64-musl" : "@tensorlake/native-linux-x64-gnu",
      );
    }
    expect(getReport).toHaveBeenCalledTimes(1);
    expect(reportOptions).toEqual([true]);
  });

  it.runIf(process.platform === "linux").each([false, true])(
    "restores excludeNetwork=%s after detection succeeds or throws",
    async (excludeNetwork) => {
      processReport.excludeNetwork = excludeNetwork;
      const reportOptions: boolean[] = [];
      const getReport = vi.spyOn(process.report, "getReport").mockImplementation(() => {
        reportOptions.push(processReport.excludeNetwork);
        return { header: { glibcVersionRuntime: "2.35" } } as ReturnType<typeof process.report.getReport>;
      });
      expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
      expect(processReport.excludeNetwork).toBe(excludeNetwork);

      vi.resetModules();
      delete require.cache[runtimePath];
      runtime = loader === "SDK"
        ? await import("../src/native-binding.js")
        : require(runtimePath);
      getReport.mockImplementation(() => {
        reportOptions.push(processReport.excludeNetwork);
        throw new Error("report unavailable");
      });
      expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
      expect(processReport.excludeNetwork).toBe(excludeNetwork);
      expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
      expect(getReport).toHaveBeenCalledTimes(2);
      expect(reportOptions).toEqual([true, true]);
    },
  );

  it("does not probe for explicit inputs or non-Linux targets", () => {
    const getReport = vi.spyOn(process.report, "getReport");
    expect(runtime.nativeTargetId("linux", "x64", { libc: "gnu" })).toBe("linux-x64");
    expect(runtime.nativeTargetId("linux", "x64", { report: { header: {} } })).toBe("linux-x64-musl");
    expect(runtime.nativeTargetId("darwin", "arm64")).toBe("darwin-arm64");
    expect(getReport).not.toHaveBeenCalled();
  });

  it.runIf(process.platform === "linux")("keeps explicit inputs independent of cached detection", () => {
    const getReport = vi.spyOn(process.report, "getReport").mockReturnValue(
      { header: { glibcVersionRuntime: "2.35" } } as ReturnType<typeof process.report.getReport>,
    );
    expect(runtime.nativeTargetId("linux", "x64", { report: { header: {} } })).toBe("linux-x64-musl");
    expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
    expect(runtime.nativeTargetId("linux", "x64", { libc: "musl" })).toBe("linux-x64-musl");
    expect(runtime.nativeTargetId("linux", "x64", { report: { header: {} } })).toBe("linux-x64-musl");
    expect(runtime.nativeTargetId("linux", "x64")).toBe("linux-x64");
    expect(getReport).toHaveBeenCalledTimes(1);
  });
});
