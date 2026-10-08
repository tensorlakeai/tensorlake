import { readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { nativePackageName } from "../src/native-binding.js";

const packageRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

interface NativeTarget {
  id: string;
  name: string;
  platform: NodeJS.Platform;
  arch: string;
  libc?: "glibc" | "musl";
  loaderLibc?: "gnu" | "musl";
}

const targets: readonly NativeTarget[] = [
  {
    id: "darwin-arm64",
    name: "@tensorlakeai/native-darwin-arm64",
    platform: "darwin",
    arch: "arm64",
  },
  {
    id: "linux-arm64",
    name: "@tensorlakeai/native-linux-arm64-gnu",
    platform: "linux",
    arch: "arm64",
    libc: "glibc",
    loaderLibc: "gnu",
  },
  {
    id: "linux-arm64-musl",
    name: "@tensorlakeai/native-linux-arm64-musl",
    platform: "linux",
    arch: "arm64",
    libc: "musl",
    loaderLibc: "musl",
  },
  {
    id: "linux-x64",
    name: "@tensorlakeai/native-linux-x64-gnu",
    platform: "linux",
    arch: "x64",
    libc: "glibc",
    loaderLibc: "gnu",
  },
  {
    id: "linux-x64-musl",
    name: "@tensorlakeai/native-linux-x64-musl",
    platform: "linux",
    arch: "x64",
    libc: "musl",
    loaderLibc: "musl",
  },
  {
    id: "win32-x64",
    name: "@tensorlakeai/native-win32-x64",
    platform: "win32",
    arch: "x64",
  },
];

function readJson(relativePath: string): Record<string, any> {
  return JSON.parse(readFileSync(path.join(packageRoot, relativePath), "utf8"));
}

describe("native package metadata", () => {
  it("keeps root optional dependencies and platform manifests in sync", () => {
    const rootPackage = readJson("package.json");
    const expectedDependencies = Object.fromEntries(
      targets.map(({ name }) => [name, rootPackage.version]),
    );

    expect(rootPackage.optionalDependencies).toEqual(expectedDependencies);

    for (const target of targets) {
      const manifest = readJson(`npm/${target.id}/package.json`);
      expect(manifest).toMatchObject({
        name: target.name,
        version: rootPackage.version,
        main: "tensorlake-node.node",
        files: ["tensorlake-node.node"],
        os: [target.platform],
        cpu: [target.arch],
        preferUnplugged: true,
      });
      expect(manifest.libc).toEqual(target.libc ? [target.libc] : undefined);
      expect(
        nativePackageName(
          target.platform,
          target.arch,
          target.loaderLibc ? { libc: target.loaderLibc } : {},
        ),
      ).toBe(target.name);
    }
  });

  it("pins the tensorlake wrapper to the same SDK release", () => {
    const rootPackage = readJson("package.json");
    const wrapper = readJson("tensorlake-wrapper/package.json");

    expect(rootPackage.name).toBe("@tensorlakeai/tensorlake");
    expect(wrapper).toMatchObject({
      name: "tensorlake",
      version: rootPackage.version,
      dependencies: { [rootPackage.name]: rootPackage.version },
    });
    expect(Object.keys(wrapper.exports)).toEqual([".", "./applications"]);
    expect(wrapper.bin).toBeUndefined();
  });

  it("keeps the npm lockfile ready for clean installs before first publish", () => {
    const rootPackage = readJson("package.json");
    const packageLock = readJson("package-lock.json");

    expect(packageLock.packages[""].optionalDependencies).toEqual(
      rootPackage.optionalDependencies,
    );
    for (const { name } of targets) {
      expect(packageLock.packages[`node_modules/${name}`]).toMatchObject({
        version: rootPackage.version,
        resolved: `https://registry.npmjs.org/${name}/-/${name.split("/").pop()}-${rootPackage.version}.tgz`,
        optional: true,
      });
    }
  });
});
