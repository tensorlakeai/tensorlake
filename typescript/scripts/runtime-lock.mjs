import { readFileSync } from "node:fs";
import path from "node:path";
import { parse } from "yaml";

// npm consumers need shrinkwraps. Check versions, tarball integrities, and
// dependency edges against pnpm before allowing either capsule to be built.
export function loadRuntimeLock(root) {
  const lock = parse(readFileSync(path.join(root, "pnpm-lock.yaml"), "utf8"));
  const runtime = JSON.parse(readFileSync(path.join(root, "scripts/runtime-package-lock.json"), "utf8"));
  const compatible = new Map();
  for (const [packagePath, metadata] of Object.entries(runtime.packages)) {
    if (!packagePath || metadata.dev === true || packagePath.startsWith("node_modules/@tensorlakeai/native-")) {
      continue;
    }
    const name = packagePath.split("node_modules/").at(-1);
    const key = `${name}@${metadata.version}`;
    if (!metadata.integrity || !metadata.resolved?.startsWith("https://registry.npmjs.org/")) {
      throw new Error(`Runtime entry is not registry integrity-locked: ${key}`);
    }
    const entries = compatible.get(key) ?? [];
    entries.push({ packagePath, metadata });
    compatible.set(key, entries);
  }

  const canonical = new Set();
  const visited = new Set();
  const pending = Object.entries(lock.importers["."].dependencies ?? {}).map(
    ([name, dependency]) => [name, dependency.version],
  );
  while (pending.length > 0) {
    const [name, version] = pending.pop();
    const snapshotKey = `${name}@${version}`;
    if (visited.has(snapshotKey)) continue;
    visited.add(snapshotKey);
    const packageKey = snapshotKey.replace(/\(.*$/, "");
    const metadata = lock.packages[packageKey];
    const snapshot = lock.snapshots[snapshotKey];
    if (!metadata?.resolution?.integrity || !snapshot) {
      throw new Error(`Runtime dependency is not integrity-locked: ${snapshotKey}`);
    }
    const entries = compatible.get(packageKey);
    if (!entries || entries.some((entry) => entry.metadata.integrity !== metadata.resolution.integrity)) {
      throw new Error(`Runtime npm/pnpm lock mismatch: ${packageKey}`);
    }
    const dependencies = {
      ...snapshot.dependencies,
      ...snapshot.optionalDependencies,
    };
    const hasMatchingEdges = entries.some(({ packagePath }) =>
      Object.entries(dependencies).every(([dependencyName, dependencyVersion]) => {
        const actual = resolveNpmDependency(runtime.packages, packagePath, dependencyName);
        return actual?.version === dependencyVersion.replace(/\(.*$/, "");
      }),
    );
    if (!hasMatchingEdges) throw new Error(`Runtime dependency edge mismatch: ${snapshotKey}`);
    canonical.add(packageKey);
    pending.push(...Object.entries(dependencies));
  }
  for (const key of compatible.keys()) {
    if (!canonical.has(key)) throw new Error(`Runtime npm lock has an extra production dependency: ${key}`);
  }
  for (const [name, dependency] of Object.entries(lock.importers["."].dependencies)) {
    const version = dependency.version.replace(/\(.*$/, "");
    if (runtime.packages[`node_modules/${name}`]?.version !== version) {
      throw new Error(`Runtime direct dependency mismatch: ${name}`);
    }
  }
  return runtime;
}

function resolveNpmDependency(packages, consumerPath, name) {
  let prefix = consumerPath;
  while (true) {
    const candidate = `${prefix ? `${prefix}/` : ""}node_modules/${name}`;
    if (packages[candidate]) return packages[candidate];
    if (!prefix) return undefined;
    const parent = prefix.lastIndexOf("/node_modules/");
    prefix = parent < 0 ? "" : prefix.slice(0, parent);
  }
}
