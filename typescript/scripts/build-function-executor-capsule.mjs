import { createHash } from "node:crypto";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const sharedProtoRoot = path.resolve(root, "..", "proto");
const packageJson = JSON.parse(readFileSync(path.join(root, "package.json"), "utf8"));
const packageLock = JSON.parse(readFileSync(path.join(root, "package-lock.json"), "utf8"));
const capsuleRoot = path.join(root, "runtime", "function-executor");
const capsulePackage = path.join(capsuleRoot, "package");

rmSync(capsuleRoot, { recursive: true, force: true });
mkdirSync(path.join(capsulePackage, "bin"), { recursive: true });
mkdirSync(path.join(capsulePackage, "dist", "function-executor"), { recursive: true });

copyRequired("bin/function-executor.js");
copyRequired("dist/function-executor/main.js");
copyRequired("dist/function-executor/main.js.map");
copyRequired("dist/native-worker.cjs");
copyRequired("dist/native-worker.cjs.map");
cpSync(sharedProtoRoot, path.join(capsulePackage, "proto"), { recursive: true });
chmodSync(path.join(capsulePackage, "bin", "function-executor.js"), 0o755);

const dependencies = {};
for (const dependency of Object.keys(packageJson.dependencies).sort()) {
  const locked = packageLock.packages[`node_modules/${dependency}`];
  if (typeof locked?.version !== "string") {
    throw new Error(`package-lock.json does not contain a resolved version for ${dependency}`);
  }
  dependencies[dependency] = locked.version;
}

const capsulePackageJson = {
  name: "@tensorlake/function-executor-runtime",
  version: packageJson.version,
  description: "Tensorlake Node.js function executor runtime",
  type: "module",
  bin: { "function-executor": "./bin/function-executor.js" },
  engines: { node: ">=24.0.0" },
  dependencies,
  license: packageJson.license,
};
writeJson(path.join(capsulePackage, "package.json"), capsulePackageJson);

// Reuse the SDK's checked-in npm resolution instead of invoking npm and
// resolving dependency ranges again. In npm lockfile v3, packages used only
// by devDependencies have `dev: true`; shared and production packages do not.
const lockedProductionPackages = Object.fromEntries(
  Object.entries(packageLock.packages)
    .filter(([packagePath, metadata]) =>
      packagePath !== ""
      && !packagePath.startsWith("node_modules/tensorlake-native-")
      && metadata.dev !== true
    )
    .sort(([left], [right]) => left.localeCompare(right)),
);
writeJson(path.join(capsulePackage, "npm-shrinkwrap.json"), {
  name: capsulePackageJson.name,
  version: capsulePackageJson.version,
  lockfileVersion: 3,
  requires: true,
  packages: {
    "": capsulePackageJson,
    ...lockedProductionPackages,
  },
});

const files = {};
for (const relativePath of collectFiles(capsulePackage)) {
  const fullPath = path.join(capsulePackage, ...relativePath.split("/"));
  const contents = readFileSync(fullPath);
  files[`package/${relativePath}`] = {
    sha256: createHash("sha256").update(contents).digest("hex"),
    size: contents.length,
    mode: relativePath === "bin/function-executor.js" ? 0o755 : 0o644,
  };
}

const runtimeHash = createHash("sha256");
for (const [file, metadata] of Object.entries(files)) {
  runtimeHash.update(`${file}\0${metadata.sha256}\0${metadata.size}\0${metadata.mode}\n`);
}
const runtimeId = `sha256:${runtimeHash.digest("hex")}`;
writeJson(path.join(capsuleRoot, "manifest.json"), {
  format_version: 1,
  sdk_version: packageJson.version,
  minimum_node_major: 24,
  package_name: "@tensorlake/function-executor-runtime",
  runtime_id: runtimeId,
  files,
});

process.stdout.write(`Built function executor capsule ${runtimeId}\n`);

function copyRequired(relativePath) {
  const source = path.join(root, ...relativePath.split("/"));
  if (!existsSync(source)) throw new Error(`Missing build output: ${relativePath}`);
  const destination = path.join(capsulePackage, ...relativePath.split("/"));
  mkdirSync(path.dirname(destination), { recursive: true });
  cpSync(source, destination);
}

function collectFiles(directory, prefix = "") {
  const files = [];
  for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const relativePath = prefix === "" ? entry.name : `${prefix}/${entry.name}`;
    const fullPath = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...collectFiles(fullPath, relativePath));
    else if (entry.isFile()) files.push(relativePath);
    else throw new Error(`Function executor capsule cannot contain a symlink: ${relativePath}`);
  }
  return files.sort();
}

function writeJson(file, value) {
  writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}
