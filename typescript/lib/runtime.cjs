#!/usr/bin/env node

const { detectLinuxLibc } = require("./libc.cjs");
const fs = require("node:fs");
const path = require("node:path");

const NATIVE_PACKAGES = {
  "darwin-arm64": "@tensorlakeai/native-darwin-arm64",
  "linux-arm64": "@tensorlakeai/native-linux-arm64-gnu",
  "linux-arm64-musl": "@tensorlakeai/native-linux-arm64-musl",
  "linux-x64": "@tensorlakeai/native-linux-x64-gnu",
  "linux-x64-musl": "@tensorlakeai/native-linux-x64-musl",
  "win32-x64": "@tensorlakeai/native-win32-x64",
};

function packageRoot() {
  return path.resolve(__dirname, "..");
}

function baseTargetId(platform = process.platform, arch = process.arch) {
  return `${platform}-${arch}`;
}

let cachedLinuxLibc;

function linuxLibcFamily(options = {}) {
  if (options.libc === "gnu" || options.libc === "musl") {
    return options.libc;
  }
  if (options.report != null) {
    return options.report.header?.glibcVersionRuntime ? "gnu" : "musl";
  }
  if (cachedLinuxLibc != null) return cachedLinuxLibc;

  cachedLinuxLibc = detectLinuxLibc(fs);
  return cachedLinuxLibc;
}

function packageTargetId(platform = process.platform, arch = process.arch, options = {}) {
  const targetId = baseTargetId(platform, arch);
  if (platform !== "linux") {
    return targetId;
  }
  if (!options.libc && !options.report && platform !== process.platform) {
    return targetId;
  }
  return linuxLibcFamily(options) === "musl" ? `${targetId}-musl` : targetId;
}

function nativeTargetId(platform = process.platform, arch = process.arch, options = {}) {
  return packageTargetId(platform, arch, options);
}

function nativeBindingPath(options = {}) {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const root = options.packageRoot ?? packageRoot();
  return path.join(
    root,
    "dist",
    "native",
    nativeTargetId(platform, arch, options),
    "tensorlake-node.node",
  );
}

function nativePackageName(platform = process.platform, arch = process.arch, options = {}) {
  return NATIVE_PACKAGES[nativeTargetId(platform, arch, options)];
}

function loadNative() {
  const targetId = nativeTargetId();
  const bindingPath = nativeBindingPath();
  if (fs.existsSync(bindingPath)) {
    return require(bindingPath);
  }

  const packageName = nativePackageName();
  if (!packageName) {
    throw new Error(
      `Tensorlake does not provide a native binding for ${targetId}.`,
    );
  }

  let packageEntry;
  try {
    packageEntry = require.resolve(packageName);
  } catch (cause) {
    throw new Error(
      `Missing native binding package ${packageName} for ${targetId}. Reinstall @tensorlakeai/tensorlake without omitting optional dependencies, and install dependencies on the machine where they will run.`,
      { cause },
    );
  }
  return require(packageEntry);
}

module.exports = {
  loadNative,
  nativeTargetId,
  nativeBindingPath,
  nativePackageName,
};
