#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

const NATIVE_PACKAGES = {
  "darwin-arm64": "@tensorlake/native-darwin-arm64",
  "linux-arm64": "@tensorlake/native-linux-arm64-gnu",
  "linux-arm64-musl": "@tensorlake/native-linux-arm64-musl",
  "linux-x64": "@tensorlake/native-linux-x64-gnu",
  "linux-x64-musl": "@tensorlake/native-linux-x64-musl",
  "win32-x64": "@tensorlake/native-win32-x64",
};

function packageRoot() {
  return path.resolve(__dirname, "..");
}

function baseTargetId(platform = process.platform, arch = process.arch) {
  return `${platform}-${arch}`;
}

function linuxLibcFamily(options = {}) {
  if (options.libc === "gnu" || options.libc === "musl") {
    return options.libc;
  }

  try {
    const report =
      options.report ?? (process.report?.getReport ? process.report.getReport() : undefined);
    if (!report) {
      return "gnu";
    }
    if (report?.header?.glibcVersionRuntime) {
      return "gnu";
    }
  } catch {
    return "gnu";
  }

  return "musl";
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
      `Missing native binding package ${packageName} for ${targetId}. Reinstall tensorlake without omitting optional dependencies, and install dependencies on the machine where they will run.`,
      { cause },
    );
  }
  return require(packageEntry);
}

function exitWithSpawnResult(result) {
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  process.exit(result.status ?? 1);
}

function findPython() {
  const candidates =
    process.platform === "win32"
      ? [
          { command: "py", prefix: ["-3"] },
          { command: "python", prefix: [] },
        ]
      : [
          { command: "python3", prefix: [] },
          { command: "python", prefix: [] },
        ];

  for (const candidate of candidates) {
    const probe = spawnSync(candidate.command, [...candidate.prefix, "--version"], {
      stdio: "ignore",
      env: process.env,
    });
    if (!probe.error && probe.status === 0) {
      return candidate;
    }
  }

  return null;
}

function runPythonModule(moduleName, helpText) {
  const python = findPython();
  if (!python) {
    console.error(helpText);
    process.exit(1);
  }

  const result = spawnSync(
    python.command,
    [...python.prefix, "-m", moduleName, ...process.argv.slice(2)],
    {
      stdio: "inherit",
      env: process.env,
    },
  );
  exitWithSpawnResult(result);
}

module.exports = {
  loadNative,
  nativeTargetId,
  nativeBindingPath,
  nativePackageName,
  runPythonModule,
};
