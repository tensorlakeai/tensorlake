#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function packageRoot() {
  return path.resolve(__dirname, "..");
}

function binaryTargetId(platform = process.platform, arch = process.arch) {
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
  const baseTargetId = binaryTargetId(platform, arch);
  if (platform !== "linux") {
    return baseTargetId;
  }
  if (!options.libc && !options.report && platform !== process.platform) {
    return baseTargetId;
  }
  return linuxLibcFamily(options) === "musl" ? `${baseTargetId}-musl` : baseTargetId;
}

function nativeTargetId(platform = process.platform, arch = process.arch, options = {}) {
  return packageTargetId(platform, arch, options);
}

function binaryPath(binaryName, options = {}) {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const extension = platform === "win32" ? ".exe" : "";
  const root = options.packageRoot ?? packageRoot();
  return path.join(
    root,
    "dist",
    "bin",
    packageTargetId(platform, arch, options),
    `${binaryName}${extension}`,
  );
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

function loadNative() {
  const targetId = nativeTargetId();
  const bindingPath = nativeBindingPath();
  if (!fs.existsSync(bindingPath)) {
    throw new Error(
      `Missing native binding for ${targetId}. Run 'npm run build' in tensorlake before packaging or install a package published with support for your platform.`,
    );
  }
  return require(bindingPath);
}

function exitWithSpawnResult(result) {
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  process.exit(result.status ?? 1);
}

function runBinary(binaryName) {
  const targetId = packageTargetId();
  const executable = binaryPath(binaryName);
  if (!fs.existsSync(executable)) {
    console.error(
      `Missing packaged binary '${binaryName}' for ${targetId}. Run 'npm run build' in tensorlake before packaging or install a package published with support for your platform.`,
    );
    process.exit(1);
  }

  const result = spawnSync(executable, process.argv.slice(2), {
    stdio: "inherit",
    env: process.env,
  });
  exitWithSpawnResult(result);
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
  binaryPath,
  binaryTargetId,
  loadNative,
  nativeTargetId,
  nativeBindingPath,
  runBinary,
  runPythonModule,
};
