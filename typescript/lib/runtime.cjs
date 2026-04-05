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

function binaryPath(binaryName, options = {}) {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  const extension = platform === "win32" ? ".exe" : "";
  const root = options.packageRoot ?? packageRoot();
  return path.join(root, "dist", "bin", binaryTargetId(platform, arch), `${binaryName}${extension}`);
}

function exitWithSpawnResult(result) {
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  process.exit(result.status ?? 1);
}

function runBinary(binaryName) {
  const executable = binaryPath(binaryName);
  if (!fs.existsSync(executable)) {
    console.error(
      `Missing packaged binary '${binaryName}' for ${binaryTargetId()}. Run 'npm run build' in tensorlake before packaging or install a package published with support for your platform.`,
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
  runBinary,
  runPythonModule,
};
