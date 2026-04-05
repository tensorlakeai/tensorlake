#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function packageRoot() {
  return path.resolve(__dirname, "..");
}

function binaryPath(binaryName) {
  const extension = process.platform === "win32" ? ".exe" : "";
  return path.join(packageRoot(), "dist", "bin", `${binaryName}${extension}`);
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
      `Missing packaged binary '${binaryName}'. Run 'npm run build' in tensorlake before packaging.`,
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
  runBinary,
  runPythonModule,
};
