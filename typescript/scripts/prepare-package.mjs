import { chmodSync, copyFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const packageRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(packageRoot, "..");
const cliPackageName = "tensorlake-cli";
const targetId =
  process.env.TENSORLAKE_CLI_TARGET_ID ?? `${process.platform}-${process.arch}`;
const targetTriple = process.env.TENSORLAKE_CLI_TARGET_TRIPLE || undefined;
const buildTool = process.env.TENSORLAKE_CLI_BUILD_TOOL ?? "cargo";
const outputDir = path.join(packageRoot, "dist", "bin", targetId);
const targetPlatform = targetId.split("-")[0];
const extension = targetPlatform === "win32" ? ".exe" : "";

function cargoTargetRoot() {
  if (!process.env.CARGO_TARGET_DIR) {
    return path.join(repoRoot, "target");
  }
  return path.resolve(repoRoot, process.env.CARGO_TARGET_DIR);
}

function cargoReleaseDirs() {
  if (!targetTriple) {
    return [path.join(cargoTargetRoot(), "release")];
  }

  const targetDirNames = [
    targetTriple.replace(/\.\d+(?:\.\d+)?$/, ""),
    targetTriple,
  ];
  return [...new Set(targetDirNames)].map((targetDirName) =>
    path.join(cargoTargetRoot(), targetDirName, "release"),
  );
}

const cargoSubcommand =
  buildTool === "cargo-zigbuild"
    ? "zigbuild"
    : buildTool === "cargo"
      ? "build"
      : undefined;

if (!cargoSubcommand) {
  console.error(
    `Unsupported TENSORLAKE_CLI_BUILD_TOOL '${buildTool}'. Expected 'cargo' or 'cargo-zigbuild'.`,
  );
  process.exit(1);
}

// Cargo features to enable, comma/space separated (e.g. "mount"). CI sets this to "mount" for
// Unix targets so the shipped CLI includes the `tl fs mount` stack; empty otherwise.
const cliFeatures = (process.env.TENSORLAKE_CLI_FEATURES ?? "").trim();

const cargoArgs = [
  cargoSubcommand,
  "--release",
  "-p",
  cliPackageName,
  ...(cliFeatures ? ["--features", cliFeatures] : []),
  ...(targetTriple ? ["--target", targetTriple] : []),
];

console.log(
  `Building CLI binaries for ${targetId}${
    targetTriple ? ` (${targetTriple})` : ""
  } with cargo ${cargoSubcommand}${cliFeatures ? ` (features: ${cliFeatures})` : ""}`,
);

const cargo = spawnSync("cargo", cargoArgs, {
  cwd: repoRoot,
  stdio: "inherit",
  env: process.env,
});

if (cargo.status !== 0) {
  process.exit(cargo.status ?? 1);
}

mkdirSync(outputDir, { recursive: true });

for (const binaryName of ["tl", "tensorlake"]) {
  const source = cargoReleaseDirs()
    .map((releaseDir) => path.join(releaseDir, `${binaryName}${extension}`))
    .find((candidate) => existsSync(candidate));
  const destination = path.join(outputDir, `${binaryName}${extension}`);
  if (!source) {
    console.error(
      `Expected compiled binary in one of: ${cargoReleaseDirs()
        .map((releaseDir) => path.join(releaseDir, `${binaryName}${extension}`))
        .join(", ")}`,
    );
    process.exit(1);
  }
  copyFileSync(source, destination);
  if (targetPlatform !== "win32") {
    chmodSync(destination, 0o755);
  }
}
