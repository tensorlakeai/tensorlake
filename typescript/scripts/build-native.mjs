import { chmodSync, copyFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const packageRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(packageRoot, "..");
const nativePackageName = "tensorlake-rust-cloud-sdk-node";
const targetId =
  process.env.TENSORLAKE_NODE_TARGET_ID ?? `${process.platform}-${process.arch}`;
const targetTriple = process.env.TENSORLAKE_NODE_TARGET_TRIPLE || undefined;
const buildTool = process.env.TENSORLAKE_NODE_BUILD_TOOL ?? "cargo";

const outputDir = path.join(
  packageRoot,
  "dist",
  "native",
  targetId,
);

// The cdylib's filename varies by platform; the staged copy is always
// `tensorlake-node.node` so the Node loader can require() it uniformly.
const targetPlatform = targetId.split("-")[0];
const sourceFilename =
  targetPlatform === "win32"
    ? "tensorlake_node.dll"
    : targetPlatform === "darwin"
      ? "libtensorlake_node.dylib"
      : "libtensorlake_node.so";

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

  // cargo-zigbuild accepts glibc-versioned triples such as
  // `x86_64-unknown-linux-gnu.2.28`; today it stages artifacts under the
  // unversioned target directory, but also check the exact target string.
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
    `Unsupported TENSORLAKE_NODE_BUILD_TOOL '${buildTool}'. Expected 'cargo' or 'cargo-zigbuild'.`,
  );
  process.exit(1);
}

const cargoArgs = [
  cargoSubcommand,
  "--release",
  "-p",
  nativePackageName,
  ...(targetTriple ? ["--target", targetTriple] : []),
];

console.log(
  `Building native binding for ${targetId}${
    targetTriple ? ` (${targetTriple})` : ""
  } with cargo ${cargoSubcommand}`,
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

const source = cargoReleaseDirs()
  .map((releaseDir) => path.join(releaseDir, sourceFilename))
  .find((candidate) => existsSync(candidate));
const destination = path.join(outputDir, "tensorlake-node.node");
if (!source) {
  console.error(
    `Expected compiled native binding in one of: ${cargoReleaseDirs()
      .map((releaseDir) => path.join(releaseDir, sourceFilename))
      .join(", ")}`,
  );
  process.exit(1);
}
copyFileSync(source, destination);
if (targetPlatform !== "win32") {
  chmodSync(destination, 0o644);
}
