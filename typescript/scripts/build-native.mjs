import { chmodSync, copyFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const packageRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(packageRoot, "..");

const outputDir = path.join(
  packageRoot,
  "dist",
  "native",
  `${process.platform}-${process.arch}`,
);

// The cdylib's filename varies by platform; the staged copy is always
// `tensorlake-node.node` so the Node loader can require() it uniformly.
const sourceFilename =
  process.platform === "win32"
    ? "tensorlake_node.dll"
    : process.platform === "darwin"
      ? "libtensorlake_node.dylib"
      : "libtensorlake_node.so";

const cargo = spawnSync(
  "cargo",
  ["build", "--release", "-p", "tensorlake-rust-cloud-sdk-node"],
  {
    cwd: repoRoot,
    stdio: "inherit",
    env: process.env,
  },
);

if (cargo.status !== 0) {
  process.exit(cargo.status ?? 1);
}

mkdirSync(outputDir, { recursive: true });

const source = path.join(repoRoot, "target", "release", sourceFilename);
const destination = path.join(outputDir, "tensorlake-node.node");
if (!existsSync(source)) {
  console.error(`Expected compiled native binding at ${source}`);
  process.exit(1);
}
copyFileSync(source, destination);
if (process.platform !== "win32") {
  chmodSync(destination, 0o644);
}
