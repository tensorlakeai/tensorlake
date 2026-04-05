import { chmodSync, copyFileSync, existsSync, mkdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const packageRoot = path.resolve(__dirname, "..");
const repoRoot = path.resolve(packageRoot, "..");
const outputDir = path.join(packageRoot, "dist", "bin");
const extension = process.platform === "win32" ? ".exe" : "";

const cargo = spawnSync("cargo", ["build", "--release", "-p", "tensorlake-cli"], {
  cwd: repoRoot,
  stdio: "inherit",
  env: process.env,
});

if (cargo.status !== 0) {
  process.exit(cargo.status ?? 1);
}

mkdirSync(outputDir, { recursive: true });

for (const binaryName of ["tl", "tensorlake"]) {
  const source = path.join(repoRoot, "target", "release", `${binaryName}${extension}`);
  const destination = path.join(outputDir, `${binaryName}${extension}`);
  if (!existsSync(source)) {
    console.error(`Expected compiled binary at ${source}`);
    process.exit(1);
  }
  copyFileSync(source, destination);
  if (process.platform !== "win32") {
    chmodSync(destination, 0o755);
  }
}
