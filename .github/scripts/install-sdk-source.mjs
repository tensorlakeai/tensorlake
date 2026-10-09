import { readFile, writeFile, mkdtemp, rm } from "node:fs/promises";
import { resolve, join } from "node:path";
import { checkReleaseAge, runNpm } from "./check-npm-release-age.mjs";

const project = resolve(process.argv[2] ?? "typescript");
const manifestPath = join(project, "package.json");
const lockPath = join(project, "package-lock.json");
const manifestBytes = await readFile(manifestPath, "utf8");
const lockBytes = await readFile(lockPath, "utf8");
const manifest = JSON.parse(manifestBytes);
const lock = JSON.parse(lockBytes);
if (manifest.name !== "@tensorlakeai/tensorlake") {
  throw new Error("Source-build setup applies only to the checked-out Tensorlake SDK");
}
// Source workflows build these native artifacts themselves. Do not install
// unpublished or freshly published copies from the registry during that build.
const nativePackages = [
  "@tensorlakeai/native-darwin-arm64", "@tensorlakeai/native-linux-arm64-gnu",
  "@tensorlakeai/native-linux-arm64-musl", "@tensorlakeai/native-linux-x64-gnu",
  "@tensorlakeai/native-linux-x64-musl", "@tensorlakeai/native-win32-x64",
];
for (const name of nativePackages) {
  if (manifest.optionalDependencies?.[name] !== manifest.version) {
    throw new Error(`Unexpected native release version: ${name}`);
  }
  delete manifest.optionalDependencies[name];
  delete lock.packages[""].optionalDependencies[name];
  delete lock.packages[`node_modules/${name}`];
}
const scratch = await mkdtemp(join(project, ".npm-source-check-"));
try {
  const validationLock = join(scratch, "package-lock.json");
  await writeFile(validationLock, JSON.stringify(lock));
  await checkReleaseAge(validationLock);
  if (!process.argv.includes("--check-only")) {
    // npm ci validates the filtered manifest and lock together. Restore release
    // metadata even on failure; packaging still uses the original exact pins.
    try {
      await writeFile(manifestPath, JSON.stringify(manifest));
      await writeFile(lockPath, JSON.stringify(lock));
      runNpm(["ci", "--ignore-scripts"], { cwd: project, stdio: "inherit" });
    } finally {
      await writeFile(manifestPath, manifestBytes);
      await writeFile(lockPath, lockBytes);
    }
  }
} finally {
  await rm(scratch, { recursive: true });
}
