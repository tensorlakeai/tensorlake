import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Staged native packages are not public until a maintainer approves them, so
// `npm install --package-lock-only` cannot read their checksums from the
// registry. Release CI stages the exact tarballs it packed and records their
// integrity; this writes those checksums into package-lock.json so the
// function runner capsule's shrinkwrap pins the artifacts being released.
const [recordsDirectory] = process.argv.slice(2);
if (!recordsDirectory) {
  throw new Error("usage: lock-native-integrity.mjs <records-directory>");
}

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const lockPath = path.join(root, "package-lock.json");
const lock = JSON.parse(readFileSync(lockPath, "utf8"));
const nativeDependencies = lock.packages[""].optionalDependencies ?? {};

const records = readdirSync(recordsDirectory, { recursive: true })
  .filter((file) => file.endsWith(".json"))
  .map((file) => JSON.parse(readFileSync(path.join(recordsDirectory, file), "utf8")));

const locked = new Set();
for (const { name, version, integrity } of records) {
  if (nativeDependencies[name] !== version) {
    throw new Error(`${name}@${version} is not a native dependency of this release`);
  }
  if (typeof integrity !== "string" || !integrity.startsWith("sha512-")) {
    throw new Error(`${name}@${version} has no sha512 integrity`);
  }
  const key = `node_modules/${name}`;
  const entry = lock.packages[key];
  if (entry?.version !== version) {
    throw new Error(`package-lock.json does not lock ${name}@${version}`);
  }
  // Keep npm's field order: version, resolved, integrity, then the rest.
  const { version: _version, resolved, integrity: _previous, ...rest } = entry;
  lock.packages[key] = { version, resolved, integrity, ...rest };
  locked.add(name);
}

const missing = Object.keys(nativeDependencies).filter((name) => !locked.has(name));
if (missing.length > 0) {
  throw new Error(`Missing integrity records for ${missing.join(", ")}`);
}

writeFileSync(lockPath, `${JSON.stringify(lock, null, 2)}\n`);
process.stdout.write(`Locked integrity for ${locked.size} native packages\n`);
