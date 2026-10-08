#!/usr/bin/env node
// Supply-chain tripwire. tensorlake@0.5.144 was compromised by adding a
// `preinstall` hook to package.json that ran an obfuscated loader from lib/.
// Fail the build if any published manifest declares a lifecycle script that
// npm runs on install, or if lib/ contains anything beyond the known runtime shims.
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const forbiddenScripts = new Set([
  "preinstall", "install", "postinstall",
  "preuninstall", "uninstall", "postuninstall",
  "prepare", "prepublish",
]);
const manifests = [
  "package.json",
  ...readdirSync(join(root, "npm"), { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => join("npm", entry.name, "package.json")),
];
const allowedLibFiles = new Set(["libc.cjs", "libc.d.cts", "runtime.cjs"]);

const problems = [];
for (const manifest of manifests) {
  const pkg = JSON.parse(readFileSync(join(root, manifest), "utf8"));
  for (const name of Object.keys(pkg.scripts ?? {})) {
    if (forbiddenScripts.has(name)) {
      problems.push(`${manifest}: forbidden lifecycle script "${name}"`);
    }
  }
}
for (const entry of readdirSync(join(root, "lib"))) {
  if (!allowedLibFiles.has(entry)) {
    problems.push(`lib/${entry}: unexpected file in published lib/ directory`);
  }
}

if (problems.length > 0) {
  console.error("Install-script check failed:");
  for (const problem of problems) console.error(`  - ${problem}`);
  process.exit(1);
}
console.log(`Install-script check passed (${manifests.length} manifests, lib/ allowlisted).`);
