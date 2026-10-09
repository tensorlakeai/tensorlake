import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { loadRuntimeLock } from "./runtime-lock.mjs";

const source = fileURLToPath(new URL("../", import.meta.url));
for (const [name, mutate] of [
  ["altered tarball integrity", (lock) => { lock.packages["node_modules/ajv"].integrity = "sha512-tampered"; }],
  ["additional production package", (lock) => { lock.packages["node_modules/unexpected"] = { version: "1.0.0", integrity: "sha512-tampered", resolved: "https://registry.npmjs.org/unexpected/-/unexpected-1.0.0.tgz" }; }],
  ["shadowed transitive resolution", (lock) => { lock.packages["node_modules/ajv/node_modules/fast-uri"] = { ...lock.packages["node_modules/fast-uri"], version: "0.0.0" }; }],
]) {
  test(`rejects ${name}`, () => {
    const root = mkdtempSync(path.join(tmpdir(), "tensorlake-runtime-lock-"));
    try {
      mkdirSync(path.join(root, "scripts"));
      writeFileSync(path.join(root, "pnpm-lock.yaml"), readFileSync(path.join(source, "pnpm-lock.yaml")));
      const lock = JSON.parse(readFileSync(path.join(source, "scripts/runtime-package-lock.json"), "utf8"));
      mutate(lock);
      writeFileSync(path.join(root, "scripts/runtime-package-lock.json"), JSON.stringify(lock));
      assert.throws(() => loadRuntimeLock(root), /Runtime/);
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}
test("accepts the checked-in production graph", () => loadRuntimeLock(source));
