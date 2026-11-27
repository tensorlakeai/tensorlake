import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { access } from "node:fs/promises";
import { fileURLToPath } from "node:url";
const require = createRequire(import.meta.url);

const commonJS = require("@tensorlakeai/tensorlake");
const esm = await import("@tensorlakeai/tensorlake");

for (const [label, module] of [
  ["CommonJS SDK", commonJS],
  ["ESM SDK", esm],
]) {
  if (typeof module.Sandbox !== "function") {
    throw new Error(`${label} does not export Sandbox`);
  }
}

await access(new URL("../dist/native-worker.cjs", import.meta.url));
if (require.resolve("@tensorlakeai/tensorlake/internal/native-worker") !== fileURLToPath(new URL("../dist/native-worker.cjs", import.meta.url))) {
  throw new Error("Native worker package export does not resolve to its packaged entrypoint");
}

process.stdout.write("Verified ESM and CommonJS package entrypoints\n");

// Trusted native-build CI stages an addon for this host before validating the
// package. Pure-JS packaging jobs intentionally omit local native artifacts.
if (process.platform === "linux") {
  const { nativeBindingPath } = require("../lib/runtime.cjs");
  if (existsSync(nativeBindingPath())) {
    await import("./test-native-sandbox.mjs");
    await import("./test-native-package.mjs");
  }
}
