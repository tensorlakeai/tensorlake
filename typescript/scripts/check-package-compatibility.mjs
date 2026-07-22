import { createRequire } from "node:module";
import { access } from "node:fs/promises";
const require = createRequire(import.meta.url);

const commonJS = require("tensorlake");
const commonJSApplications = require("tensorlake/applications");
const esm = await import("tensorlake");
const esmApplications = await import("tensorlake/applications");

for (const [label, module] of [
  ["CommonJS SDK", commonJS],
  ["ESM SDK", esm],
]) {
  if (typeof module.Sandbox !== "function") {
    throw new Error(`${label} does not export Sandbox`);
  }
}

for (const [label, module] of [
  ["CommonJS applications SDK", commonJSApplications],
  ["ESM applications SDK", esmApplications],
]) {
  if (typeof module.registerApplication !== "function") {
    throw new Error(`${label} does not export registerApplication`);
  }
}

await access(new URL("../runtime/function-executor/manifest.json", import.meta.url));

process.stdout.write("Verified ESM and CommonJS package entrypoints and executor capsule\n");
