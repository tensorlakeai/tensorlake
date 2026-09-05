import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { access, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
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
  if (typeof module.remoteOptions !== "function") {
    throw new Error(`${label} does not export remoteOptions`);
  }
  if (typeof module.HttpBody !== "function") {
    throw new Error(`${label} does not export HttpBody`);
  }
  if (typeof module.Headers !== "function") {
    throw new Error(`${label} does not export Headers`);
  }
}

for (const [label, module] of [
  ["CommonJS applications SDK", commonJSApplications],
  ["ESM applications SDK", esmApplications],
]) {
  if (typeof module.registerApplication !== "function") {
    throw new Error(`${label} does not export registerApplication`);
  }
  if (typeof module.remoteOptions !== "function") {
    throw new Error(`${label} does not export remoteOptions`);
  }
  if (
    typeof module.HttpBody !== "function"
    || typeof module.schema?.httpBody !== "function"
  ) {
    throw new Error(`${label} does not export HttpBody and schema.httpBody`);
  }
  if (typeof module.createApplicationManifest !== "function") {
    throw new Error(`${label} does not export createApplicationManifest`);
  }
  if (typeof module.Headers !== "function") {
    throw new Error(`${label} does not export Headers`);
  }
  if (module.remoteOptions()[Symbol.for("tensorlake.applications.remote-options.v1")] !== true) {
    throw new Error(`${label} remoteOptions does not use the cross-bundle brand`);
  }
  const body = new module.HttpBody(
    new TextEncoder().encode('{"event":"packaged"}'),
    "application/json",
  );
  if (body.json().event !== "packaged") {
    throw new Error(`${label} HttpBody helpers are not usable`);
  }
  const headers = new module.Headers([
    ["X-Test", "first"],
    ["x-test", "second"],
  ]);
  if (headers.get("X-TEST") !== "second" || headers.getAll("x-test").length !== 2) {
    throw new Error(`${label} request Headers helpers are not usable`);
  }
  const application = module.registerApplication(
    label.startsWith("CommonJS") ? "commonjs_public_app" : "esm_public_app",
    async () => null,
    { allow: ["unauthenticated_requests"] },
  );
  const manifest = module.createApplicationManifest(application.definition);
  if (manifest.allow?.[0] !== "unauthenticated_requests") {
    throw new Error(`${label} does not emit application allow capabilities`);
  }
}

const crossBundleHeaders = new commonJSApplications.Headers([
  ["X-Test", "cross-bundle"],
]);
if (!(crossBundleHeaders instanceof esmApplications.Headers)) {
  throw new Error("Headers identity is not preserved across ESM and CommonJS bundles");
}

await access(new URL("../runtime/function-executor/manifest.json", import.meta.url));
await access(new URL("../runtime/typescript-function-runner/manifest.json", import.meta.url));
await access(new URL("../dist/native-worker.cjs", import.meta.url));
await access(new URL("../runtime/function-executor/package/dist/native-worker.cjs", import.meta.url));
await access(new URL("../runtime/typescript-function-runner/package/dist/native-worker.cjs", import.meta.url));
if (require.resolve("tensorlake/internal/native-worker") !== fileURLToPath(new URL("../dist/native-worker.cjs", import.meta.url))) {
  throw new Error("Native worker package export does not resolve to its packaged entrypoint");
}
const functionRunnerManifest = JSON.parse(await readFile(
  new URL("../runtime/typescript-function-runner/manifest.json", import.meta.url),
  "utf8",
));
if (Object.keys(functionRunnerManifest.files).some(
  (file) => file.endsWith("/tensorlake-node.node"),
)) {
  throw new Error("Function runner capsule embeds a native Rust agent core");
}
const functionRunnerPackage = JSON.parse(await readFile(
  new URL("../runtime/typescript-function-runner/package/package.json", import.meta.url),
  "utf8",
));
if (
  Object.keys(functionRunnerPackage.optionalDependencies ?? {}).length === 0
  || Object.keys(functionRunnerPackage.optionalDependencies).some(
    (dependency) => !dependency.startsWith("@tensorlake/native-"),
  )
) {
  throw new Error("Function runner capsule does not declare native platform packages");
}

process.stdout.write(
  "Verified ESM and CommonJS package entrypoints, legacy executor capsule, and platform-aware function runner capsule\n",
);

// Trusted native-build CI stages an addon for this host before validating the
// package. Pure-JS packaging jobs intentionally omit local native artifacts.
if (process.platform === "linux") {
  const { nativeBindingPath } = require("../lib/runtime.cjs");
  if (existsSync(nativeBindingPath())) {
    await import("./test-native-sandbox.mjs");
    await import("./test-native-package.mjs");
  }
}
