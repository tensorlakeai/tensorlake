import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { existsSync } from "node:fs";
import { cp, mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

// Run after build:sdk and build:runtime. Test tarballs rather than workspace
// symlinks so standalone output cannot inherit the checkout's native addon.
const root = fileURLToPath(new URL("../", import.meta.url));
const require = createRequire(import.meta.url);
const runtime = require("../lib/runtime.cjs");
const npm = process.platform === "win32" ? "npm.cmd" : "npm";
const directory = await mkdtemp(path.join(tmpdir(), "tensorlake-nextjs-"));
const requests = [];
const mock = createServer((req, res) => {
  requests.push(req.url);
  req.resume();
  res.setHeader("Content-Type", "application/json");
  res.end(JSON.stringify(req.method === "POST"
    ? { request_id: "next-repro-request" }
    : { sandboxes: [], processes: [] }));
});

function run(command, args, cwd) {
  const result = spawnSync(command, args, {
    cwd, encoding: "utf8", timeout: 120_000, maxBuffer: 16 * 1024 * 1024,
    env: { ...process.env, NEXT_TELEMETRY_DISABLED: "1" },
  });
  assert.equal(result.status, 0, `${command} ${args.join(" ")}\n${result.stdout}\n${result.stderr}`);
  return result.stdout;
}

async function checkStandalone() {
  // Move outside the app directory so missing traced dependencies cannot be
  // resolved from the original node_modules by accident.
  const isolated = await mkdtemp(path.join(tmpdir(), "tensorlake-nextjs-deploy-"));
  const reservation = createServer();
  reservation.listen(0, "127.0.0.1");
  await once(reservation, "listening");
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  let child;
  let log = "";
  try {
    await cp(path.join(directory, ".next/standalone"), isolated, { recursive: true });
    child = spawn(process.execPath, ["server.js"], {
      cwd: isolated, stdio: ["ignore", "pipe", "pipe"],
      env: { ...process.env, PORT: String(port), HOSTNAME: "127.0.0.1", TENSORLAKE_REPRO_API_URL: `http://127.0.0.1:${mock.address().port}` },
    });
    child.stdout.on("data", data => { log += data; });
    child.stderr.on("data", data => { log += data; });
    let response;
    for (let attempt = 0; attempt < 100; attempt++) {
      try { response = await fetch(`http://127.0.0.1:${port}/api/application`); break; }
      catch { await delay(100); }
    }
    assert.ok(response, log);
    const body = await response.text();
    assert.equal(response.status, 200, `${body}\n${log}`);
    assert.deepEqual(JSON.parse(body), { requestId: "next-repro-request" });
    const sandboxResponse = await fetch(`http://127.0.0.1:${port}/api/sandbox`);
    const sandboxBody = await sandboxResponse.text();
    assert.equal(sandboxResponse.status, 200, `${sandboxBody}\n${log}`);
    assert.deepEqual(JSON.parse(sandboxBody), { sandboxes: [], processes: [] });
    assert.equal(requests.length, 3, "Expected application, lifecycle and proxy calls through the native worker");
  } finally {
    if (child?.exitCode === null) { child.kill(); await once(child, "exit"); }
    await rm(isolated, { recursive: true, force: true });
  }
}

try {
  await cp(path.join(root, "tests/fixtures/nextjs"), directory, { recursive: true });
  const packed = [root, path.join(root, "tensorlake-wrapper")].map(cwd => {
    const [{ filename }] = JSON.parse(run(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", directory], cwd));
    return path.join(directory, filename);
  });
  run(npm, ["install", "--ignore-scripts", "--no-audit", "--no-fund", ...packed], directory);
  // Prefer CI's freshly built addon. A JavaScript-only local checkout can reuse
  // its installed optional native package without compiling Rust.
  const addon = existsSync(runtime.nativeBindingPath())
    ? runtime.nativeBindingPath() : require.resolve(runtime.nativePackageName());
  const nativePackage = path.join(directory, "node_modules", runtime.nativePackageName());
  await mkdir(nativePackage, { recursive: true });
  await cp(addon, path.join(nativePackage, "tensorlake-node.node"));
  await cp(path.join(root, "npm", runtime.nativeTargetId(), "package.json"), path.join(nativePackage, "package.json"));

  mock.listen(0, "127.0.0.1");
  await once(mock, "listening");
  const routes = await Promise.all(["sandbox", "application"].map(async name => {
    const routePath = path.join(directory, `app/api/${name}/route.ts`);
    return [routePath, await readFile(routePath, "utf8")];
  }));
  for (const name of ["@tensorlakeai/tensorlake", "tensorlake"]) {
    for (const [routePath, route] of routes) {
      await writeFile(routePath, route.replaceAll("@tensorlakeai/tensorlake", name));
    }
    for (const external of [false, true]) {
      requests.length = 0;
      await writeFile(path.join(directory, "next.config.mjs"), `export default ${JSON.stringify({ output: "standalone", ...(external ? { serverExternalPackages: ["tensorlake", "@tensorlakeai/tensorlake"] } : {}) })};\n`);
      await rm(path.join(directory, ".next"), { recursive: true, force: true });
      run(process.execPath, ["node_modules/next/dist/bin/next", "build", "--turbopack"], directory);
      await checkStandalone();
      console.log(`Next.js 16.2.4 ${name}, external=${external}: build and isolated native runtime passed`);
    }
  }
} finally {
  mock.closeAllConnections();
  if (mock.listening) await new Promise(resolve => mock.close(resolve));
  await rm(directory, { recursive: true, force: true });
}
