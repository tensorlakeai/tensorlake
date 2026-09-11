import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { copyFile, mkdir, mkdtemp, readFile, rename, rm, symlink, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

// Exercise the actual npm tarball, including optional-package resolution,
// without inheriting the checkout's dist/native layout or module resolution.
const require = createRequire(import.meta.url);
const runtime = require("../lib/runtime.cjs");
const root = fileURLToPath(new URL("../", import.meta.url));
const directory = await mkdtemp(path.join(tmpdir(), "tensorlake-packed-worker-"));
try {
  const npm = process.platform === "win32" ? "npm.cmd" : "npm";
  const packed = spawnSync(npm, ["pack", "--ignore-scripts", "--json", "--pack-destination", directory], { cwd: root, encoding: "utf8" });
  assert.equal(packed.status, 0, packed.stderr);
  const [{ filename }] = JSON.parse(packed.stdout);
  const unpacked = spawnSync("tar", ["-xzf", path.join(directory, filename), "-C", directory], { encoding: "utf8" });
  assert.equal(unpacked.status, 0, unpacked.stderr);
  const modules = path.join(directory, "node_modules");
  await mkdir(modules);
  const sdk = path.join(modules, "tensorlake");
  await rename(path.join(directory, "package"), sdk);
  // Reuse the already-installed, locked JS dependencies. Only Tensorlake and
  // its native package must come from the isolated packaging layout.
  const manifest = JSON.parse(await readFile(path.join(sdk, "package.json"), "utf8"));
  for (const dependency of Object.keys(manifest.dependencies)) {
    const link = path.join(modules, dependency);
    await mkdir(path.dirname(link), { recursive: true });
    await symlink(path.join(root, "node_modules", dependency), link, "dir");
  }
  const addon = path.join(sdk, "node_modules", runtime.nativePackageName());
  await mkdir(addon, { recursive: true });
  await writeFile(path.join(addon, "package.json"), JSON.stringify({ main: "tensorlake-node.node" }));
  await copyFile(runtime.nativeBindingPath(), path.join(addon, "tensorlake-node.node"));
  const driver = path.join(directory, "verify.mjs");
  await writeFile(driver, `
    import assert from 'node:assert/strict';
    import { createServer } from 'node:http';
    import { once } from 'node:events';
    import { createRequire } from 'node:module';
    const dispatcherKey = Symbol.for('undici.globalDispatcher.1');
    const applicationDispatcher = {};
    globalThis[dispatcherKey] = applicationDispatcher;
    process.dlopen = () => { throw new Error('Main-thread addon load'); };
    const { SandboxClient, CloudClient } = await import('tensorlake');
    const { Sandbox, CloudClient: CommonJSCloudClient } = createRequire(import.meta.url)('tensorlake');
    assert.equal(globalThis[dispatcherKey], applicationDispatcher, 'SDK replaced application dispatcher');
    const server = createServer((_req, res) => {
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ sandboxes: [], processes: [], applications: [] }));
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    try {
      const url = 'http://127.0.0.1:' + server.address().port;
      const client = new SandboxClient({ apiUrl: url }, true);
      const sandbox = new Sandbox({ sandboxId: 'packaged', proxyUrl: url });
      assert.deepEqual(Array.from(await client.list()), []);
      assert.deepEqual(Array.from(await sandbox.listProcesses()), []);
      for (const Client of [CloudClient, CommonJSCloudClient]) {
        const cloud = new Client({ apiUrl: url });
        assert.deepEqual(await cloud.applications(), []);
        cloud.close();
      }
      sandbox.close(); client.close();
    } finally { server.closeAllConnections(); server.close(); }
  `);
  for (const flag of ["--require", "--import"]) {
    const preload = path.join(directory, flag === "--require" ? "preload.cjs" : "preload.mjs");
    await writeFile(preload, flag === "--require"
      ? `require('node:fs').appendFileSync(process.env.TENSORLAKE_TEST_PRELOAD_LOG, String(require('node:worker_threads').threadId) + '\\n');`
      : `import {appendFileSync} from 'node:fs'; import {threadId} from 'node:worker_threads'; appendFileSync(process.env.TENSORLAKE_TEST_PRELOAD_LOG, String(threadId) + '\\n');`);
    for (const source of ["cli", "env"]) {
      const log = path.join(directory, `${flag}-${source}.log`);
      const child = spawn(process.execPath, [...(source === "cli" ? [flag, preload] : []), driver], {
        cwd: directory, stdio: "inherit", timeout: 20_000, killSignal: "SIGKILL",
        env: { ...process.env, TENSORLAKE_TEST_PRELOAD_LOG: log, NODE_OPTIONS: source === "env" ? `${flag}=${JSON.stringify(preload)}` : "" },
      });
      const [code, signal] = await once(child, "exit");
      assert.equal(code, 0, `Packed worker test failed (${flag}, ${source}, ${signal ?? code})`);
      assert.equal(await readFile(log, "utf8"), "0\n", "Customer preload ran inside the SDK worker");
    }
  }
  // Capsules must carry exactly the worker tested in the SDK package.
  const worker = await readFile(path.join(sdk, "dist/native-worker.cjs"));
  for (const capsule of ["function-executor", "typescript-function-runner"]) {
    assert.deepEqual(await readFile(path.join(sdk, "runtime", capsule, "package/dist/native-worker.cjs")), worker);
  }
  console.log("Packed ESM/CommonJS SDK passed using optional native package; worker exits cleanly when idle.");
} finally {
  await rm(directory, { recursive: true, force: true });
}
