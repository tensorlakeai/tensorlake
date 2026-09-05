import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { setEnvironmentData } from "node:worker_threads";

// Run in a disposable process: certificate settings and native module state
// must be fresh, and the parent must be able to detect a blocked event loop.
if (!process.argv.includes("--child")) {
  await import("./test-native-stream-lifetime.mjs");
  if (process.platform !== "linux") {
    console.log("Forbidden certificate I/O regression requires Linux FIFOs.");
  } else {
    const child = spawn(process.execPath, [
      "--require", fileURLToPath(new URL("../tests/fixtures/delayed-native-load.cjs", import.meta.url)),
      fileURLToPath(import.meta.url), "--child",
    ], {
      stdio: "inherit",
      timeout: 20_000,
      killSignal: "SIGKILL",
    });
    const [code, signal] = await once(child, "exit");
    assert.equal(code, 0, `Native sandbox regression failed (${signal ?? code})`);
  }
} else {
  const directory = mkdtempSync(path.join(tmpdir(), "tensorlake-native-"));
  const fifo = path.join(directory, "certificates.pem");
  const emptyDirectory = path.join(directory, "empty");
  mkdirSync(emptyDirectory);
  assert.equal(spawnSync("mkfifo", [fifo]).status, 0);
  process.env.SSL_CERT_FILE = fifo;
  process.env.SSL_CERT_DIR = emptyDirectory;

  // No writer ever opens this FIFO. Any regression to platform CA discovery
  // blocks initialization and is killed by the parent's 20s deadline. Shipped
  // clients must use bundled roots, independent of guest certificate files.

  let connections = 0;
  let streamsClosed = 0;
  let fileContents = Buffer.from([0, 255, 128, 7]);
  const authorizations = [];
  let failNextRequest = false;
  const server = createServer((request, response) => {
    authorizations.push(request.headers.authorization);
    if (failNextRequest) {
      failNextRequest = false;
      response.writeHead(400, { "Content-Type": "application/json" });
      response.end(JSON.stringify({ message: "injected request failure" }));
      return;
    }
    if (request.url.endsWith("/follow")) {
      response.setHeader("Content-Type", "text/event-stream");
      response.flushHeaders();
      if (!request.url.includes("/quiet/")) {
        for (let i = 0; i < 20; i++) response.write(`data: ${JSON.stringify({ line: String(i), timestamp: i })}\n\n`);
      }
      response.on("close", () => { streamsClosed++; });
      return;
    }
    if (request.url.startsWith("/api/v1/files?")) {
      if (request.method === "PUT") {
        const chunks = [];
        request.on("data", (chunk) => chunks.push(chunk));
        request.on("end", () => { fileContents = Buffer.concat(chunks); response.end(); });
      } else {
        response.end(fileContents);
      }
      return;
    }
    response.setHeader("Content-Type", "application/json");
    response.end(JSON.stringify({ sandboxes: [], processes: [] }));
  });
  server.on("connection", () => { connections++; });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const url = `http://127.0.0.1:${server.address().port}`;
  let ticks = 0;
  let reports = 0;
  const originalGetReport = process.report.getReport;
  const originalDlopen = process.dlopen;
  // Shared fixture state: addon load count, blocked flag, main-thread release.
  const nativeLoads = new Int32Array(new SharedArrayBuffer(3 * Int32Array.BYTES_PER_ELEMENT));
  let blockedTicks = 0;
  setEnvironmentData("tensorlake-test-native-loads", nativeLoads.buffer);
  process.dlopen = () => { throw new Error("Sandbox SDK must not load native code on the main thread"); };
  process.report.getReport = () => {
    reports++;
    throw new Error("SDK must not generate diagnostic reports");
  };
  const timer = setInterval(() => {
    ticks++;
    if (Atomics.load(nativeLoads, 1) === 1 && Atomics.load(nativeLoads, 2) === 0) {
      if (++blockedTicks === 3) {
        console.log("Main event loop advanced while addon loading was blocked; releasing worker");
        Atomics.store(nativeLoads, 2, 1);
        Atomics.notify(nativeLoads, 2);
      }
    }
  }, 10);

  try {
    const { Sandbox, SandboxClient } = await import("../dist/index.js");
    const setupStart = performance.now();
    const client = new SandboxClient({ apiUrl: url, apiKey: "client-one", timeoutMs: 1_000 }, true);
    const handles = Array.from({ length: 20 }, (_, i) => client.connect(`sandbox-${i}`, url));
    const direct = await Sandbox.connect({ sandboxId: "static-connect", proxyUrl: url, apiUrl: url });
    const standalone = new Sandbox({ sandboxId: "direct-constructor", proxyUrl: url });
    const setupMs = performance.now() - setupStart;
    assert.ok(setupMs < 500, `Handle construction blocked for ${setupMs.toFixed(1)} ms`);
    assert.equal(Atomics.load(nativeLoads, 0), 0, "Constructors must not load the native addon");
    assert.equal(reports, 0, "Platform detection generated a diagnostic report");

    ticks = 0;
    await Promise.all([client.list(), handles[0].listProcesses(), handles[1].listProcesses()]);
    assert.equal(blockedTicks, 3, "Main event loop must advance while addon loading is blocked");
    assert.equal(Atomics.load(nativeLoads, 0), 1, "Concurrent first requests must share addon loading");
    const initialConnections = connections;
    for (const handle of handles) await handle.listProcesses();
    await client.listLogProcesses("sandbox-0");
    assert.equal(connections, initialConnections, "Proxy handles rebuilt the connection pool");
    assert.ok(authorizations.every((value) => value === "Bearer client-one"));

    // A distinct client must keep its own credentials even after another
    // client's shared transport has been initialized.
    const other = new SandboxClient({ apiUrl: url, apiKey: "client-two" }, true);
    await other.list();
    assert.equal(authorizations.at(-1), "Bearer client-two");
    await client.list();
    assert.equal(authorizations.at(-1), "Bearer client-one");
    await direct.listProcesses();
    await standalone.listProcesses();
    assert.deepEqual(Array.from(await standalone.readFile("/bytes")), [0, 255, 128, 7]);
    const bytes = new Uint8Array([7, 128, 255, 0]);
    await standalone.writeFile("/bytes", bytes);
    assert.equal(bytes.byteLength, 4, "Upload detached the caller's buffer");
    assert.deepEqual(Array.from(await standalone.readFile("/bytes")), [7, 128, 255, 0]);

    // Load the CJS facade too. It must share the existing worker and preserve
    // native routing helpers without loading an addon on the main thread.
    const { createRequire } = await import("node:module");
    const commonJS = createRequire(import.meta.url)("../dist/index.cjs");
    const cjsSandbox = new commonJS.Sandbox({ sandboxId: "commonjs", proxyUrl: url });
    await cjsSandbox.listProcesses();
    assert.equal(Atomics.load(nativeLoads, 0), 1, "ESM and CommonJS must share one worker and addon load");

    const firstClosed = streamsClosed;
    const stream = standalone.followStdout(7)[Symbol.asyncIterator]();
    assert.equal((await stream.next()).value.line, "0");
    await stream.return();
    for (let i = 0; i < 100 && streamsClosed === firstClosed; i++) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(streamsClosed, firstClosed + 1, "Early return did not cancel native HTTP stream");

    const controller = new AbortController();
    const quiet = standalone.followStdout("quiet", { signal: controller.signal })[Symbol.asyncIterator]();
    const next = quiet.next();
    await new Promise((resolve) => setTimeout(resolve, 50));
    controller.abort();
    assert.equal((await next).done, true);
    for (let i = 0; i < 100 && streamsClosed === firstClosed + 1; i++) await new Promise((resolve) => setTimeout(resolve, 10));
    assert.equal(streamsClosed, firstClosed + 2, "Abort did not cancel a quiet native HTTP stream");

    // Empty/invalid platform CA files must not affect new clients either.
    // A failed request must still leave the initialized client usable.
    const certFile = path.join(directory, "retry.pem");
    writeFileSync(certFile, "");
    process.env.SSL_CERT_FILE = certFile;
    const retry = new SandboxClient({ apiUrl: url }, true);
    await retry.list();
    failNextRequest = true;
    await assert.rejects(retry.list());
    assert.equal(failNextRequest, false, "Injected failure did not reach the server");
    await retry.list();
    writeFileSync(certFile, "not a CA certificate\n");
    const invalidRoots = new SandboxClient({ apiUrl: url }, true);
    await invalidRoots.list();
    for (const handle of [...handles, direct, standalone, cjsSandbox]) handle.close();
    client.close(); other.close(); retry.close(); invalidRoots.close();
    console.log(`Native sandbox regression passed: ${setupMs.toFixed(1)} ms setup, ${ticks} timer ticks, worker-only addon loading, no platform CA discovery, shared transport, stream cancellation and request retry verified.`);
  } finally {
    clearInterval(timer);
    process.report.getReport = originalGetReport;
    process.dlopen = originalDlopen;
    server.closeAllConnections();
    server.close();
    rmSync(directory, { recursive: true, force: true });
  }
}
