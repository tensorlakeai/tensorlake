import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

// Exercise the raw addon without the worker's streams map keeping the
// cancellation function or native client reachable until each call settles.
// A dangling native reference can abort Node, so isolate GC in a child.
if (!process.argv.includes("--stream-lifetime-child")) {
  const child = spawn(process.execPath, [
    "--expose-gc", fileURLToPath(import.meta.url), "--stream-lifetime-child",
  ], { stdio: "inherit", timeout: 30_000, killSignal: "SIGKILL" });
  const [code, signal] = await once(child, "exit");
  assert.equal(code, 0, `Native stream lifetime regression failed (${signal ?? code})`);
} else {
  const require = createRequire(import.meta.url);
  const { loadNative } = require("../lib/runtime.cjs");
  const binding = loadNative();
  let onRequest;
  const server = createServer((request, response) => {
    request.resume();
    response.setHeader("Content-Type", "text/event-stream");
    response.flushHeaders();
    onRequest(response);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const url = `http://127.0.0.1:${server.address().port}`;
  const methods = ["followStdout", "followStderr", "followOutput", "runProcessStreaming"];

  async function collect() {
    // Yield between collections to let V8 run native finalizers. The marker
    // ensures this test actually exercises GC without requiring a particular
    // implementation of the native control's ownership.
    let collected = false;
    const registry = new FinalizationRegistry(() => { collected = true; });
    registry.register({}, "marker");
    for (let i = 0; i < 50 && !collected; i++) {
      global.gc();
      await delay(10);
    }
    assert.ok(collected, "GC did not collect the test marker");
  }

  function start(method, argument) {
    // The native client is not retained by the caller.
    const client = new binding.NativeSandboxProxyClient(url, "sandbox");
    return client[method](argument, () => {});
  }

  async function checkStream(method, mode) {
    const incoming = new Promise((resolve) => { onRequest = resolve; });
    // Slow Rust parsing extends the interval before the native future first
    // uses cancellation. Previously, GC during this interval crashed Node.
    const argument = method === "runProcessStreaming"
      ? JSON.stringify({ command: "echo", args: mode === "complete" ? ["x".repeat(32 * 1024 * 1024)] : [] })
      : "quiet";
    // In the completion case, retain only the Promise so GC can collect the
    // cancellation function while Rust is parsing or waiting for output.
    let call = start(method, argument);
    const cancel = mode === "cancel" ? call.cancel : undefined;
    const promise = call.result;
    call = undefined;
    assert.ok(promise instanceof Promise);
    let settled = false;
    const result = promise.then(
      (value) => ({ value }),
      (error) => ({ error }),
    ).finally(() => { settled = true; });
    await collect();
    assert.equal(settled, false, `${method}: GC settled the stream before the request`);
    const response = await incoming;
    await collect();
    assert.equal(settled, false, `${method}: GC cancelled a quiet stream`);
    assert.equal(response.destroyed, false, `${method}: GC closed the HTTP response`);
    if (mode === "cancel") {
      const closed = once(response, "close");
      cancel();
      await closed;
    } else {
      response.end();
    }
    const outcome = await result;
    if (outcome.error) throw outcome.error;
    assert.equal(typeof outcome.value, "string");
    // Cancellation remains harmless after the stream has finished.
    cancel?.();
  }

  try {
    for (const method of methods) {
      for (const mode of ["complete", "cancel"]) {
        await checkStream(method, mode);
      }
    }
    // Invalid payloads still reject asynchronously after native state has
    // been copied; callers receive a Promise rather than a synchronous throw.
    const invalid = start("runProcessStreaming", "{").result;
    assert.ok(invalid instanceof Promise);
    await assert.rejects(invalid, /invalid JSON payload/);
    await collect();
    console.log("Native stream lifetime regression passed: all four methods own their state, survive GC, and cancel explicitly.");
  } finally {
    server.closeAllConnections();
    server.close();
  }
}
