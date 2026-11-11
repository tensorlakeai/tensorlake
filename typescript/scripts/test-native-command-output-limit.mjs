import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";
import { Sandbox } from "../dist/index.js";

// Exercise the actual native addon and Sandbox.run() against a local SSE
// server. A large stream is generated from reusable chunks, without buffering
// the entire response in the test process.
const smallEvents = [
  { line: "old output", stream: "stdout", timestamp: 1 },
  { line: "old error", stream: "stderr", timestamp: 2 },
  { line: "", stream: "stdout", timestamp: 3 },
  { line: "é\n界 🚀", stream: "stderr", timestamp: 4 },
  { line: "last output", stream: "stdout", timestamp: 5 },
];
const exit = { exit_code: null, signal: 9, reason: "timed_out" };
const encode = (event) => `data: ${JSON.stringify(event)}\n\n`;
const largeLine = "x".repeat(1024);
const largeCount = 152_000; // 148.4375 MiB of output, before separators.
const largeTail = "FINAL DIAGNOSTIC";
const largeChunk = Buffer.from(encode({ line: largeLine, stream: "stdout", timestamp: 1 }).repeat(64));
let requestCount = 0;
let traceId;
let sentExit = false;
let failure;
const server = createServer(async (request, response) => {
  try {
    requestCount++;
    assert.equal(request.method, "POST");
    assert.equal(request.url, "/api/v1/processes/run");
    let body = "";
    for await (const chunk of request) body += chunk;
    const payload = JSON.parse(body);
    assert.equal(payload.maxOutputBytes, undefined);
    assert.equal(payload.max_output_bytes, undefined);
    traceId = request.headers.traceparent.split("-")[1];
    response.setHeader("Content-Type", "text/event-stream");
    if (payload.command === "large") {
      assert.equal(largeCount % 64, 0);
      for (let i = 0; i < largeCount / 64; i++) {
        if (!response.write(largeChunk)) await once(response, "drain");
      }
      response.write(encode({ line: largeTail, stream: "stderr", timestamp: 2 }));
    } else if (payload.command === "small") {
      for (const event of smallEvents) response.write(encode(event));
    } else if (payload.command === "reset") {
      response.write(encode(smallEvents[0]));
      await delay(20);
      response.destroy();
      return;
    } else {
      assert.equal(payload.command, "empty");
    }
    // Reaching the cap must not close the stream or manufacture an exit code.
    await delay(20);
    sentExit = true;
    response.end(encode(exit));
  } catch (error) {
    failure = error;
    response.destroy(error);
  }
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const sandbox = new Sandbox({
  sandboxId: "output-limit-test", proxyUrl: `http://127.0.0.1:${server.address().port}`,
});

// Independent reference: fully materialize a small command's bytes with stream
// labels, take the suffix, then remove an incomplete leading UTF-8 character.
function expectedTail(events, limit) {
  const bytes = [];
  const seen = new Set();
  for (const { line, stream } of events) {
    const value = `${seen.has(stream) ? "\n" : ""}${line}`;
    seen.add(stream);
    for (const byte of Buffer.from(value)) bytes.push({ byte, stream });
  }
  const tail = bytes.slice(Math.max(0, bytes.length - limit));
  while (tail.length && (tail[0].byte & 0xc0) === 0x80) tail.shift();
  return {
    stdout: Buffer.from(tail.filter((entry) => entry.stream === "stdout").map((entry) => entry.byte)).toString(),
    stderr: Buffer.from(tail.filter((entry) => entry.stream === "stderr").map((entry) => entry.byte)).toString(),
    outputTruncated: bytes.length > limit,
  };
}

try {
  for (const maxOutputBytes of [0, 1, 12, 14, 18, 25, 1000]) {
    sentExit = false;
    const result = await sandbox.run("small", { maxOutputBytes });
    assert.deepEqual(result, {
      ...expectedTail(smallEvents, maxOutputBytes),
      exitCode: -9, reason: "timed_out", timedOut: true, traceId,
    });
    assert.ok(sentExit);
    assert.ok(Buffer.byteLength(result.stdout) + Buffer.byteLength(result.stderr) <= maxOutputBytes);
  }
  const empty = await sandbox.run("empty", { maxOutputBytes: 0 });
  assert.equal(empty.outputTruncated, false);
  const unlimited = await sandbox.run("small");
  const expected = expectedTail(smallEvents, Infinity);
  assert.equal(unlimited.stdout, expected.stdout);
  assert.equal(unlimited.stderr, expected.stderr);
  assert.equal(unlimited.outputTruncated, undefined);

  const requestsBeforeReset = requestCount;
  await assert.rejects(sandbox.run("reset", { maxOutputBytes: 1 }));
  assert.equal(requestCount, requestsBeforeReset + 1, "A mid-stream error must not replay the command");

  const maxOutputBytes = 1024 * 1024;
  sentExit = false;
  const result = await sandbox.run("large", { maxOutputBytes });
  // The diagnostic arrives last and must survive eviction of earlier stdout.
  assert.equal(result.stderr, largeTail);
  const expectedStdout = Array(1024).fill(largeLine).join("\n").slice(-(maxOutputBytes - largeTail.length));
  assert.equal(result.stdout, expectedStdout);
  assert.equal(Buffer.byteLength(result.stdout) + Buffer.byteLength(result.stderr), maxOutputBytes);
  assert.equal(result.outputTruncated, true);
  assert.equal(result.exitCode, -9);
  assert.equal(result.reason, "timed_out");
  assert.equal(result.timedOut, true);
  assert.equal(result.traceId, traceId);
  assert.ok(sentExit);
  if (failure) throw failure;
  console.log("Native output cap passed: 148.4 MiB of generated output retained as a 1 MiB tail; final diagnostics, exit status, UTF-8, trace IDs, and errors verified.");
} finally {
  sandbox.close();
  server.closeAllConnections();
  server.close();
}
