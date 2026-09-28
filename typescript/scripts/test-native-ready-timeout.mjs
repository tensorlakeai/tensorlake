import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { setTimeout as delay } from "node:timers/promises";

// `PendingSandbox.ready` honours its timeout through the real native binding
// (ADR 0086). A scripted server acknowledges the create at once, answers the
// first readiness poll with `pending` and stalls the second. `ready` must
// reject with `SandboxPending` within its budget plus a small margin, without
// waiting for the stalled answer and without issuing a DELETE.
const SECOND_POLL_DELAY_MS = 2_000;

const requests = [];
let failFirstPoll = false;
const server = createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  requests.push({ method: request.method, url: request.url, body: Buffer.concat(chunks).toString() });
  response.setHeader("Content-Type", "application/json");
  if (request.method === "POST") {
    response.writeHead(202);
    response.end(JSON.stringify({ sandbox_id: "sbx-1", state: "pending", pending_reason: "scheduling" }));
    return;
  }
  if (request.method === "GET") {
    const polls = requests.filter((r) => r.method === "GET").length;
    if (polls === 1 && failFirstPoll) {
      response.writeHead(503);
      response.end(JSON.stringify({ message: "busy" }));
      return;
    }
    if (polls > 1) await delay(SECOND_POLL_DELAY_MS);
    response.end(JSON.stringify({
      id: "sbx-1", namespace: "default", status: "pending",
      pending_reason: "no_resources_available",
      resources: { cpus: 1, memory_mb: 1024, disk_mb: 10240 },
    }));
    return;
  }
  response.end("{}");
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const url = `http://127.0.0.1:${server.address().port}`;

try {
  const { PendingSandbox, RemoteAPIError, SandboxClient, SandboxPending } = await import("../dist/index.js");
  const client = new SandboxClient({ apiUrl: url, requestTimeout: 30 }, true);
  const pending = await client.create({ image: "python:3.11", wait: false });
  assert.ok(pending instanceof PendingSandbox);
  assert.equal(pending.sandboxId, "sbx-1");
  assert.equal(JSON.parse(requests[0].body).wait, false);

  const budget = 0.05;
  const started = performance.now();
  const error = await pending.ready({ timeout: budget, pollInterval: 0.01 }).catch((e) => e);
  const elapsedMs = performance.now() - started;
  assert.ok(error instanceof SandboxPending, `expected SandboxPending, got ${error}`);
  assert.ok(elapsedMs < budget * 1000 + 300, `ready overshot its budget: ${elapsedMs.toFixed(1)} ms`);
  assert.equal(error.sandboxId, "sbx-1");
  assert.equal(error.pendingReason, "no_resources_available");
  assert.equal(error.timeout, budget);
  assert.ok(!requests.some((r) => r.method === "DELETE"), "a stalled poll must never DELETE");

  // A 503 on the first poll, then a stalled poll: the retry backoff consumes
  // the budget and no further poll may be granted the first-poll floor. The
  // last error surfaces within the budget.
  requests.length = 0;
  failFirstPoll = true;
  const failing = await client.create({ image: "python:3.11", wait: false });
  const failedStart = performance.now();
  const failure = await failing.ready({ timeout: budget, pollInterval: 0.01 }).catch((e) => e);
  const failedMs = performance.now() - failedStart;
  assert.ok(failure instanceof RemoteAPIError, `expected the 503 to surface, got ${failure}`);
  assert.equal(failure.statusCode, 503);
  assert.ok(failedMs < 150, `ready overshot its budget after a failed poll: ${failedMs.toFixed(1)} ms`);
  assert.ok(!requests.some((r) => r.method === "DELETE"), "a failed poll must never DELETE");
  client.close();
  console.log(`Native ready() timeout regression passed: ${elapsedMs.toFixed(1)} ms with a stalled second poll, ${failedMs.toFixed(1)} ms after a failed first poll, for a ${budget * 1000} ms budget.`);
} finally {
  server.closeAllConnections();
  server.close();
}
