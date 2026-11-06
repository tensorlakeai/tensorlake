// Public package entrypoints through the compiled native worker and local HTTP.
import assert from "node:assert/strict";
import http from "node:http";
import { once } from "node:events";
import { SandboxClient, SandboxResumeError, SandboxStatus, RemoteAPIError } from "../dist/index.js";

const requests = [];
const diagnosis = { message: "restore failed", debug_data: { attempt_id: 17 } };
let status = "resume_failed";
let reply = 409;
let body = "restore failed";
let nextStatus = "resume_failed";
const server = http.createServer((req, res) => {
  req.resume();
  const path = new URL(req.url, "http://localhost").pathname;
  requests.push([req.method, path]);
  if (path.endsWith("/resume")) {
    status = nextStatus;
    res.writeHead(reply);
    res.end(body);
    return;
  }
  res.setHeader("content-type", "application/json");
  if (req.method === "POST") {
    res.writeHead(202);
    res.end(JSON.stringify({ sandbox_id: "sbx-1", state: "pending" }));
    return;
  }
  const info = { id: "sbx-1", namespace: "default", status, snapshot_id: "snap-1",
    error_details: status === "running" ? null : diagnosis,
    resources: { cpus: 1, memory_mb: 512, disk_mb: 1024 }, sandbox_url: url };
  res.end(JSON.stringify(path.endsWith("/sandboxes") ? { sandboxes: [info] } : info));
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const url = `http://127.0.0.1:${server.address().port}`;
const client = SandboxClient.forLocalhost({ apiUrl: url });
try {
  // This is the documented get-before-resume contract, including root imports.
  const info = await client.get("sbx-1");
  assert.equal(info.status, SandboxStatus.RESUME_FAILED);
  assert.equal(info.snapshotId, "snap-1");
  assert.deepEqual(info.errorDetails, diagnosis);
  assert.deepEqual((await client.list())[0].errorDetails, diagnosis);
  const pending = await client.create({ wait: false });
  await assert.rejects(pending.ready({ timeout: 0, cancelOnTimeout: true }), SandboxResumeError);
  const connected = client.connect("sbx-1");
  await assert.rejects(connected.readFile("/tmp/test"), SandboxResumeError);
  connected.close();
  assert.equal(requests.filter(([, path]) => path.endsWith("/resume")).length, 0);
  if (info.status === SandboxStatus.RESUME_FAILED) {
    await assert.rejects(client.resume("sbx-1", { timeout: 120 }), (error) => {
      assert.ok(error instanceof SandboxResumeError);
      assert.equal(error.sandboxId, "sbx-1");
      assert.equal(error.status, "resume_failed");
      assert.equal(error.snapshotId, "snap-1");
      assert.deepEqual(error.errorDetails, diagnosis);
      return true;
    });
  }
  // Explicit retry clears diagnosis. Success responses have no JSON body.
  for (const code of [200, 202]) {
    reply = code; body = ""; nextStatus = "running";
    await client.resume("sbx-1", { timeout: 120 });
    assert.equal((await client.get("sbx-1")).status, SandboxStatus.RUNNING);
    assert.equal((await client.get("sbx-1")).errorDetails, undefined);
  }
  reply = 202; nextStatus = "resume_failed";
  await assert.rejects(client.resume("sbx-1", { timeout: 120 }), SandboxResumeError);
  reply = 409; body = '{"message":"Resident resume failed"}'; nextStatus = "pending";
  await assert.rejects(client.resume("sbx-1"), RemoteAPIError);
  assert.equal(requests.filter(([method]) => method === "DELETE").length, 0);
  assert.equal(requests.filter(([, path]) => path.endsWith("/resume")).length, 5);
  console.log("Recoverable resume HTTP/native contract passed");
} finally {
  client.close();
  server.closeAllConnections();
  await new Promise((resolve) => server.close(resolve));
}
