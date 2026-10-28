import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";

// Public SDK + real native binding against a local contract server. This is
// not a live server/proxy test: lifecycle state changes below are scripted.
let autoResume = true;
let status = "running";
let legacy = false;
const requests = [];
const server = createServer(async (request, response) => {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString();
  const body = text ? JSON.parse(text) : {};
  const path = new URL(request.url, "http://localhost").pathname;
  requests.push({ method: request.method, url: path, body });
  response.setHeader("Content-Type", "application/json");
  let payload = {};
  if (path.endsWith("/resume")) status = "running";
  else if (path.endsWith("/suspend")) status = "suspended";
  else if (path.startsWith("/api/v1/files")) {
    response.statusCode = 409;
    payload = { error: "Automatic resume disabled", code: "SANDBOX_AUTO_RESUME_DISABLED" };
  } else if (request.method === "POST") {
    autoResume = body.auto_resume ?? true;
    payload = { sandbox_id: "sbx-1", status: "running" };
  } else {
    if (request.method === "PATCH") autoResume = body.auto_resume ?? autoResume;
    payload = {
      id: "sbx-1", name: "parked-agent", namespace: "default", status,
      resources: { cpus: 1, memory_mb: 512, disk_mb: 1024 }, sandbox_url: url,
      ...(!legacy && { auto_resume: autoResume }),
    };
    if (path.endsWith("/sandboxes")) payload = { sandboxes: [payload] };
  }
  response.end(JSON.stringify(payload));
});
server.listen(0, "127.0.0.1");
await once(server, "listening");
const url = `http://127.0.0.1:${server.address().port}`;
try {
  const { Sandbox, SandboxClient, RemoteAPIError } = await import("../dist/index.js");
  const sandbox = await Sandbox.create({ name: "parked-agent", autoResume: false, apiUrl: url, proxyUrl: url, apiKey: "test" });
  const client = new SandboxClient({ apiUrl: url, apiKey: "test" }, true);
  try {
    assert.equal(requests[0].body.auto_resume, false);
    assert.equal((await sandbox.info()).autoResume, false);
    assert.equal((await client.list())[0].autoResume, false);
    await sandbox.suspend();
    const before = requests.length;
    const error = await sandbox.readFile("/test").catch((e) => e);
    assert.ok(error instanceof RemoteAPIError);
    assert.equal(error.statusCode, 409);
    assert.equal(error.code, "SANDBOX_AUTO_RESUME_DISABLED");
    assert.equal(requests.length, before + 1, "proxy 409 must not retry or resume");
    await sandbox.resume();
    assert.equal((await sandbox.info()).autoResume, false);
    await sandbox.suspend();
    await sandbox.update({ autoResume: true });
    assert.equal((await sandbox.info()).autoResume, true);
    assert.equal(status, "suspended", "PATCH must not wake the sandbox");
    assert.equal(requests.filter((r) => r.url.endsWith("/resume")).length, 1);
    legacy = true;
    assert.equal((await client.get("sbx-1")).autoResume, true);
    assert.equal((await client.list())[0].autoResume, true);
    console.log("Native auto-resume create/inspect/park/resume/release, legacy defaults, and final proxy 409 passed.");
  } finally {
    sandbox.close();
    client.close();
  }
} finally {
  server.closeAllConnections();
  server.close();
}
