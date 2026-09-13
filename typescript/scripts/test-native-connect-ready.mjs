import assert from "node:assert/strict";
import { once } from "node:events";
import { createServer } from "node:http";
import { Sandbox } from "../dist/index.js";

let connections = 0;
let metadataReads = 0;
const requests = [];
const server = createServer((request, response) => {
  requests.push({ path: request.url, timeout: Number(request.headers["x-tensorlake-request-timeout-ms"]) });
  const value = request.url === "/api/v1/health"
    ? { healthy: true }
    : {
        id: "transport-test", namespace: "default",
        status: ++metadataReads <= 3 ? "pending" : "running",
        resources: { cpus: 1, memory_mb: 1024, disk_mb: 1024 },
        created_at: 0, sandbox_url: `http://127.0.0.1:${server.address().port}`,
      };
  response.setHeader("Content-Type", "application/json");
  response.end(JSON.stringify(value));
});
server.on("connection", () => connections++);
server.listen(0, "127.0.0.1");
await once(server, "listening");
let sandbox;
try {
  sandbox = await Sandbox.connect({
    sandboxId: "transport-test", apiUrl: `http://127.0.0.1:${server.address().port}`,
    requestTimeout: 3,
  });
  assert.equal((await sandbox.health()).healthy, true);
  assert.equal(metadataReads, 5);
  assert.equal(connections, 1, "Readiness polls must share the lifecycle client's connection pool");
  const deadlines = requests.filter(({ path }) => path !== "/api/v1/health").map(({ timeout }) => timeout);
  assert.ok(deadlines[0] > deadlines.at(-1));
  assert.ok(deadlines.every((value) => value > 0 && value <= 3000));
  assert.deepEqual(requests.at(-1), { path: "/api/v1/health", timeout: 3000 });
  console.log("Native connect readiness passed: five metadata reads, two health requests, one connection, independent deadlines.");
} finally {
  sandbox?.close();
  server.closeAllConnections();
  await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
}
