// Exercise the public TypeScript API through the worker, N-API and Rust relay.
// Run after build:sdk and build:native (or after staging a debug native build).
import assert from "node:assert/strict";
import { once } from "node:events";
import { createRequire } from "node:module";
import net from "node:net";
import { setTimeout as delay } from "node:timers/promises";
import { WebSocketServer } from "ws";
import { TcpTunnel, Sandbox, SandboxError } from "../dist/index.js";

const deadline = setTimeout(() => {
  console.error("native tunnel regression exceeded 30 seconds");
  process.exit(1);
}, 30_000);
const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
await once(server, "listening");
const url = `http://127.0.0.1:${server.address().port}`;
const requests = [];
server.on("connection", (ws, request) => {
  requests.push(request);
  ws.on("error", () => {});
  ws.on("message", (data, binary) => {
    assert.equal(binary, true);
    ws.send(data, { binary: true });
  });
});
const tunnels = [];
try {
  // Check the raw binding too: forged JS receivers must fail before N-API
  // unwraps the Rust handle, including an object wrapping another Rust type.
  const require = createRequire(import.meta.url);
  const { loadNative } = require("../lib/runtime.cjs");
  const { NativeTcpTunnel, NativeSandboxProxyClient } = loadNative();
  const other = new NativeSandboxProxyClient(url, "sandbox");
  Object.setPrototypeOf(other, NativeTcpTunnel.prototype);
  const invalidReceivers = [
    {}, Object.create(NativeTcpTunnel.prototype), null, undefined, other,
  ];
  for (const receiver of invalidReceivers) {
    for (const [method, args] of [
      ["close", []], ["waitClosed", []],
      ["listen", [url, {}, 9000, "127.0.0.1", 0, 1]],
    ]) {
      await assert.rejects(async () => {
        await NativeTcpTunnel.prototype[method].call(receiver, ...args);
      }, /Illegal invocation/);
    }
  }
  const empty = new NativeTcpTunnel();
  await empty.close();
  await empty.waitClosed();
  const sandbox = new Sandbox({ sandboxId: "sbx-test", proxyUrl: url, apiKey: "test-key" });
  const tunnel = await sandbox.createTunnel(9000, { localPort: 0 });
  tunnels.push(tunnel);
  async function one(n) {
    const client = net.createConnection(tunnel.address());
    const expected = Buffer.alloc(n % 10 === 0 ? 256 * 1024 : 128, n);
    const received = [];
    let length = 0;
    const complete = new Promise((resolve, reject) => {
      client.on("data", (data) => {
        received.push(data);
        length += data.length;
        if (length === expected.length) resolve();
      });
      client.on("error", reject);
      client.on("end", () => { if (length !== expected.length) reject(new Error("premature EOF")); });
    });
    // Write before the connect callback, so data arrives during WS establishment.
    client.write(expected);
    try {
      await complete;
      assert.deepEqual(Buffer.concat(received), expected);
    } finally { client.destroy(); }
  }
  for (let start = 0; start < 100; start += 8) {
    await Promise.all(Array.from({ length: Math.min(8, 100 - start) }, (_, i) => one(start + i)));
  }
  assert.equal(requests.length, 100);
  for (const request of requests) {
    assert.equal(request.url, "/api/v1/tunnels/tcp?port=9000");
    assert.equal(request.headers.host, "sbx-test.local");
    assert.equal(request.headers.authorization, "Bearer test-key");
  }
  const active = net.createConnection(tunnel.address());
  await once(active, "connect");
  const ended = once(active, "end");
  active.resume();
  await Promise.all([tunnel.close(), tunnel.close()]);
  await ended;
  active.destroy();
  const refused = net.createConnection(tunnel.address());
  assert.equal((await once(refused, "error"))[0].code, "ECONNREFUSED");
  refused.destroy();

  const occupied = net.createServer();
  occupied.listen(0, "127.0.0.1");
  await once(occupied, "listening");
  try {
    await assert.rejects(TcpTunnel.listen({ baseUrl: url, wsHeaders: {}, remotePort: 9000,
      localPort: occupied.address().port }), SandboxError);
  } finally { await new Promise(resolve => occupied.close(resolve)); }

  let accepted;
  const stalled = net.createServer(socket => { accepted = socket; });
  stalled.listen(0, "127.0.0.1");
  await once(stalled, "listening");
  try {
    const pending = await TcpTunnel.listen({ baseUrl: `http://127.0.0.1:${stalled.address().port}`,
      wsHeaders: {}, remotePort: 9000, localPort: 0 });
    tunnels.push(pending);
    const client = net.createConnection(pending.address());
    await once(client, "connect");
    while (!accepted) await delay(1);
    const eof = once(client, "end");
    client.resume();
    await pending.close();
    await eof;
    client.destroy();
  } finally {
    accepted?.destroy();
    await new Promise(resolve => stalled.close(resolve));
  }
  console.log("TypeScript native tunnel: 100/100 immediate writes; 15/15 invalid receivers; routing, shutdown, bind errors and stalled handshake passed");
} finally {
  await Promise.all(tunnels.map(tunnel => tunnel.close()));
  for (const client of server.clients) client.terminate();
  await new Promise(resolve => server.close(resolve));
  clearTimeout(deadline);
}
