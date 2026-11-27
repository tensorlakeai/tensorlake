import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { once } from "node:events";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Sandbox } from "../src/sandbox.js";
import { SandboxClient } from "../src/client.js";
import { ResizeStatus } from "../src/models.js";

// Run against the built worker/addon with `npm run test:native-cloud`. Ordinary
// unit tests do not require native build artifacts.
describe.skipIf(process.env.TENSORLAKE_TEST_NATIVE_CLOUD !== "1")("native sandbox HTTP against a local server", () => {
  let server: Server;
  let url: string;
  let handler: (request: IncomingMessage, response: ServerResponse) => void;
  const clients: Array<{ close(): void }> = [];

  beforeEach(async () => {
    handler = (_request, response) => response.end("{}");
    server = createServer((request, response) => handler(request, response));
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    url = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  });
  afterEach(async () => {
    for (const client of clients.splice(0)) client.close();
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    vi.restoreAllMocks();
  });
  it.each([ResizeStatus.WAITING_FOR_CAPACITY, ResizeStatus.MIGRATING])("waits through resize %s and a pending sandbox migration", async (admittedStatus) => {
    function observation(resizeStatus: string, sandboxStatus = "running") {
      return {
        id: "sb-1", namespace: "default", runtime: "cloud-hypervisor", status: sandboxStatus,
        resources: { cpus: 1, memory_mb: resizeStatus === "succeeded" ? 2048 : 1024, disk_mb: 1024 },
        resource_resize: {
          generation: 7, status: resizeStatus, error_message: null,
          requested: { cpus: 1, memory_mb: 2048, disk_mb: 1024 },
        },
      };
    }
    const observations = [
      observation("pending"),
      observation(admittedStatus, admittedStatus === ResizeStatus.MIGRATING ? "pending" : "running"),
      observation("migrating", "pending"),
      observation("pending"),
      observation("succeeded"),
    ];
    const requests: Array<{ method: string | undefined; body: unknown }> = [];
    handler = async (request, response) => {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(chunk);
      const text = Buffer.concat(chunks).toString();
      requests.push({ method: request.method, body: text ? JSON.parse(text) : null });
      expect(observations.length).toBeGreaterThan(0);
      response.setHeader("Content-Type", "application/json");
      response.end(JSON.stringify(observations.shift()));
    };
    const client = new SandboxClient({ apiUrl: url, apiKey: "test" }, true);
    clients.push(client);
    const result = await client.update("sb-1", { memoryMb: 2048, timeout: 2, pollInterval: 0.001 });
    expect(result.resourceResize?.status).toBe(ResizeStatus.SUCCEEDED);
    expect(result.resources.memoryMb).toBe(2048);
    expect(requests.map((r) => r.method)).toEqual(["GET", "PATCH", "GET", "GET", "GET"]);
    expect(requests[1].body).toEqual({ resources: { memory_mb: 2048 } });
    expect(observations).toHaveLength(0);
  });

  it.each(["startProcess", "writeStdin"] as const)("does not broaden %s retries", async (operation) => {
    let attempts = 0;
    handler = (request, response) => {
      if (++attempts === 1) request.socket.destroy();
      else response.end('{"pid":123,"status":"running","command":"true","args":[],"started_at":0}');
    };
    const sandbox = new Sandbox({ sandboxId: "test", proxyUrl: url });
    clients.push(sandbox);
    const result = operation === "startProcess"
      ? sandbox.startProcess("true")
      : sandbox.writeStdin(123, Buffer.from("once\n"));
    await expect(result).rejects.toThrow();
    expect(attempts).toBe(1);
  });
});
