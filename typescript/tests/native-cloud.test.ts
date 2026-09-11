import { createServer, type IncomingMessage, type ServerResponse, type Server } from "node:http";
import { once } from "node:events";
import { constants, createGzip, gzipSync } from "node:zlib";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { NativeCloudTransport } from "../src/native-cloud.js";
import { CloudClient } from "../src/cloud-client.js";
import { Sandbox } from "../src/sandbox.js";
import { RemoteAPIError, SandboxConnectionError } from "../src/errors.js";

// Run against the built worker/addon with `npm run test:native-cloud`. Ordinary
// unit tests use cloud-stub.ts and do not require native build artifacts.
describe.skipIf(process.env.TENSORLAKE_TEST_NATIVE_CLOUD !== "1")("native cloud transport", () => {
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
  function transport(overrides: Partial<ConstructorParameters<typeof NativeCloudTransport>[0]> = {}) {
    const client = new NativeCloudTransport({ baseUrl: url, userAgent: "tensorlake-test", maxRetries: 0, retryBackoffMs: 1, timeoutMs: 1000, ...overrides });
    clients.push(client);
    return client;
  }

  it("sends auth, scope and trace headers from Rust without loading the addon or fetching in the calling isolate", async () => {
    const captured: IncomingMessage[] = [];
    handler = (request, response) => { captured.push(request); response.end('{"applications":[]}'); };
    vi.spyOn(globalThis, "fetch").mockImplementation(() => { throw new Error("JS fetch used"); });
    vi.spyOn(process, "dlopen").mockImplementation(() => { throw new Error("Addon loaded in caller"); });
    const client = new CloudClient({ apiUrl: url, apiKey: "test-key", organizationId: "org", projectId: "project", namespace: "space /" });
    clients.push(client);
    expect(await client.applications()).toEqual([]);
    expect(captured[0].url).toBe("/v1/namespaces/space%20%2F/applications");
    expect(captured[0].headers.authorization).toBe("Bearer test-key");
    expect(captured[0].headers["x-forwarded-organization-id"]).toBe("org");
    expect(captured[0].headers["x-forwarded-project-id"]).toBe("project");
    expect(captured[0].headers["user-agent"]).toMatch(/^tensorlake-typescript-sdk\//);
    expect(captured[0].headers.traceparent).toMatch(/^00-[a-f0-9]{32}-[a-f0-9]{16}-01$/);
  });

  it("omits explicitly empty API keys and scope headers", async () => {
    let headers: IncomingMessage["headers"] | undefined;
    handler = (request, response) => {
      headers = request.headers;
      response.statusCode = request.headers.authorization === undefined ? 200 : 401;
      response.end('{"applications":[]}');
    };
    const client = new CloudClient({ apiUrl: url, apiKey: "", organizationId: "", projectId: "" });
    clients.push(client);
    expect(await client.applications()).toEqual([]);
    expect(headers?.authorization).toBeUndefined();
    expect(headers?.["x-forwarded-organization-id"]).toBeUndefined();
    expect(headers?.["x-forwarded-project-id"]).toBeUndefined();
  });

  it("sends Content-Length zero for bodyless POSTs", async () => {
    const captured: IncomingMessage[] = [];
    handler = (request, response) => {
      captured.push(request);
      response.statusCode = request.headers["content-length"] === "0" ? 200 : 411;
      response.end('{}');
    };
    const client = new CloudClient({ apiUrl: url });
    clients.push(client);
    await client.cancelBuild("/build-service", "build");
    expect(captured).toHaveLength(1);
    expect(captured[0].headers["content-length"]).toBe("0");
  });

  it("preserves arbitrary bytes, content type and returned trace identity", async () => {
    let traceparent: string | undefined;
    handler = (request, response) => {
      traceparent = request.headers.traceparent as string;
      response.setHeader("Content-Type", "application/octet-stream");
      response.end(Buffer.from([0, 255, 128, 7]));
    };
    const response = await transport().requestResponse("GET", "/bytes");
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(new Uint8Array([0, 255, 128, 7]));
    expect(response.headers.get("content-type")).toBe("application/octet-stream");
    expect(traceparent).toContain(response.traceId);
  });

  it("advertises gzip and decodes gzip JSON, binary responses and SSE", async () => {
    const bytes = Buffer.from([0, 255, 128, 7]);
    handler = (request, response) => {
      const body = request.url === "/bytes" ? bytes
        : Buffer.from(request.url === "/events" ? 'data: {"text":"héllo"}\n\n' : '{"applications":[]}');
      expect(request.headers["accept-encoding"]).toBe("gzip");
      const encoded = gzipSync(body);
      response.writeHead(200, {
        "Content-Encoding": "gzip",
        "Content-Length": encoded.length,
        "Content-Type": request.url === "/bytes" ? "application/octet-stream"
          : request.url === "/events" ? "text/event-stream" : "application/json",
      });
      response.end(encoded);
    };
    const cloud = new CloudClient({ apiUrl: url });
    clients.push(cloud);
    expect(await cloud.applications()).toEqual([]);
    const client = transport();
    const response = await client.requestResponse("GET", "/bytes");
    expect(new Uint8Array(await response.arrayBuffer())).toEqual(Uint8Array.from(bytes));
    expect(response.headers.get("content-type")).toBe("application/octet-stream");
    expect(response.headers.has("content-encoding")).toBe(false);
    expect(response.headers.has("content-length")).toBe(false);
    const events = [];
    for await (const event of client.stream("GET", "/events")) events.push(event);
    expect(events).toEqual([{ text: "héllo" }]);
  });

  it("decodes a live gzip stream before EOF and cancels it on abort", async () => {
    let disconnected = false;
    handler = (_request, response) => {
      response.writeHead(200, { "Content-Type": "text/event-stream", "Content-Encoding": "gzip" });
      const gzip = createGzip();
      gzip.pipe(response);
      gzip.write('data: {"line":"live"}\n\n');
      gzip.flush(constants.Z_SYNC_FLUSH);
      response.on("close", () => { disconnected = true; gzip.destroy(); });
    };
    const controller = new AbortController();
    const stream = transport().stream("GET", "/events", controller.signal);
    expect((await stream.next()).value).toEqual({ line: "live" });
    const next = stream.next();
    controller.abort();
    expect((await next).done).toBe(true);
    await vi.waitFor(() => expect(disconnected).toBe(true));
  });

  it("accepts empty and bodyless gzip responses", async () => {
    handler = (request, response) => {
      response.writeHead(request.url === "/no-content" ? 204 : 200, { "Content-Encoding": "gzip" });
      response.end();
    };
    const client = transport();
    for (const [method, path] of [["GET", "/empty"], ["GET", "/no-content"], ["HEAD", "/head"]]) {
      expect(await (await client.requestResponse(method, path)).text()).toBe("");
    }
  });

  it("decodes concatenated gzip members", async () => {
    handler = (_request, response) => {
      response.writeHead(200, { "Content-Encoding": "gzip" });
      response.end(Buffer.concat([gzipSync('{"ok":'), gzipSync('true}')]));
    };
    expect(await transport().requestJson("GET", "/gzip")).toMatchObject({ ok: true });
  });

  it("rejects a truncated HTTP body even after a complete gzip member", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      attempts++;
      const body = gzipSync('{"ok":true}');
      response.writeHead(200, { "Content-Encoding": "gzip", "Content-Length": body.length + 100 });
      response.write(body);
      setImmediate(() => response.destroy());
    };
    await expect(transport({ maxRetries: 1 }).requestJson("GET", "/truncated-compressed"))
      .rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(2);
  });

  it("retries corrupt compressed reads without replaying mutations or losing HTTP error status", async () => {
    let attempts = 0;
    handler = (request, response) => {
      attempts++;
      response.writeHead(request.url === "/denied" ? 401 : 200, { "Content-Encoding": "gzip" });
      response.end("corrupt gzip");
    };
    const client = transport({ maxRetries: 1 });
    await expect(client.requestJson("GET", "/corrupt")).rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(2);
    await expect(client.requestResponse("POST", "/corrupt")).rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(3);
    await expect(client.requestJson("GET", "/denied")).rejects.toMatchObject({ statusCode: 401 });
    expect(attempts).toBe(4);
  });

  it("keeps the request deadline while waiting for the rest of a compressed body", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      attempts++;
      response.writeHead(200, { "Content-Encoding": "gzip" });
      response.write(gzipSync("incomplete").subarray(0, 12));
    };
    await expect(transport({ maxRetries: 2, timeoutMs: 80 }).requestJson("GET", "/stalled-gzip"))
      .rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(1);
  });

  it("builds multipart requests in Rust with field names, filenames, MIME types and binary contents", async () => {
    let received: FormData | undefined;
    handler = (request, response) => {
      void (async () => {
        const chunks = [];
        for await (const chunk of request) chunks.push(chunk as Buffer);
        received = await new Response(Buffer.concat(chunks), { headers: { "Content-Type": request.headers["content-type"]! } }).formData();
        response.end("{}");
      })();
    };
    const form = new FormData();
    form.append("application", '{"name":"cloud"}');
    form.append("code", new Blob([new Uint8Array([0, 255, 128])], { type: "application/zip" }), "code.zip");
    await transport().requestResponse("POST", "/upload", { body: form });
    expect(received?.get("application")).toBe('{"name":"cloud"}');
    const code = received?.get("code") as File;
    expect(code.name).toBe("code.zip");
    expect(code.type).toBe("application/zip");
    expect(new Uint8Array(await code.arrayBuffer())).toEqual(new Uint8Array([0, 255, 128]));
  });

  it("preserves expected 404/409 responses and maps other HTTP errors", async () => {
    handler = (request, response) => { response.statusCode = Number(request.url!.slice(1)); response.end('{"message":"test error"}'); };
    const client = transport();
    for (const status of [404, 409]) {
      expect((await client.requestResponse("GET", `/${status}`, { statusOnlyCodes: new Set([status]) })).status).toBe(status);
    }
    await expect(client.requestResponse("GET", "/401")).rejects.toBeInstanceOf(RemoteAPIError);
    await expect(client.requestResponse("GET", "/500")).rejects.toThrow("test error");
  });

  it("returns a missing image after 404 even when its unused error body is truncated", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      attempts++;
      response.writeHead(404, { "Content-Length": 100 });
      response.write("not found");
      setImmediate(() => response.destroy());
    };
    const client = new CloudClient({ apiUrl: url, maxRetries: 1, retryBackoffMs: 1 });
    clients.push(client);
    expect(await client.findSandboxImageByName("missing")).toBeNull();
    expect(attempts).toBe(1);
  });

  it("updates an existing secret after 409 even when its unused error body is truncated", async () => {
    const requests: string[] = [];
    handler = (request, response) => {
      requests.push(`${request.method} ${request.url}`);
      if (request.url?.endsWith("/secrets")) {
        response.writeHead(409, { "Content-Length": 100 });
        response.write("already exists");
        setImmediate(() => response.destroy());
      } else if (request.method === "GET") response.end('{"id":"existing"}');
      else response.end('{"id":"existing","name":"token","created_at_ms":0}');
    };
    const client = new CloudClient({ apiUrl: url, namespace: "test" });
    clients.push(client);
    expect(await client.upsertSecrets({ name: "token", value: "updated" })).toMatchObject({ id: "existing" });
    expect(requests).toEqual([
      "POST /v1/namespaces/test/secrets",
      "GET /v1/namespaces/test/secret-names/token",
      "POST /v1/namespaces/test/secrets/existing/versions",
    ]);
  });

  it.each([404, 409])("returns status-only HTTP %s without waiting for a stalled body", async (status) => {
    let disconnected = false;
    handler = (_request, response) => {
      response.writeHead(status, { "Content-Length": 100 });
      response.flushHeaders();
      response.on("close", () => { disconnected = true; });
    };
    const response = await transport({ timeoutMs: 250 }).requestResponse("GET", "/status", { statusOnlyCodes: new Set([status]) });
    expect(response.status).toBe(status);
    expect(await response.text()).toBe("");
    await vi.waitFor(() => expect(disconnected).toBe(true));
  });

  it.each([401, 403])("preserves HTTP %s and avoids retries when its error body is truncated", async (status) => {
    let attempts = 0;
    handler = (_request, response) => {
      attempts++;
      response.writeHead(status, { "Content-Length": 100 });
      response.write('{"message":"denied"}');
      setImmediate(() => response.destroy());
    };
    const client = transport({ maxRetries: 2 });
    await expect(client.requestJson("GET", "/denied")).rejects.toMatchObject({ statusCode: status });
    expect(attempts).toBe(1);
    await expect(client.stream("GET", "/events").next()).rejects.toMatchObject({ statusCode: status });
    expect(attempts).toBe(2);
  });

  it("gives concurrent requests independent deadlines", async () => {
    handler = () => {};
    const client = transport({ timeoutMs: 80 });
    const results = await Promise.allSettled([
      client.requestResponse("GET", "/first"), client.requestResponse("GET", "/second"),
    ]);
    expect(results.map((result) => result.status)).toEqual(["rejected", "rejected"]);
    for (const result of results) if (result.status === "rejected") expect(result.reason).toBeInstanceOf(SandboxConnectionError);
  });

  it("keeps the deadline active after response headers arrive", async () => {
    handler = (_request, response) => { response.writeHead(200); response.flushHeaders(); };
    await expect(transport({ timeoutMs: 80 }).requestJson("GET", "/stalled-body")).rejects.toBeInstanceOf(SandboxConnectionError);
  });

  it("cancels every active request on close and rejects subsequent calls", async () => {
    let requests = 0;
    handler = () => { requests++; };
    const client = transport({ maxRetries: 3 });
    const results = Promise.allSettled([client.requestResponse("GET", "/a"), client.requestResponse("POST", "/b")]);
    await vi.waitFor(() => expect(requests).toBe(2));
    client.close();
    expect((await results).map((result) => result.status)).toEqual(["rejected", "rejected"]);
    await expect(client.requestResponse("GET", "/c")).rejects.toThrow("closed");
    expect(requests).toBe(2);
  });

  it("retries transient reads and cancels retry backoff on close", async () => {
    let attempts = 0;
    handler = (_request, response) => { response.statusCode = ++attempts === 1 ? 503 : 200; response.end("{}"); };
    await transport({ maxRetries: 1 }).requestJson("GET", "/retry");
    expect(attempts).toBe(2);
    attempts = 0;
    const client = transport({ maxRetries: 3, retryBackoffMs: 500 });
    const result = Promise.allSettled([client.requestJson("GET", "/retry")]);
    await vi.waitFor(() => expect(attempts).toBe(1));
    client.close();
    expect((await result)[0].status).toBe("rejected");
    expect(attempts).toBe(1);
  });

  it("keeps transient backoff independent from undelivered retries", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      attempts++;
      response.statusCode = attempts <= 2 ? 503 : 200;
      response.end(attempts === 1 ? '{"code":"AUTH_SERVICE_UNAVAILABLE"}' : '{}');
    };
    // 150 ms for the undelivered replay + 1 s for the first transient retry.
    // Counting both together incorrectly waits 2 s and misses the deadline.
    await transport({ maxRetries: 1, retryBackoffMs: 1000, timeoutMs: 1800 }).requestJson("GET", "/mixed");
    expect(attempts).toBe(3);
  });

  it.each(["startProcess", "writeStdin"] as const)("does not broaden %s retries when adding cloud read retries", async (operation) => {
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

  it("does not replay mutations after a lost response or ambiguous 503", async () => {
    let attempts = 0;
    handler = (request) => { attempts++; request.socket.destroy(); };
    const client = transport({ maxRetries: 3 });
    await expect(client.requestResponse("POST", "/invoke")).rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(1);
    handler = (_request, response) => { attempts++; response.writeHead(503); response.end("unavailable"); };
    await expect(client.requestResponse("POST", "/invoke")).rejects.toBeInstanceOf(RemoteAPIError);
    expect(attempts).toBe(2);
  });

  it.each([301, 302, 303, 307, 308])("rejects a mutation's %s redirect without following or replaying it", async (status) => {
    let mutations = 0;
    let redirected = 0;
    handler = (request, response) => {
      if (request.url === "/result") {
        redirected++;
        response.writeHead(503);
        response.end('{"code":"AUTH_SERVICE_UNAVAILABLE"}');
      } else {
        mutations++;
        response.writeHead(status, { Location: `${url}/result` });
        response.end();
      }
    };
    const client = transport({ timeoutMs: 500 });
    await expect(client.requestResponse("POST", "/invoke")).rejects.toMatchObject({ statusCode: status });
    expect(mutations).toBe(1);
    expect(redirected).toBe(0);
  });

  it("does not replay a POST that redirects to an unavailable result with maxRetries zero", async () => {
    const unavailable = createServer();
    unavailable.listen(0, "127.0.0.1");
    await once(unavailable, "listening");
    const port = (unavailable.address() as { port: number }).port;
    await new Promise<void>((resolve) => unavailable.close(() => resolve()));
    let mutations = 0;
    handler = (_request, response) => {
      mutations++;
      response.writeHead(303, { Location: `http://127.0.0.1:${port}/result` });
      response.end();
    };
    await expect(transport({ timeoutMs: 500, maxRetries: 0 }).requestResponse("POST", "/invoke"))
      .rejects.toMatchObject({ statusCode: 303 });
    expect(mutations).toBe(1);
  });

  it("continues to follow redirects for safe reads", async () => {
    const paths: string[] = [];
    handler = (request, response) => {
      paths.push(request.url!);
      if (request.url === "/before") {
        response.writeHead(307, { Location: `${url}/after` });
        response.end();
      } else response.end('{"ok":true}');
    };
    expect(await transport().requestJson("GET", "/before")).toMatchObject({ ok: true });
    expect(paths).toEqual(["/before", "/after"]);
  });

  it.each(["before headers", "during body"])("retries a safe read after a connection drops %s", async (phase) => {
    let attempts = 0;
    handler = (request, response) => {
      if (++attempts === 1) {
        if (phase === "before headers") request.socket.destroy();
        else {
          response.writeHead(200, { "Content-Length": 100 });
          response.write('{"applications":');
          setImmediate(() => response.destroy());
        }
      } else response.end('{"applications":[]}');
    };
    const client = new CloudClient({ apiUrl: url, maxRetries: 1, retryBackoffMs: 1 });
    clients.push(client);
    expect(await client.applications()).toEqual([]);
    expect(attempts).toBe(2);
  });

  it.each([0, 2])("bounds connection-reset retries by maxRetries=%s", async (maxRetries) => {
    let attempts = 0;
    handler = (request) => { attempts++; request.socket.destroy(); };
    await expect(transport({ maxRetries }).requestJson("GET", "/reset")).rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(maxRetries + 1);
  });

  it("retries stream establishment after a reset but never replays emitted events", async () => {
    let attempts = 0;
    handler = (request, response) => {
      if (++attempts === 1) request.socket.destroy();
      else {
        response.writeHead(200, { "Content-Type": "text/event-stream" });
        response.write('data: {"line":"once"}\n\n');
      }
    };
    const stream = transport({ maxRetries: 2 }).stream("GET", "/events");
    expect((await stream.next()).value).toEqual({ line: "once" });
    const next = stream.next();
    server.closeAllConnections();
    await expect(next).rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(2);
  });

  it("does not retry a read deadline or restart it after a connection reset", async () => {
    let attempts = 0;
    handler = (request) => { if (++attempts === 1) request.socket.destroy(); };
    await expect(transport({ maxRetries: 3, timeoutMs: 100 }).requestJson("GET", "/deadline"))
      .rejects.toBeInstanceOf(SandboxConnectionError);
    expect(attempts).toBe(2);
  });

  it("preserves read retries for rate limits", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      response.statusCode = ++attempts === 1 ? 429 : 200;
      response.end("{}");
    };
    await transport({ maxRetries: 1 }).requestJson("GET", "/limited");
    expect(attempts).toBe(2);
  });

  it("retains structured gateway codes so undelivered mutations can be replayed safely", async () => {
    let attempts = 0;
    handler = (_request, response) => {
      if (++attempts === 1) {
        response.writeHead(503);
        response.end('{"code":"AUTH_SERVICE_UNAVAILABLE","error":"Auth service unavailable"}');
      } else response.end("{}");
    };
    await transport().requestResponse("POST", "/invoke");
    expect(attempts).toBe(2);
  });

  it.each([undefined, "gzip"])("cancels a quiet stream with encoding %s and releases its HTTP connection", async (encoding) => {
    let connected = false;
    let disconnected = false;
    handler = (_request, response) => {
      connected = true;
      if (encoding) response.setHeader("Content-Encoding", encoding);
      response.writeHead(200, { "Content-Type": "text/event-stream" }); response.flushHeaders();
      response.on("close", () => { disconnected = true; });
    };
    const controller = new AbortController();
    const stream = transport().stream("GET", "/events", controller.signal);
    const next = stream.next();
    await vi.waitFor(() => expect(connected).toBe(true));
    controller.abort();
    expect((await next).done).toBe(true);
    await vi.waitFor(() => expect(disconnected).toBe(true));
  });

  it("cancels progress streams when the application finishes", async () => {
    let disconnected = false;
    handler = (_request, response) => {
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      response.write('data: {"RequestFinished":{}}\n\n');
      response.on("close", () => { disconnected = true; });
    };
    const client = new CloudClient({ apiUrl: url });
    clients.push(client);
    await client.waitOnRequestCompletion("app", "request");
    await vi.waitFor(() => expect(disconnected).toBe(true));
  });

  it("rejects HTTP errors before reading stream events", async () => {
    handler = (_request, response) => { response.writeHead(401); response.end('{"message":"unauthorized"}'); };
    const stream = transport().stream("GET", "/events");
    await expect(stream.next()).rejects.toBeInstanceOf(RemoteAPIError);
  });

  it("parses comments, CRLF, multiline JSON and UTF-8 while skipping malformed events", async () => {
    handler = (_request, response) => {
      response.writeHead(200, { "Content-Type": "text/event-stream" });
      const wire = Buffer.from(': keepalive\r\n\r\ndata: not-json\r\n\r\ndata: {"text":\r\ndata: "héllo"}\r\n\r\n');
      response.write(wire.subarray(0, wire.indexOf(Buffer.from("é")) + 1));
      response.end(wire.subarray(wire.indexOf(Buffer.from("é")) + 1));
    };
    const events = [];
    for await (const event of transport().stream("GET", "/events")) events.push(event);
    expect(events).toEqual([{ text: "héllo" }]);
  });
});
