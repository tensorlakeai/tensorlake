import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { CloudClient } from "../src/cloud-client.js";

describe("CloudClient", () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    globalThis.fetch = vi.fn(handler as typeof fetch);
  }

  it("runs single-part requests as raw bodies when the part name is 0", async () => {
    mockFetch((_url, init) => {
      const headers = init?.headers as Record<string, string>;
      expect(headers["Accept"]).toBe("application/json");
      expect(headers["Content-Type"]).toBe("application/json");
      expect(init?.body).toBe('{"city":"San Francisco"}');
      return new Response(JSON.stringify({ request_id: "req-1" }), {
        status: 200,
      });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    const requestId = await client.runRequest("weather", [
      {
        name: "0",
        data: '{"city":"San Francisco"}',
        contentType: "application/json",
      },
    ]);

    expect(requestId).toBe("req-1");
    client.close();
  });

  it("runs multipart requests for multiple named inputs", async () => {
    mockFetch(async (_url, init) => {
      const headers = init?.headers as Record<string, string>;
      expect(headers["Accept"]).toBe("application/json");
      expect(init?.body).toBeInstanceOf(FormData);

      const form = init?.body as FormData;
      const document = form.get("document");
      const config = form.get("config");
      expect(document).toBeTruthy();
      expect(config).toBeTruthy();
      expect(await (document as Blob).text()).toBe("hello");
      expect(await (config as Blob).text()).toBe('{"pages":[1]}');

      return new Response(JSON.stringify({ request_id: "req-2" }), {
        status: 200,
      });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    const requestId = await client.runRequest("parse", [
      {
        name: "document",
        data: "hello",
        contentType: "text/plain",
      },
      {
        name: "config",
        data: '{"pages":[1]}',
        contentType: "application/json",
      },
    ]);

    expect(requestId).toBe("req-2");
    client.close();
  });

  it("uploads applications as multipart form data", async () => {
    mockFetch(async (_url, init) => {
      expect(init?.body).toBeInstanceOf(FormData);
      const form = init?.body as FormData;
      expect(form.get("application")).toBe('{"name":"weather"}');
      expect(form.get("code_content_type")).toBe("application/zip");
      expect(form.get("upgrade_requests_to_latest_code")).toBe("true");
      const code = form.get("code");
      expect(code).toBeTruthy();
      expect(await (code as Blob).text()).toBe("zip-bytes");
      return new Response("", { status: 200 });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    await client.upsertApplication({ name: "weather" }, "zip-bytes", true);
    client.close();
  });

  it("streams build logs as camel-cased events", async () => {
    mockFetch(() =>
      new Response(
        'data: {"build_id":"build-1","timestamp":"2026-03-07T10:00:00Z","stream":"stderr","message":"hello","sequence_number":1,"build_status":"building"}\n\n',
        { status: 200 },
      ),
    );

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    const events = [];
    for await (const event of client.streamBuildLogs("/images/v2", "build-1")) {
      events.push(event);
    }

    expect(events).toEqual([
      {
        buildId: "build-1",
        timestamp: new Date("2026-03-07T10:00:00Z"),
        stream: "stderr",
        message: "hello",
        sequenceNumber: 1,
        buildStatus: "building",
      },
    ]);
    client.close();
  });
});
