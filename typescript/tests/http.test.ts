import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { HttpClient } from "../src/http.js";
import {
  PoolInUseError,
  PoolNotFoundError,
  RemoteAPIError,
  SandboxConnectionError,
  SandboxNotFoundError,
} from "../src/errors.js";

describe("HttpClient", () => {
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

  it("makes successful JSON requests", async () => {
    mockFetch(() => new Response(JSON.stringify({ ok: true }), { status: 200 }));

    const client = new HttpClient({ baseUrl: "http://localhost:8900" });
    const result = await client.requestJson<{ ok: boolean }>("GET", "/test");
    expect(result).toEqual({ ok: true });
    client.close();
  });

  it("sends auth headers", async () => {
    mockFetch((_url, init) => {
      const headers = init?.headers as Record<string, string>;
      expect(headers["Authorization"]).toBe("Bearer my-key");
      expect(headers["X-Organization-ID"]).toBe("org-1");
      expect(headers["X-Project-ID"]).toBe("proj-1");
      return new Response("{}", { status: 200 });
    });

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      apiKey: "my-key",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    await client.requestJson("GET", "/test");
    client.close();
  });

  it("sends Host header when specified", async () => {
    mockFetch((_url, init) => {
      const headers = init?.headers as Record<string, string>;
      expect(headers["Host"]).toBe("sbx-123.local");
      return new Response("{}", { status: 200 });
    });

    const client = new HttpClient({
      baseUrl: "http://localhost:9443",
      hostHeader: "sbx-123.local",
    });
    await client.requestJson("GET", "/test");
    client.close();
  });

  it("retries on 503", async () => {
    let attempts = 0;
    mockFetch(() => {
      attempts++;
      if (attempts < 3) {
        return new Response("unavailable", { status: 503 });
      }
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    });

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 3,
      retryBackoffMs: 10,
    });
    const result = await client.requestJson<{ ok: boolean }>("GET", "/test");
    expect(result).toEqual({ ok: true });
    expect(attempts).toBe(3);
    client.close();
  });

  it("throws RemoteAPIError on non-retryable status", async () => {
    mockFetch(() => new Response("bad request", { status: 400 }));

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    await expect(client.requestJson("POST", "/test")).rejects.toThrow(
      RemoteAPIError,
    );
    client.close();
  });

  it("throws SandboxNotFoundError on 404 for sandbox path", async () => {
    mockFetch(() => new Response("not found", { status: 404 }));

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    await expect(
      client.requestJson("GET", "/sandboxes/sbx-abc"),
    ).rejects.toThrow(SandboxNotFoundError);
    client.close();
  });

  it("throws PoolNotFoundError on 404 for pool path", async () => {
    mockFetch(() => new Response("not found", { status: 404 }));

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    await expect(
      client.requestJson("GET", "/sandbox-pools/pool-abc"),
    ).rejects.toThrow(PoolNotFoundError);
    client.close();
  });

  it("throws PoolInUseError on 409 for pool path", async () => {
    mockFetch(
      () => new Response("pool has active containers", { status: 409 }),
    );

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    await expect(
      client.requestJson("DELETE", "/sandbox-pools/pool-abc"),
    ).rejects.toThrow(PoolInUseError);
    client.close();
  });

  it("throws SandboxConnectionError on network failure", async () => {
    mockFetch(() => {
      throw new TypeError("fetch failed");
    });

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    await expect(client.requestJson("GET", "/test")).rejects.toThrow(
      SandboxConnectionError,
    );
    client.close();
  });

  it("retries on network failure then succeeds", async () => {
    let attempts = 0;
    mockFetch(() => {
      attempts++;
      if (attempts === 1) throw new TypeError("fetch failed");
      return new Response(JSON.stringify({ ok: true }), { status: 200 });
    });

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 2,
      retryBackoffMs: 10,
    });
    const result = await client.requestJson<{ ok: boolean }>("GET", "/test");
    expect(result).toEqual({ ok: true });
    expect(attempts).toBe(2);
    client.close();
  });

  it("parses error message from JSON body", async () => {
    mockFetch(
      () =>
        new Response(JSON.stringify({ message: "quota exceeded" }), {
          status: 429,
        }),
    );

    const client = new HttpClient({
      baseUrl: "http://localhost:8900",
      maxRetries: 0,
    });
    try {
      await client.requestJson("POST", "/test");
      expect.unreachable("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(RemoteAPIError);
      expect((err as RemoteAPIError).responseMessage).toBe("quota exceeded");
    }
    client.close();
  });
});
