import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { APIClient } from "../src/api-client.js";
import {
  RequestExecutionError,
  RequestFailedError,
  RequestNotFinishedError,
} from "../src/errors.js";

describe("APIClient", () => {
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

  it("throws RequestNotFinishedError when output is requested too early", async () => {
    mockFetch(() =>
      new Response(
        JSON.stringify({
          id: "req-1",
          outcome: null,
          applicationVersion: "v1",
          createdAt: 1700000000,
        }),
        { status: 200 },
      ),
    );

    const client = new APIClient({ apiUrl: "http://localhost:8900" });
    await expect(client.requestOutput("app", "req-1")).rejects.toThrow(
      RequestNotFinishedError,
    );
    client.close();
  });

  it("throws RequestExecutionError when request metadata includes requestError", async () => {
    mockFetch(() =>
      new Response(
        JSON.stringify({
          id: "req-1",
          outcome: { failure: "request_error" },
          applicationVersion: "v1",
          createdAt: 1700000000,
          requestError: {
            functionName: "main",
            message: "bad input",
          },
        }),
        { status: 200 },
      ),
    );

    const client = new APIClient({ apiUrl: "http://localhost:8900" });
    await expect(client.requestOutput("app", "req-1")).rejects.toThrow(
      RequestExecutionError,
    );
    client.close();
  });

  it("throws RequestFailedError when the request failed without requestError details", async () => {
    mockFetch(() =>
      new Response(
        JSON.stringify({
          id: "req-1",
          outcome: { failure: "internal_error" },
          applicationVersion: "v1",
          createdAt: 1700000000,
        }),
        { status: 200 },
      ),
    );

    const client = new APIClient({ apiUrl: "http://localhost:8900" });
    await expect(client.requestOutput("app", "req-1")).rejects.toThrow(
      RequestFailedError,
    );
    client.close();
  });
});
