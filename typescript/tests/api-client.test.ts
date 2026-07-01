import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { APIClient } from "../src/api-client.js";
import {
  RequestExecutionError,
  RequestFailedError,
  RequestNotFinishedError,
} from "../src/errors.js";

vi.mock("undici", async (importOriginal) => {
  const actual = await importOriginal<typeof import("undici")>();
  return { ...actual, fetch: vi.fn() };
});

describe("APIClient", () => {

  afterEach(() => {
    vi.mocked(undici.fetch).mockReset();
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    vi.mocked(undici.fetch).mockImplementation(handler as typeof undici.fetch);
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

  it("creates a filesystem through the scoped platform route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/file-systems",
      );
      expect(init?.method).toBe("POST");
      expect(init?.body).toBe('{"name":"skills"}');
      return new Response(
        JSON.stringify({ id: "file_system_abc", name: "skills" }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const fs = await client.createFilesystem("skills");
    expect(fs.id).toBe("file_system_abc");
    client.close();
  });

  it("deletes a filesystem through the scoped platform route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/file-systems/file_system_abc",
      );
      expect(init?.method).toBe("DELETE");
      return new Response(null, { status: 204 });
    });

    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    await client.deleteFilesystem("file_system_abc");
    client.close();
  });
});
