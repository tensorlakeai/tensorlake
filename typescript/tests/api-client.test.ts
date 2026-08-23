import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { APIClient } from "../src/api-client.js";
import {
  RequestExecutionError,
  RequestFailedError,
  RequestNotFinishedError,
} from "../src/errors.js";
import { clearNativeStub, installNativeStub } from "./native-stub.js";

vi.mock("undici", async (importOriginal) => {
  const actual = await importOriginal<typeof import("undici")>();
  return { ...actual, fetch: vi.fn() };
});

describe("APIClient", () => {

  afterEach(() => {
    clearNativeStub();
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

  it("creates a file system through Artifact Storage with only an API key", async () => {
    const stub = installNativeStub();
    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      apiKey: "api-key",
    });
    const fs = await client.createFileSystem("skills");
    expect(fs.id).toBe("skills");
    expect(stub.repository.createFilesystem).toHaveBeenCalledWith("skills");
    expect(stub.repositoryCtorArgs.slice(0, 4)).toEqual([
      "http://localhost:8900",
      "api-key",
      null,
      null,
    ]);
    client.close();
  });

  it("deletes a file system through Artifact Storage", async () => {
    const stub = installNativeStub();
    const client = new APIClient({
      apiUrl: "http://localhost:8900",
      apiKey: "api-key",
    });
    await client.deleteFileSystem("skills");
    expect(stub.repository.deleteFilesystem).toHaveBeenCalledWith("skills");
    client.close();
  });
});
