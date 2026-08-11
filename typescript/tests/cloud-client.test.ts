import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { CloudClient } from "../src/cloud-client.js";
import { RemoteRequest, remoteOptions, runRemote } from "../src/applications/remote.js";
import { FunctionError, RequestError } from "../src/applications/errors.js";
import { File } from "../src/applications/file.js";
import { registerApplication } from "../src/applications/function.js";
import { RequestExecutionError, RequestFailedError } from "../src/errors.js";

vi.mock("undici", async (importOriginal) => {
  const actual = await importOriginal<typeof import("undici")>();
  return { ...actual, fetch: vi.fn() };
});

describe("CloudClient", () => {

  afterEach(() => {
    vi.mocked(undici.fetch).mockReset();
    vi.restoreAllMocks();
  });

  function mockFetch(
    handler: (url: string, init?: RequestInit) => Response | Promise<Response>,
  ) {
    vi.mocked(undici.fetch).mockImplementation(handler as typeof undici.fetch);
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

  it("keeps the legacy variadic name-only invocation signature", async () => {
    mockFetch((url, init) => {
      expect(url).toContain("/v1/namespaces/default/applications/echo");
      expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe('"Ada"');
      expect((init?.headers as Record<string, string>)["Content-Type"]).toBe(
        "application/json; charset=UTF-8",
      );
      return new Response(JSON.stringify({ request_id: "req-remote" }), { status: 200 });
    });

    const request = await runRemote<string>("echo", "Ada");

    expect(request.id).toBe("req-remote");
  });

  it("extracts explicit remote options from name-only invocations", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8913/v1/namespaces/default/applications/echo",
      );
      expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe('"Ada"');
      return new Response(JSON.stringify({ request_id: "req-named-options" }), { status: 200 });
    });

    const request = await runRemote<string>(
      "echo",
      "Ada",
      remoteOptions({ apiUrl: "http://localhost:8913" }),
    );

    expect(request.id).toBe("req-named-options");
  });

  it("accepts client options after omitted default application arguments", async () => {
    const application = registerApplication(
      "remote_default_options",
      async (name = "world") => `Hello, ${name}`,
    );
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8911/v1/namespaces/default/applications/remote_default_options",
      );
      expect(init?.body).toBeInstanceOf(ArrayBuffer);
      expect((init?.body as ArrayBuffer).byteLength).toBe(0);
      return new Response(JSON.stringify({ request_id: "req-default" }), { status: 200 });
    });

    const request = await runRemote(
      application,
      remoteOptions({ apiUrl: "http://localhost:8911", apiKey: undefined }),
    );

    expect(request.id).toBe("req-default");
  });

  it("rejects explicit remote options when a required application argument is missing", async () => {
    const application = registerApplication(
      "remote_required_options",
      async (name: string) => `Hello, ${name}`,
    );

    await expect(runRemote(
      application,
      remoteOptions({ apiUrl: "http://localhost:8911" }),
    )).rejects.toThrow("missing a required application argument");
    expect(undici.fetch).not.toHaveBeenCalled();
  });

  it("keeps CloudClientOptions-shaped application objects as application inputs", async () => {
    const application = registerApplication(
      "remote_options_shaped_input",
      async (input: { apiUrl: string }) => input.apiUrl,
    );
    mockFetch((_url, init) => {
      expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe(
        '{"apiUrl":"payload-value"}',
      );
      return new Response(JSON.stringify({ request_id: "req-object-input" }), { status: 200 });
    });

    const request = await runRemote(application, { apiUrl: "payload-value" });

    expect(request.id).toBe("req-object-input");
  });

  it("accepts empty legacy client options after all application arguments", async () => {
    const application = registerApplication(
      "remote_empty_options",
      async () => "ok",
    );
    mockFetch(() => new Response(
      JSON.stringify({ request_id: "req-empty-options" }),
      { status: 200 },
    ));

    const request = await runRemote(application, {});

    expect(request.id).toBe("req-empty-options");
  });

  it("keeps legacy trailing client options when all application arguments are supplied", async () => {
    const application = registerApplication(
      "remote_full_arguments",
      async (name = "world") => `Hello, ${name}`,
    );
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8912/v1/namespaces/default/applications/remote_full_arguments",
      );
      expect(new TextDecoder().decode(init?.body as ArrayBuffer)).toBe('"Ada"');
      return new Response(JSON.stringify({ request_id: "req-full-arguments" }), { status: 200 });
    });

    const request = await runRemote(application, "Ada", { apiUrl: "http://localhost:8912" });

    expect(request.id).toBe("req-full-arguments");
  });

  it("uses a registered application's return schema for JSON MIME files", async () => {
    const bytes = new TextEncoder().encode('{"raw":true}');
    const client = {
      waitOnRequestCompletion: async () => undefined,
      requestOutput: async () => ({
        serializedValue: bytes,
        contentType: "application/json",
      }),
    };
    const request = new RemoteRequest<File>("req-file", "file_app", client as never, true);

    const output = await request.output();
    expect(output).toBeInstanceOf(File);
    expect(output.content).toEqual(bytes);
    expect(output.contentType).toBe("application/json");
  });

  it("normalizes remote request errors to application RequestError", async () => {
    const underlying = new RequestExecutionError("invalid input", "remote_app");
    const client = {
      waitOnRequestCompletion: async () => {
        throw underlying;
      },
      requestOutput: async () => {
        throw new Error("requestOutput must not run after completion wait fails");
      },
    };
    const request = new RemoteRequest("req-error", "remote_app", client as never);

    try {
      await request.output();
      throw new Error("expected request output to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(RequestError);
      expect((error as Error).cause).toBe(underlying);
    }
  });

  it("normalizes failed remote requests to application FunctionError", async () => {
    const underlying = new RequestFailedError("function_error");
    const client = {
      waitOnRequestCompletion: async () => {
        throw underlying;
      },
      requestOutput: async () => {
        throw new Error("requestOutput must not run after completion wait fails");
      },
    };
    const request = new RemoteRequest("req-failure", "remote_app", client as never);

    try {
      await request.output();
      throw new Error("expected request output to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(FunctionError);
      expect((error as Error).cause).toBe(underlying);
    }
  });

  it("normalizes application errors discovered while fetching output", async () => {
    const requestFailure = new RequestExecutionError("invalid input", "remote_app");
    const functionFailure = new RequestFailedError("function_error");
    const cases = [
      { underlying: requestFailure, expected: RequestError },
      { underlying: functionFailure, expected: FunctionError },
    ];

    for (const { underlying, expected } of cases) {
      const client = {
        waitOnRequestCompletion: async () => undefined,
        requestOutput: async () => {
          throw underlying;
        },
      };
      const request = new RemoteRequest("req-output-error", "remote_app", client as never);

      try {
        await request.output();
        throw new Error("expected request output to fail");
      } catch (error) {
        expect(error).toBeInstanceOf(expected);
        expect((error as Error).cause).toBe(underlying);
      }
    }
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

  it("generates a public endpoint ID for a new public application", async () => {
    const requested: string[] = [];
    mockFetch(async (url, init) => {
      requested.push(url);
      if (init?.method === "GET") {
        return new Response(JSON.stringify({ message: "not found" }), {
          status: 404,
        });
      }

      const form = init?.body as FormData;
      const manifest = JSON.parse(
        form.get("application") as string,
      ) as Record<string, unknown>;
      expect(manifest.public_endpoint_id).toMatch(
        /^endpoint_[A-Za-z0-9_-]{21}$/,
      );
      return new Response("", { status: 200 });
    });

    const manifest = {
      name: "public-webhook",
      allow: ["unauthenticated_requests"],
    };
    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    await client.upsertApplication(manifest, "zip-bytes");

    expect(requested).toEqual([
      "http://localhost:8900/v1/namespaces/default/applications/public-webhook",
      "http://localhost:8900/v1/namespaces/default/applications",
    ]);
    expect(manifest).not.toHaveProperty("public_endpoint_id");
    client.close();
  });

  it("reuses an existing public endpoint ID", async () => {
    const endpointId = "endpoint_0123456789abcdefghijk";
    mockFetch(async (_url, init) => {
      if (init?.method === "GET") {
        return new Response(
          JSON.stringify({ public_endpoint_id: endpointId }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }

      const form = init?.body as FormData;
      const manifest = JSON.parse(
        form.get("application") as string,
      ) as Record<string, unknown>;
      expect(manifest.public_endpoint_id).toBe(endpointId);
      return new Response("", { status: 200 });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    await client.upsertApplication(
      {
        name: "public-webhook",
        allow: ["unauthenticated_requests"],
      },
      "zip-bytes",
    );
    client.close();
  });

  it("preserves an endpoint ID already supplied by the caller", async () => {
    const endpointId = "endpoint_0123456789abcdefghijk";
    mockFetch(async (_url, init) => {
      expect(init?.method).toBe("POST");
      const form = init?.body as FormData;
      const manifest = JSON.parse(
        form.get("application") as string,
      ) as Record<string, unknown>;
      expect(manifest.public_endpoint_id).toBe(endpointId);
      expect(manifest).not.toHaveProperty("publicEndpointId");
      return new Response("", { status: 200 });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    await client.upsertApplication(
      {
        name: "public-webhook",
        allow: ["unauthenticated_requests"],
        publicEndpointId: endpointId,
      },
      "zip-bytes",
    );
    client.close();
  });

  it("deletes sandbox images through the namespaced image route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/v1/namespaces/default/sandbox-images/tensorlake%2Ftest%3A1",
      );
      expect(init?.method).toBe("DELETE");
      return new Response(null, { status: 204 });
    });

    const client = new CloudClient({ apiUrl: "http://localhost:8900" });
    await client.deleteSandboxImage("tensorlake/test:1");
    client.close();
  });

  it("finds a sandbox image by name through the platform templates route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/sandbox-templates/by-name/tensorlake%2Ftest%3A1",
      );
      expect(init?.method).toBe("GET");
      // The platform API emits snake_case keys; the client must convert them.
      return new Response(
        JSON.stringify({
          id: "tpl-1",
          name: "tensorlake/test:1",
          snapshot_id: "snap-1",
          rootfs_disk_bytes: 1024,
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const template = await client.findSandboxImageByName("tensorlake/test:1");
    expect(template).toEqual({
      id: "tpl-1",
      name: "tensorlake/test:1",
      snapshotId: "snap-1",
      rootfsDiskBytes: 1024,
    });
    client.close();
  });

  it("returns null when a sandbox image is not found", async () => {
    mockFetch(
      () => new Response(JSON.stringify({ message: "not found" }), { status: 404 }),
    );

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const template = await client.findSandboxImageByName("missing");
    expect(template).toBeNull();
    client.close();
  });

  it("lists sandbox images following pagination through the platform route", async () => {
    const base =
      "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/sandbox-templates";
    const requested: string[] = [];
    mockFetch((url) => {
      requested.push(url);
      // The platform API emits snake_case keys; the client must convert them.
      if (url === `${base}?pageSize=100`) {
        return new Response(
          JSON.stringify({
            items: [
              { id: "tpl-1", name: "image-a", snapshot_id: "snap-a", rootfs_disk_bytes: 1 },
              { id: "tpl-2", name: "image-b", snapshot_id: "snap-b", rootfs_disk_bytes: 2 },
            ],
            pagination: { next: `${base}?pageSize=100&cursor=abc` },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return new Response(
        JSON.stringify({
          items: [{ id: "tpl-3", name: "image-c", snapshot_id: "snap-c", rootfs_disk_bytes: 3 }],
          pagination: { next: null },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const images = await client.listSandboxImages();
    expect(images.map((image) => image.name)).toEqual([
      "image-a",
      "image-b",
      "image-c",
    ]);
    // Confirm snake_case keys from each page were converted to camelCase.
    expect(images.map((image) => image.snapshotId)).toEqual([
      "snap-a",
      "snap-b",
      "snap-c",
    ]);
    expect(images.map((image) => image.rootfsDiskBytes)).toEqual([1, 2, 3]);
    expect(requested).toEqual([
      `${base}?pageSize=100`,
      `${base}?pageSize=100&cursor=abc`,
    ]);
    client.close();
  });

  it("creates a file system through the platform file-systems route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/file-systems",
      );
      expect(init?.method).toBe("POST");
      const headers = init?.headers as Record<string, string>;
      expect(headers["Content-Type"]).toBe("application/json");
      expect(init?.body).toBe('{"name":"skills","description":"shared"}');
      return new Response(
        JSON.stringify({
          id: "file_system_abc",
          name: "skills",
          region: "us-east-1",
          status: "ready",
          createdAt: "2026-06-25T00:00:00Z",
          updatedAt: "2026-06-25T00:00:00Z",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const fs = await client.createFileSystem({
      name: "skills",
      description: "shared",
    });
    expect(fs.id).toBe("file_system_abc");
    expect(fs.name).toBe("skills");
    expect(fs.region).toBe("us-east-1");
    expect(fs.status).toBe("ready");
    expect(fs.createdAt).toBeInstanceOf(Date);
    client.close();
  });

  it("lists file systems following pagination through the platform route", async () => {
    const base =
      "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/file-systems";
    const requested: string[] = [];
    mockFetch((url) => {
      requested.push(url);
      if (url === `${base}?pageSize=100`) {
        return new Response(
          JSON.stringify({
            items: [
              { id: "file_system_a", name: "fs-a", status: "ready" },
              { id: "file_system_b", name: "fs-b", status: "ready" },
            ],
            pagination: { next: `${base}?pageSize=100&cursor=abc` },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        );
      }
      return new Response(
        JSON.stringify({
          items: [{ id: "file_system_c", name: "fs-c", status: "ready" }],
          pagination: { next: null },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      );
    });

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    const fileSystems = await client.listFileSystems();
    expect(fileSystems.map((fs) => fs.id)).toEqual([
      "file_system_a",
      "file_system_b",
      "file_system_c",
    ]);
    expect(requested).toEqual([
      `${base}?pageSize=100`,
      `${base}?pageSize=100&cursor=abc`,
    ]);
    client.close();
  });

  it("deletes a file system through the platform file-systems route", async () => {
    mockFetch((url, init) => {
      expect(url).toBe(
        "http://localhost:8900/platform/v1/organizations/org-1/projects/proj-1/file-systems/file_system_abc",
      );
      expect(init?.method).toBe("DELETE");
      return new Response(null, { status: 204 });
    });

    const client = new CloudClient({
      apiUrl: "http://localhost:8900",
      organizationId: "org-1",
      projectId: "proj-1",
    });
    await client.deleteFileSystem("file_system_abc");
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
