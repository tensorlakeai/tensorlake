import { afterEach, describe, expect, it, vi } from "vitest";
import * as undici from "undici";
import { CloudClient } from "../src/cloud-client.js";

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

  it("creates a filesystem through the platform file-systems route", async () => {
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
    const fs = await client.createFilesystem({
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

  it("lists filesystems following pagination through the platform route", async () => {
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
    const filesystems = await client.listFilesystems();
    expect(filesystems.map((fs) => fs.id)).toEqual([
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

  it("deletes a filesystem through the platform file-systems route", async () => {
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
    await client.deleteFilesystem("file_system_abc");
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
