import { describe, expect, it, afterEach, vi } from "vitest";
import { isLocalhost, resolveProxyUrl, resolveProxyTarget, lifecyclePath } from "../src/url.js";

describe("isLocalhost", () => {
  it("returns true for localhost", () => {
    expect(isLocalhost("http://localhost:8900")).toBe(true);
  });

  it("returns true for 127.0.0.1", () => {
    expect(isLocalhost("http://127.0.0.1:8900")).toBe(true);
  });

  it("returns false for cloud URL", () => {
    expect(isLocalhost("https://api.tensorlake.ai")).toBe(false);
  });

  it("returns false for invalid URL", () => {
    expect(isLocalhost("not a url")).toBe(false);
  });
});

describe("resolveProxyUrl", () => {
  afterEach(() => {
    delete process.env.TENSORLAKE_SANDBOX_PROXY_URL;
  });

  it("returns env var when set", () => {
    process.env.TENSORLAKE_SANDBOX_PROXY_URL = "http://custom:1234";
    expect(resolveProxyUrl("https://api.tensorlake.ai")).toBe("http://custom:1234");
  });

  it("returns localhost proxy for localhost API", () => {
    expect(resolveProxyUrl("http://localhost:8900")).toBe("http://localhost:9443");
  });

  it("transforms api.X to sandbox.X", () => {
    expect(resolveProxyUrl("https://api.tensorlake.ai")).toBe(
      "https://sandbox.tensorlake.ai",
    );
  });

  it("transforms api.tensorlake.dev", () => {
    expect(resolveProxyUrl("https://api.tensorlake.dev")).toBe(
      "https://sandbox.tensorlake.dev",
    );
  });

  it("falls back to default for non-api host", () => {
    const result = resolveProxyUrl("https://custom.example.com");
    expect(result).toBe("https://sandbox.tensorlake.ai");
  });
});

describe("resolveProxyTarget", () => {
  it("uses Host header for localhost", () => {
    const result = resolveProxyTarget("http://localhost:9443", "sbx-123");
    expect(result.baseUrl).toBe("http://localhost:9443");
    expect(result.hostHeader).toBe("sbx-123.local");
  });

  it("uses Host header for 127.0.0.1", () => {
    const result = resolveProxyTarget("http://127.0.0.1:9443", "sbx-123");
    expect(result.baseUrl).toBe("http://127.0.0.1:9443");
    expect(result.hostHeader).toBe("sbx-123.local");
  });

  it("uses subdomain for cloud", () => {
    const result = resolveProxyTarget(
      "https://sandbox.tensorlake.ai",
      "sbx-123",
    );
    expect(result.baseUrl).toBe("https://sbx-123.sandbox.tensorlake.ai");
    expect(result.hostHeader).toBeUndefined();
  });

  it("preserves port in cloud subdomain", () => {
    const result = resolveProxyTarget(
      "https://sandbox.example.com:8443",
      "sbx-abc",
    );
    expect(result.baseUrl).toBe("https://sbx-abc.sandbox.example.com:8443");
    expect(result.hostHeader).toBeUndefined();
  });

  it("strips trailing slash from localhost", () => {
    const result = resolveProxyTarget("http://localhost:9443/", "sbx-123");
    expect(result.baseUrl).toBe("http://localhost:9443");
  });
});

describe("lifecyclePath", () => {
  it("returns namespaced path for local", () => {
    expect(lifecyclePath("sandboxes", true, "default")).toBe(
      "/v1/namespaces/default/sandboxes",
    );
  });

  it("returns flat path for cloud", () => {
    expect(lifecyclePath("sandboxes", false, "default")).toBe("/sandboxes");
  });

  it("handles custom namespace", () => {
    expect(lifecyclePath("sandbox-pools", true, "my-ns")).toBe(
      "/v1/namespaces/my-ns/sandbox-pools",
    );
  });
});
