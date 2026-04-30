import * as defaults from "./defaults.js";

/** Remove a trailing slash from a URL string if present. */
function trimTrailingSlashes(url: string): string {
  return url.endsWith("/") ? url.slice(0, -1) : url;
}

/** Check whether a URL points to localhost. */
export function isLocalhost(apiUrl: string): boolean {
  try {
    const parsed = new URL(apiUrl);
    return parsed.hostname === "localhost" || parsed.hostname === "127.0.0.1";
  } catch {
    return false;
  }
}

/**
 * Derive the sandbox proxy URL from the API URL.
 *
 * Priority:
 * 1. TENSORLAKE_SANDBOX_PROXY_URL env var
 * 2. `http://localhost:9443` for localhost API URLs
 * 3. Transform `api.X` → `sandbox.X`
 * 4. Default fallback
 */
export function resolveProxyUrl(apiUrl: string): string {
  const explicit = process.env.TENSORLAKE_SANDBOX_PROXY_URL;
  if (explicit) return explicit;

  if (isLocalhost(apiUrl)) return "http://localhost:9443";

  try {
    const parsed = new URL(apiUrl);
    const host = parsed.hostname;
    if (host.startsWith("api.")) {
      const proxyHost = "sandbox." + host.slice(4);
      return `${parsed.protocol}//${proxyHost}`;
    }
  } catch {
    // fall through to default
  }

  return defaults.SANDBOX_PROXY_URL;
}

/**
 * Resolve the proxy target for a specific sandbox.
 *
 * - Localhost: base URL stays the same, Host header set to `{sandboxId}.local`
 * - Cloud: apex proxy domain with `X-Tensorlake-Sandbox-Id` header
 *
 * Returns `{ baseUrl, hostHeader, sandboxIdHeader }`.
 */
export function resolveProxyTarget(
  proxyUrl: string,
  sandboxId: string,
): { baseUrl: string; hostHeader: string | undefined; sandboxIdHeader: string | undefined } {
  try {
    const parsed = new URL(proxyUrl);
    const host = parsed.hostname;

    if (host === "localhost" || host === "127.0.0.1") {
      return {
        baseUrl: trimTrailingSlashes(proxyUrl),
        hostHeader: `${sandboxId}.local`,
        sandboxIdHeader: undefined,
      };
    }

    const port = parsed.port ? `:${parsed.port}` : "";
    return {
      baseUrl: `${parsed.protocol}//${host}${port}`,
      hostHeader: undefined,
      sandboxIdHeader: sandboxId,
    };
  } catch {
    return {
      baseUrl: `${trimTrailingSlashes(proxyUrl)}/${sandboxId}`,
      hostHeader: undefined,
      sandboxIdHeader: undefined,
    };
  }
}

/**
 * Derive the sandbox lifecycle URL from the API URL.
 *
 * Transforms `api.X` → `sandbox.X` for cloud. Returns the URL unchanged for
 * localhost or unrecognised patterns.
 */
export function resolveSandboxLifecycleUrl(apiUrl: string): string {
  if (isLocalhost(apiUrl)) return apiUrl;

  try {
    const parsed = new URL(apiUrl);
    if (parsed.hostname.startsWith("api.")) {
      parsed.hostname = "sandbox." + parsed.hostname.slice(4);
      return parsed.toString().replace(/\/$/, "");
    }
  } catch {
    // fall through
  }

  return apiUrl;
}

/**
 * Build a lifecycle API path.
 *
 * - Localhost (namespaced): `/v1/namespaces/{namespace}/{path}`
 * - Cloud (flat): `/{path}`
 */
export function lifecyclePath(
  path: string,
  isLocal: boolean,
  namespace: string,
): string {
  if (isLocal) {
    return `/v1/namespaces/${namespace}/${path}`;
  }
  return `/${path}`;
}
