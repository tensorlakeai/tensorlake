import { CloudClient } from "./cloud-client.js";
import type { SharedFileSystem } from "./cloud-models.js";
import { buildContextFromEnv } from "./sandbox-image.js";

/**
 * Project-scoped shared-file-system registry helpers (create, list, delete).
 *
 * Uses the same environment-based Tensorlake auth as `createSandboxImage`, and
 * requires `TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID` because the
 * shared-file-system API is organization/project-scoped.
 *
 * To mount a registered shared file system into a sandbox, pass
 * `sharedFileSystems` to `Sandbox.create()` (mount at boot) or call
 * `sandbox.attachSharedFileSystem()` / `sandbox.detachSharedFileSystem()` on a
 * running sandbox.
 */

function requireCloudClient(): CloudClient {
  const context = buildContextFromEnv();
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }
  if (!context.organizationId || !context.projectId) {
    throw new Error(
      "Managing shared file systems requires organization and project context " +
        "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID).",
    );
  }
  return new CloudClient({
    apiUrl: context.apiUrl,
    apiKey: bearerToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    namespace: context.namespace,
  });
}

/** Register a new shared file system for the current project. */
export async function createSharedFileSystem(
  name: string,
  description?: string,
): Promise<SharedFileSystem> {
  if (typeof name !== "string" || name.length === 0) {
    throw new TypeError("name must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    return await client.createSharedFileSystem({ name, description });
  } finally {
    client.close();
  }
}

/** List all registered shared file systems for the current project. */
export async function listSharedFileSystems(): Promise<SharedFileSystem[]> {
  const client = requireCloudClient();
  try {
    return await client.listSharedFileSystems();
  } finally {
    client.close();
  }
}

/** Delete a registered shared file system by its id (e.g. `file_system_...`). */
export async function deleteSharedFileSystem(
  fileSystemId: string,
): Promise<void> {
  if (typeof fileSystemId !== "string" || fileSystemId.length === 0) {
    throw new TypeError("fileSystemId must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    await client.deleteSharedFileSystem(fileSystemId);
  } finally {
    client.close();
  }
}
