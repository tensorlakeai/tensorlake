import { CloudClient } from "./cloud-client.js";
import type { Filesystem } from "./cloud-models.js";
import { buildContextFromEnv } from "./sandbox-image.js";

/**
 * Project-scoped filesystem registry helpers (create, list, delete).
 *
 * Uses the same environment-based Tensorlake auth as `createSandboxImage`, and
 * requires `TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID` because the
 * filesystem API is organization/project-scoped.
 *
 * To mount a registered filesystem into a sandbox, pass
 * `filesystems` to `Sandbox.create()` (mount at boot) or call
 * `sandbox.attachFilesystem()` / `sandbox.detachFilesystem()` on a
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
      "Managing filesystems requires organization and project context " +
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

/** Register a new filesystem for the current project. */
export async function createFilesystem(
  name: string,
  description?: string,
): Promise<Filesystem> {
  if (typeof name !== "string" || name.length === 0) {
    throw new TypeError("name must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    return await client.createFilesystem({ name, description });
  } finally {
    client.close();
  }
}

/** List all registered filesystems for the current project. */
export async function listFilesystems(): Promise<Filesystem[]> {
  const client = requireCloudClient();
  try {
    return await client.listFilesystems();
  } finally {
    client.close();
  }
}

/** Delete a registered filesystem by its id (e.g. `file_system_...`). */
export async function deleteFilesystem(
  fileSystemId: string,
): Promise<void> {
  if (typeof fileSystemId !== "string" || fileSystemId.length === 0) {
    throw new TypeError("fileSystemId must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    await client.deleteFilesystem(fileSystemId);
  } finally {
    client.close();
  }
}
