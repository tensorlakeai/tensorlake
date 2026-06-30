import { CloudClient } from "./cloud-client.js";
import type { FileSystem } from "./cloud-models.js";
import { buildContextFromEnv } from "./sandbox-image.js";

/**
 * Project-scoped file-system registry helpers (create, list, delete).
 *
 * Uses the same environment-based Tensorlake auth as `createSandboxImage`, and
 * requires `TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID` because the
 * file-system API is organization/project-scoped.
 *
 * To mount a registered file system into a sandbox, pass
 * `fileSystems` to `Sandbox.create()` (mount at boot) or call
 * `sandbox.attachFileSystem()` / `sandbox.detachFileSystem()` on a
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
      "Managing file systems requires organization and project context " +
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

/** Register a new file system for the current project. */
export async function createFileSystem(
  name: string,
  description?: string,
): Promise<FileSystem> {
  if (typeof name !== "string" || name.length === 0) {
    throw new TypeError("name must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    return await client.createFileSystem({ name, description });
  } finally {
    client.close();
  }
}

/** List all registered file systems for the current project. */
export async function listFileSystems(): Promise<FileSystem[]> {
  const client = requireCloudClient();
  try {
    return await client.listFileSystems();
  } finally {
    client.close();
  }
}

/** Delete a registered file system by its id (e.g. `file_system_...`). */
export async function deleteFileSystem(
  fileSystemId: string,
): Promise<void> {
  if (typeof fileSystemId !== "string" || fileSystemId.length === 0) {
    throw new TypeError("fileSystemId must be a non-empty string");
  }
  const client = requireCloudClient();
  try {
    await client.deleteFileSystem(fileSystemId);
  } finally {
    client.close();
  }
}
