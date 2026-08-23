import type { FileSystem } from "./cloud-models.js";
import { FilesystemClient } from "./filesystem.js";

/**
 * Compatibility helpers for Artifact Storage filesystems.
 *
 * New code should use {@link FilesystemClient} directly. These helpers retain
 * the original create/list/delete API and authenticate with
 * `TENSORLAKE_API_KEY`; ingress selects the project scope.
 *
 * To mount a registered file system into a sandbox, pass
 * `fileSystems` to `Sandbox.create()` (including warm-pool claims) or call
 * `sandbox.attachFileSystem()` / `sandbox.detachFileSystem()` on a
 * running sandbox.
 */

/** Register a new file system for the current project. */
export async function createFileSystem(
  name: string,
  description?: string,
): Promise<FileSystem> {
  if (typeof name !== "string" || name.length === 0) {
    throw new TypeError("name must be a non-empty string");
  }
  const filesystem = await new FilesystemClient().create(name);
  return {
    id: filesystem.name,
    name: filesystem.name,
    description,
    status: "ready",
  };
}

/** List all registered file systems for the current project. */
export async function listFileSystems(): Promise<FileSystem[]> {
  const filesystems = await new FilesystemClient().list();
  return filesystems.map((filesystem) => ({
    id: filesystem.name,
    name: filesystem.name,
    status: filesystem.status,
  }));
}

/** Delete a registered file system by its id (e.g. `file_system_...`). */
export async function deleteFileSystem(
  fileSystemId: string,
): Promise<void> {
  if (typeof fileSystemId !== "string" || fileSystemId.length === 0) {
    throw new TypeError("fileSystemId must be a non-empty string");
  }
  await new FilesystemClient().delete(fileSystemId);
}
