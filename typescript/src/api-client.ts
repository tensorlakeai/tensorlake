import { FilesystemClient } from "./filesystem.js";
import { deleteSandboxImageWithCredentials } from "./sandbox-image.js";
import type { CloudClientOptions, FileSystem } from "./cloud-models.js";

type APIClientOptions = Pick<
  CloudClientOptions,
  "apiUrl" | "apiKey" | "organizationId" | "projectId"
>;

export class APIClient {
  private readonly filesystemOptions: APIClientOptions;

  constructor(options?: APIClientOptions) {
    this.filesystemOptions = {
      apiKey: options?.apiKey,
      apiUrl: options?.apiUrl,
      organizationId: options?.organizationId,
      projectId: options?.projectId,
    };
  }

  /** No-op; the client holds no connections. */
  close(): void {}

  async deleteSandboxImage(imageName: string): Promise<void> {
    await deleteSandboxImageWithCredentials(this.filesystemOptions, imageName);
  }

  async createFileSystem(
    name: string,
    description?: string,
    options?: { organizationId?: string; projectId?: string; region?: string },
  ): Promise<FileSystem> {
    const filesystem = await new FilesystemClient(this.filesystemOptions).create(
      name, { region: options?.region },
    );
    return {
      id: filesystem.name,
      name: filesystem.name,
      description,
      status: "ready",
    };
  }

  async listFileSystems(_options?: {
    organizationId?: string;
    projectId?: string;
  }): Promise<FileSystem[]> {
    const filesystems = await new FilesystemClient(this.filesystemOptions).list();
    return filesystems.map((filesystem) => ({
      id: filesystem.name,
      name: filesystem.name,
      status: filesystem.status,
    }));
  }

  async deleteFileSystem(
    fileSystemId: string,
    _options?: { organizationId?: string; projectId?: string },
  ): Promise<void> {
    await new FilesystemClient(this.filesystemOptions).delete(fileSystemId);
  }
}
