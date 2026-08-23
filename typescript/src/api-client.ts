import { CloudClient } from "./cloud-client.js";
import { FilesystemClient } from "./filesystem.js";
import {
  RequestExecutionError,
  RequestFailedError,
  RequestNotFinishedError,
} from "./errors.js";
import type {
  ApplicationManifest,
  ApplicationSummary,
  CloudClientOptions,
  RequestInput,
  RequestOutput,
  FileSystem,
} from "./cloud-models.js";

export class APIClient {
  private readonly cloudClient: CloudClient;
  private readonly filesystemOptions: {
    apiKey?: string;
    apiUrl?: string;
    organizationId?: string;
    projectId?: string;
  };

  constructor(options?: CloudClientOptions) {
    this.cloudClient = new CloudClient(options);
    this.filesystemOptions = {
      apiKey: options?.apiKey,
      apiUrl: options?.apiUrl,
      organizationId: options?.organizationId,
      projectId: options?.projectId,
    };
  }

  close(): void {
    this.cloudClient.close();
  }

  async upsertApplication(
    manifest: ApplicationManifest,
    codeZip: Uint8Array | ArrayBuffer | Blob | string,
    upgradeRunningRequests = false,
  ): Promise<void> {
    await this.cloudClient.upsertApplication(
      manifest,
      codeZip,
      upgradeRunningRequests,
    );
  }

  async deleteApplication(applicationName: string): Promise<void> {
    await this.cloudClient.deleteApplication(applicationName);
  }

  async deleteSandboxImage(imageName: string): Promise<void> {
    await this.cloudClient.deleteSandboxImage(imageName);
  }

  async createFileSystem(
    name: string,
    description?: string,
    _options?: { organizationId?: string; projectId?: string },
  ): Promise<FileSystem> {
    const filesystem = await new FilesystemClient(this.filesystemOptions).create(name);
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

  async applications(): Promise<ApplicationSummary[]> {
    return this.cloudClient.applications();
  }

  async application(applicationName: string): Promise<ApplicationManifest> {
    return this.cloudClient.applicationManifest(applicationName);
  }

  async runRequest(
    applicationName: string,
    inputs: RequestInput[],
  ): Promise<string> {
    return this.cloudClient.runRequest(applicationName, inputs);
  }

  async waitOnRequestCompletion(
    applicationName: string,
    requestId: string,
  ): Promise<void> {
    await this.cloudClient.waitOnRequestCompletion(applicationName, requestId);
  }

  async requestOutput(
    applicationName: string,
    requestId: string,
  ): Promise<RequestOutput> {
    const metadata = await this.cloudClient.requestMetadata(
      applicationName,
      requestId,
    );

    if (metadata.outcome == null) {
      throw new RequestNotFinishedError();
    }

    if (typeof metadata.outcome === "object") {
      if (metadata.requestError?.message) {
        throw new RequestExecutionError(
          metadata.requestError.message,
          metadata.requestError.functionName,
        );
      }
      const failure =
        typeof metadata.outcome.failure === "string"
          ? metadata.outcome.failure
          : JSON.stringify(metadata.outcome);
      throw new RequestFailedError(failure);
    }

    return this.cloudClient.requestOutput(applicationName, requestId);
  }
}
