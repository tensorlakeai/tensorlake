import { CloudClient } from "./cloud-client.js";
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
} from "./cloud-models.js";

export class APIClient {
  private readonly cloudClient: CloudClient;

  constructor(options?: CloudClientOptions) {
    this.cloudClient = new CloudClient(options);
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
