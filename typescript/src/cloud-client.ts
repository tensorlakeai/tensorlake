import * as defaults from "./defaults.js";
import {
  deleteSandboxImageWithCredentials,
  findSandboxImageByNameWithCredentials,
  listSandboxImagesWithCredentials,
  type SandboxImageCatalogCredentials,
} from "./sandbox-image.js";
import type { CloudClientOptions, SandboxTemplate } from "./cloud-models.js";

export class CloudClient {
  private readonly apiUrl: string;
  private readonly apiKey?: string;
  private readonly organizationId?: string;
  private readonly projectId?: string;

  constructor(options?: CloudClientOptions) {
    this.apiUrl = options?.apiUrl ?? defaults.API_URL;
    this.apiKey = options?.apiKey ?? defaults.API_KEY;
    this.organizationId = options?.organizationId;
    this.projectId = options?.projectId;
  }

  static forCloud(options?: CloudClientOptions): CloudClient {
    return new CloudClient(options);
  }

  /** No-op; the client holds no connections. */
  close(): void {}

  /**
   * Delete a registered CAS or legacy sandbox image by name or id through the
   * merged catalog, with this client's credentials: an `image_` id or a name
   * the CAS image registry knows deletes a CAS image, a `sandbox_template_` id
   * or a name registered only in the legacy image registry deletes a legacy
   * image.
   * A name is deleted from both the CAS and the legacy image registries.
   * Rejects when neither catalog knows the reference. The same semantics
   * and accepted differences as the module-level `deleteSandboxImage`.
   */
  async deleteSandboxImage(imageName: string): Promise<void> {
    await deleteSandboxImageWithCredentials(this.catalogCredentials(), imageName);
  }

  /**
   * Look up a registered CAS or legacy sandbox image by name or id through
   * the merged catalog, with this client's credentials. Returns the image in
   * the sandbox-template shape, or `null` when neither the CAS nor the legacy
   * image registry knows the reference. An explicit organization/project
   * scope (the client's, or the call's override) addresses that project's
   * routes directly, as before; without one the key's own scope applies.
   */
  async findSandboxImageByName(
    imageName: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<SandboxTemplate | null> {
    return findSandboxImageByNameWithCredentials(
      this.catalogCredentials(options),
      imageName,
    );
  }

  /**
   * List every registered sandbox image of the project (CAS and legacy, in
   * the sandbox-template shape, newest first) through the merged catalog,
   * with this client's credentials. An explicit organization/project scope
   * (the client's, or the call's override) addresses that project's routes
   * directly, as before; without one the key's own scope applies.
   */
  async listSandboxImages(
    options?: { organizationId?: string; projectId?: string },
  ): Promise<SandboxTemplate[]> {
    return listSandboxImagesWithCredentials(this.catalogCredentials(options));
  }

  private catalogCredentials(
    options?: { organizationId?: string; projectId?: string },
  ): SandboxImageCatalogCredentials {
    return {
      apiUrl: this.apiUrl,
      apiKey: this.apiKey,
      organizationId: options?.organizationId ?? this.organizationId,
      projectId: options?.projectId ?? this.projectId,
    };
  }
}
