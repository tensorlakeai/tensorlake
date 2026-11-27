export interface CloudClientOptions {
  apiUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
}

/**
 * A registered CAS or legacy sandbox image in the sandbox-template shape.
 * Both image families answer it: legacy images verbatim from the legacy image
 * registry, and CAS images (`snapshotFormatVersion ===
 * "content_addressed_streaming_v1"`) derived by the core from the CAS image
 * registry's trimmed record (id, name, snapshot id, visibility, sizes,
 * timestamps); no content digest or build provenance rides on either.
 */
export interface SandboxTemplate {
  id?: string;
  name?: string;
  snapshotId?: string;
  snapshotSandboxId?: string;
  snapshotUri?: string;
  snapshotFormatVersion?: string;
  snapshotSizeBytes?: number;
  rootfsDiskBytes?: number;
  rootfsNodeKind?: string;
  public?: boolean;
  dockerfile?: string;
  createdByUserId?: string;
  createdAt?: string;
  updatedAt?: string;
}

/** A registered file system as returned by the platform API. */
export interface FileSystem {
  id?: string;
  name?: string;
  description?: string;
  region?: string;
  status?: string;
  createdAt?: Date;
  updatedAt?: Date;
}
