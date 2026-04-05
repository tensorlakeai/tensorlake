export type BinaryPayload = Uint8Array | ArrayBuffer | Blob | string;

export interface CloudClientOptions {
  apiUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
  namespace?: string;
  maxRetries?: number;
  retryBackoffMs?: number;
}

export interface RequestInput {
  name: string;
  data: BinaryPayload;
  contentType: string;
}

export interface ApplicationSummary {
  createdAt?: Date;
  description: string;
  entrypoint?: Record<string, unknown>;
  functions?: Record<string, unknown>;
  name: string;
  namespace?: string;
  state?: unknown;
  tags: Record<string, string>;
  tombstoned?: boolean;
  version: string;
}

export type ApplicationManifest = Record<string, unknown>;

export interface RequestErrorInfo {
  functionName: string;
  message: string;
}

export interface RequestMetadata {
  id: string;
  outcome?: Record<string, unknown> | string;
  applicationVersion: string;
  createdAt: Date;
  requestError?: RequestErrorInfo;
  functionRuns?: Record<string, unknown>[];
  progressUpdates?: Record<string, unknown>[];
  updatesPaginationToken?: string;
}

export interface RequestOutput {
  serializedValue: Uint8Array;
  contentType: string;
}

export type ApiKeyIntrospection = Record<string, unknown>;

export interface NewSecret {
  name: string;
  value: string;
}

export interface Secret {
  id: string;
  name: string;
  createdAt?: Date;
}

export interface SecretsPagination {
  next?: string;
  prev?: string;
  total: number;
}

export interface SecretsList {
  items: Secret[];
  pagination: SecretsPagination;
}

export type UpsertSecretResponse = Secret | Secret[];

export interface BuildInfo {
  id: string;
  status: string;
  createdAt?: Date;
  updatedAt?: Date;
  finishedAt?: Date;
  errorMessage?: string;
  imageHash?: string;
  imageName?: string;
}

export interface BuildLogEntry {
  buildId: string;
  timestamp?: Date;
  stream: string;
  message: string;
  sequenceNumber: number;
  buildStatus: string;
}

export interface StartImageBuildRequest {
  applicationName: string;
  applicationVersion: string;
  functionName: string;
  imageName: string;
  imageId: string;
  buildContext: BinaryPayload;
}

export interface CreateApplicationBuildImageRequest {
  key: string;
  name?: string;
  description?: string;
  contextTarPartName: string;
  contextSha256: string;
  functionNames: string[];
}

export interface CreateApplicationBuildRequest {
  name: string;
  version: string;
  images: CreateApplicationBuildImageRequest[];
}

export interface ApplicationBuildContext {
  contextTarPartName: string;
  contextTarGz: BinaryPayload;
}

export interface ApplicationBuildImageResult {
  id: string;
  appVersionId?: string;
  key?: string;
  name?: string;
  description?: string;
  contextSha256?: string;
  status: string;
  errorMessage?: string;
  imageUri?: string;
  imageDigest?: string;
  createdAt?: Date;
  updatedAt?: Date;
  finishedAt?: Date;
  functionNames?: string[];
}

export interface ApplicationBuildResponse {
  id: string;
  organizationId: string;
  projectId: string;
  name: string;
  version: string;
  status?: string;
  createdAt?: Date;
  updatedAt?: Date;
  finishedAt?: Date;
  imageBuilds: ApplicationBuildImageResult[];
}
