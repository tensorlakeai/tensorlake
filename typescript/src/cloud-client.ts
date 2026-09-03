import { nanoid } from "nanoid";

import * as defaults from "./defaults.js";
import { HttpClient } from "./http.js";
import { fromSnakeKeys } from "./models.js";
import { parseSSEStream } from "./sse.js";
import type {
  ApplicationBuildContext,
  ApplicationBuildResponse,
  ApplicationManifest,
  ApplicationSummary,
  BinaryPayload,
  BuildInfo,
  BuildLogEntry,
  CloudClientOptions,
  CreateApplicationBuildRequest,
  NewSecret,
  RequestInput,
  RequestMetadata,
  RequestOutput,
  SandboxTemplate,
  Secret,
  SecretsList,
  StartImageBuildRequest,
  UpsertSecretResponse,
} from "./cloud-models.js";

export class CloudClient {
  private readonly http: HttpClient;
  private readonly organizationId?: string;
  private readonly projectId?: string;
  private readonly namespace: string;

  constructor(options?: CloudClientOptions) {
    this.organizationId = options?.organizationId;
    this.projectId = options?.projectId;
    this.namespace = options?.namespace ?? defaults.NAMESPACE;
    this.http = new HttpClient({
      baseUrl: options?.apiUrl ?? defaults.API_URL,
      apiKey: options?.apiKey ?? defaults.API_KEY,
      organizationId: this.organizationId,
      projectId: this.projectId,
      maxRetries: options?.maxRetries ?? defaults.MAX_RETRIES,
      retryBackoffMs: options?.retryBackoffMs ?? defaults.RETRY_BACKOFF_MS,
    });
  }

  static forCloud(options?: CloudClientOptions): CloudClient {
    return new CloudClient(options);
  }

  close(): void {
    this.http.close();
  }

  async upsertApplication(
    manifest: ApplicationManifest,
    codeZip: BinaryPayload,
    upgradeRunningRequests = false,
  ): Promise<void> {
    const applicationManifest = { ...manifest };
    let publicEndpointId = publicEndpointIdFromManifest(applicationManifest);
    if (
      applicationAllowsUnauthenticatedRequests(applicationManifest) &&
      publicEndpointId === undefined
    ) {
      const applicationName = applicationManifest.name;
      if (typeof applicationName === "string" && applicationName.length > 0) {
        publicEndpointId =
          await this.existingApplicationPublicEndpointId(applicationName);
      }
      publicEndpointId ??= generatePublicEndpointId();
    }
    if (publicEndpointId !== undefined) {
      applicationManifest.public_endpoint_id = publicEndpointId;
      delete applicationManifest.publicEndpointId;
    }

    const form = new FormData();
    form.append(
      "code",
      new Blob([toBlobPart(codeZip)], { type: "application/zip" }),
      "code.zip",
    );
    form.append("code_content_type", "application/zip");
    form.append("application", JSON.stringify(applicationManifest));
    form.append(
      "upgrade_requests_to_latest_code",
      String(upgradeRunningRequests),
    );

    await this.http.requestResponse("POST", this.namespacePath("applications"), {
      body: form,
    });
  }

  async deleteApplication(applicationName: string): Promise<void> {
    await this.http.requestResponse(
      "DELETE",
      this.namespacePath(`applications/${encodeURIComponent(applicationName)}`),
    );
  }

  async deleteSandboxImage(imageName: string): Promise<void> {
    await this.http.requestResponse(
      "DELETE",
      this.namespacePath(`sandbox-images/${encodeURIComponent(imageName)}`),
    );
  }

  /**
   * Look up a registered sandbox image (template) by its registered name.
   *
   * Returns the template, or `null` when no image with that name exists.
   * API-key clients may omit organization/project IDs and use the
   * token-scoped Platform API route directly. Explicit scope remains
   * available for PAT callers.
   */
  async findSandboxImageByName(
    imageName: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<SandboxTemplate | null> {
    const base = this.platformProjectPath(
      "sandbox-templates",
      options?.organizationId,
      options?.projectId,
    );
    const response = await this.http.requestResponse(
      "GET",
      `${base}/by-name/${encodeURIComponent(imageName)}`,
      { allowedErrorStatusCodes: new Set([404]) },
    );
    if (response.status === 404) {
      return null;
    }
    const raw = await parseJsonResponse<Record<string, unknown>>(response);
    return fromSnakeKeys(raw) as SandboxTemplate;
  }

  /**
   * List all registered sandbox images (templates), following pagination to
   * the end. API-key clients may omit organization/project IDs and use the
   * token-scoped Platform API route directly.
   */
  async listSandboxImages(
    options?: { organizationId?: string; projectId?: string },
  ): Promise<SandboxTemplate[]> {
    const base = this.platformProjectPath(
      "sandbox-templates",
      options?.organizationId,
      options?.projectId,
    );
    let path: string | null = `${base}?pageSize=100`;
    const templates: SandboxTemplate[] = [];
    while (path !== null) {
      const page: SandboxTemplatesPage = await this.http.requestJson<SandboxTemplatesPage>(
        "GET",
        path,
      );
      for (const item of page.items ?? []) {
        templates.push(fromSnakeKeys(item) as SandboxTemplate);
      }
      const next = page.pagination?.next;
      path = next ? nextRequestPath(next) : null;
    }
    return templates;
  }

  async applications(): Promise<ApplicationSummary[]> {
    const raw = await this.http.requestJson<{ applications: Record<string, unknown>[] }>(
      "GET",
      this.namespacePath("applications"),
    );
    return (raw.applications ?? []).map(
      (application) => fromSnakeKeys(application) as ApplicationSummary,
    );
  }

  async applicationManifest(applicationName: string): Promise<ApplicationManifest> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.namespacePath(`applications/${encodeURIComponent(applicationName)}`),
    );
    return fromSnakeKeys(raw) as ApplicationManifest;
  }

  async runRequest(
    applicationName: string,
    inputs: RequestInput[] = [],
  ): Promise<string> {
    const path = this.namespacePath(
      `applications/${encodeURIComponent(applicationName)}`,
    );

    const response =
      inputs.length === 0
        ? await this.http.requestResponse("POST", path, {
            body: new Uint8Array(),
            headers: { Accept: "application/json" },
          })
        : inputs.length === 1 && inputs[0].name === "0"
          ? await this.http.requestResponse("POST", path, {
              body: toRequestBody(inputs[0].data),
              headers: {
                Accept: "application/json",
                "Content-Type": inputs[0].contentType,
              },
            })
          : await this.runMultipartRequest(path, inputs);

    const body = await parseJsonResponse<{ request_id?: string }>(response);
    const requestId = body?.request_id;
    if (!requestId) {
      throw new Error("missing request_id in run request response body");
    }
    return requestId;
  }

  async waitOnRequestCompletion(
    applicationName: string,
    requestId: string,
  ): Promise<void> {
    const stream = await this.http.requestStream(
      "GET",
      this.namespacePath(
        `applications/${encodeURIComponent(applicationName)}/requests/${encodeURIComponent(requestId)}/progress`,
      ),
    );

    for await (const event of parseSSEStream<Record<string, unknown>>(stream)) {
      if (Object.prototype.hasOwnProperty.call(event, "RequestFinished")) {
        return;
      }
    }

    throw new Error("progress stream ended before request completion");
  }

  async requestMetadata(
    applicationName: string,
    requestId: string,
  ): Promise<RequestMetadata> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      this.namespacePath(
        `applications/${encodeURIComponent(applicationName)}/requests/${encodeURIComponent(requestId)}`,
      ),
    );
    return fromSnakeKeys(raw) as RequestMetadata;
  }

  async requestOutput(
    applicationName: string,
    requestId: string,
  ): Promise<RequestOutput> {
    const response = await this.http.requestResponse(
      "GET",
      this.namespacePath(
        `applications/${encodeURIComponent(applicationName)}/requests/${encodeURIComponent(requestId)}/output`,
      ),
    );

    const serializedValue = new Uint8Array(await response.arrayBuffer());
    const contentType = response.headers.get("content-type") ?? "";
    return {
      serializedValue,
      contentType,
    };
  }

  async listSecrets(options?: {
    organizationId?: string;
    projectId?: string;
    pageSize?: number;
  }): Promise<SecretsList> {
    const base = this.secretCollectionPath(
      options?.organizationId,
      options?.projectId,
    );
    const raw = await this.http.requestJson<SecretMetadata[]>("GET", base);
    const pageSize = Math.max(0, options?.pageSize ?? 100);
    return Object.assign(
      {
        items: raw.slice(0, pageSize).map(secretFromMetadata),
        pagination: { total: raw.length },
      },
      { traceId: raw.traceId },
    );
  }

  async getSecret(
    secretId: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<Secret> {
    const base = this.secretCollectionPath(
      options?.organizationId,
      options?.projectId,
    );
    const raw = await this.http.requestJson<SecretMetadata>(
      "GET",
      `${base}/${encodeURIComponent(secretId)}`,
    );
    return Object.assign(secretFromMetadata(raw), { traceId: raw.traceId });
  }

  async upsertSecrets(
    secrets: NewSecret | NewSecret[],
    options?: { organizationId?: string; projectId?: string },
  ): Promise<UpsertSecretResponse> {
    const base = this.secretCollectionPath(
      options?.organizationId,
      options?.projectId,
    );
    if (Array.isArray(secrets)) {
      const results: Secret[] = [];
      for (const secret of secrets) {
        results.push(await this.upsertSecret(base, secret));
      }
      return results;
    }
    return this.upsertSecret(base, secrets);
  }

  async deleteSecret(
    secretId: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<void> {
    const base = this.secretCollectionPath(
      options?.organizationId,
      options?.projectId,
    );
    await this.http.requestResponse(
      "DELETE",
      `${base}/${encodeURIComponent(secretId)}`,
    );
  }

  private async upsertSecret(base: string, secret: NewSecret): Promise<Secret> {
    let response = await this.http.requestResponse("POST", base, {
      json: secret,
      headers: { "Idempotency-Key": nanoid() },
      allowedErrorStatusCodes: new Set([409]),
    });
    if (response.status === 409) {
      const prefix = base.slice(0, -"/secrets".length);
      const existing = await this.http.requestJson<{ id: string }>(
        "GET",
        `${prefix}/secret-names/${encodeURIComponent(secret.name)}`,
      );
      response = await this.http.requestResponse(
        "POST",
        `${base}/${encodeURIComponent(existing.id)}/versions`,
        {
          json: { value: secret.value },
          headers: { "Idempotency-Key": nanoid() },
        },
      );
    }
    const metadata = await parseJsonResponse<SecretMetadata>(response);
    return Object.assign(secretFromMetadata(metadata), {
      traceId: response.traceId,
    });
  }

  async startImageBuild(
    buildServicePath: string,
    request: StartImageBuildRequest,
  ): Promise<BuildInfo> {
    const form = new FormData();
    form.append("graph_name", request.applicationName);
    form.append("graph_version", request.applicationVersion);
    form.append("graph_function_name", request.functionName);
    form.append("image_name", request.imageName);
    form.append("image_id", request.imageId);
    form.append(
      "context",
      new Blob([toBlobPart(request.buildContext)]),
      "context.tar.gz",
    );

    const response = await this.http.requestResponse(
      "PUT",
      `${trimTrailingSlashes(buildServicePath)}/builds`,
      { body: form },
    );
    const raw = await parseJsonResponse<Record<string, unknown>>(response);
    return fromSnakeKeys(raw) as BuildInfo;
  }

  async createApplicationBuild(
    buildServicePath: string,
    request: CreateApplicationBuildRequest,
    imageContexts: ApplicationBuildContext[],
  ): Promise<ApplicationBuildResponse> {
    const form = createApplicationBuildForm(request, imageContexts);
    const response = await this.http.requestResponse(
      "POST",
      trimTrailingSlashes(buildServicePath),
      { body: form },
    );
    const raw = await parseJsonResponse<Record<string, unknown>>(response);
    return fromSnakeKeys(raw) as ApplicationBuildResponse;
  }

  async applicationBuildInfo(
    buildServicePath: string,
    applicationBuildId: string,
  ): Promise<ApplicationBuildResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `${trimTrailingSlashes(buildServicePath)}/${encodeURIComponent(applicationBuildId)}`,
    );
    return fromSnakeKeys(raw) as ApplicationBuildResponse;
  }

  async cancelApplicationBuild(
    buildServicePath: string,
    applicationBuildId: string,
  ): Promise<ApplicationBuildResponse> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      `${trimTrailingSlashes(buildServicePath)}/${encodeURIComponent(applicationBuildId)}/cancel`,
    );
    return fromSnakeKeys(raw) as ApplicationBuildResponse;
  }

  async buildInfo(
    buildServicePath: string,
    buildId: string,
  ): Promise<BuildInfo> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `${trimTrailingSlashes(buildServicePath)}/builds/${encodeURIComponent(buildId)}`,
    );
    return fromSnakeKeys(raw) as BuildInfo;
  }

  async cancelBuild(buildServicePath: string, buildId: string): Promise<void> {
    await this.http.requestResponse(
      "POST",
      `${trimTrailingSlashes(buildServicePath)}/builds/${encodeURIComponent(buildId)}/cancel`,
    );
  }

  async *streamBuildLogs(
    buildServicePath: string,
    buildId: string,
    signal?: AbortSignal,
  ): AsyncIterable<BuildLogEntry> {
    const stream = await this.http.requestStream(
      "GET",
      `${trimTrailingSlashes(buildServicePath)}/builds/${encodeURIComponent(buildId)}/logs`,
      { signal },
    );
    for await (const event of parseSSEStream<Record<string, unknown>>(stream, signal)) {
      yield fromSnakeKeys(event) as BuildLogEntry;
    }
  }

  private async runMultipartRequest(
    path: string,
    inputs: RequestInput[],
  ): Promise<Response> {
    const form = new FormData();
    for (const input of inputs) {
      form.append(
        input.name,
        new Blob([toBlobPart(input.data)], { type: input.contentType }),
        input.name,
      );
    }
    return this.http.requestResponse("POST", path, {
      body: form,
      headers: { Accept: "application/json" },
    });
  }

  private namespacePath(subpath: string): string {
    return `/v1/namespaces/${encodeURIComponent(this.namespace)}/${subpath.replace(/^\/+/, "")}`;
  }

  private async existingApplicationPublicEndpointId(
    applicationName: string,
  ): Promise<string | undefined> {
    const response = await this.http.requestResponse(
      "GET",
      this.namespacePath(`applications/${encodeURIComponent(applicationName)}`),
      { allowedErrorStatusCodes: new Set([404]) },
    );
    if (response.status === 404) {
      return undefined;
    }

    const application =
      await parseJsonResponse<Record<string, unknown>>(response);
    return publicEndpointIdFromManifest(application);
  }

  private platformProjectPath(
    resource: "sandbox-templates",
    organizationId?: string,
    projectId?: string,
  ): string {
    const resolvedOrganizationId = organizationId ?? this.organizationId;
    const resolvedProjectId = projectId ?? this.projectId;
    if (!resolvedOrganizationId && !resolvedProjectId) {
      return `/platform/v1/${resource}`;
    }
    if (!resolvedOrganizationId || !resolvedProjectId) {
      throw new Error(
        "organizationId and projectId must be provided together",
      );
    }
    return `/platform/v1/organizations/${encodeURIComponent(resolvedOrganizationId)}/projects/${encodeURIComponent(resolvedProjectId)}/${resource}`;
  }

  private secretCollectionPath(
    organizationId?: string,
    projectId?: string,
  ): string {
    const resolvedOrganizationId = organizationId ?? this.organizationId;
    const resolvedProjectId = projectId ?? this.projectId;
    if (!resolvedOrganizationId && !resolvedProjectId) {
      return this.namespacePath("secrets");
    }
    if (!resolvedOrganizationId || !resolvedProjectId) {
      throw new Error("organizationId and projectId must be provided together");
    }
    return this.namespacePath("secrets");
  }
}

type SecretMetadata = {
  id: string;
  name: string;
  created_at_ms: number;
};

function secretFromMetadata(metadata: SecretMetadata): Secret {
  return {
    id: metadata.id,
    name: metadata.name,
    createdAt: new Date(metadata.created_at_ms),
  };
}

const UNAUTHENTICATED_REQUESTS = "unauthenticated_requests";

function applicationAllowsUnauthenticatedRequests(
  manifest: ApplicationManifest,
): boolean {
  return (
    Array.isArray(manifest.allow) &&
    manifest.allow.includes(UNAUTHENTICATED_REQUESTS)
  );
}

function publicEndpointIdFromManifest(
  manifest: ApplicationManifest,
): string | undefined {
  for (const key of ["public_endpoint_id", "publicEndpointId"]) {
    const value = manifest[key];
    if (typeof value === "string" && value.length > 0) {
      return value;
    }
  }
  return undefined;
}

function generatePublicEndpointId(): string {
  return `endpoint_${nanoid()}`;
}

function createApplicationBuildForm(
  request: CreateApplicationBuildRequest,
  imageContexts: ApplicationBuildContext[],
): FormData {
  const contextsByPartName = new Map<string, ApplicationBuildContext>();
  for (const context of imageContexts) {
    if (contextsByPartName.has(context.contextTarPartName)) {
      throw new Error(
        `duplicate image context part name '${context.contextTarPartName}'`,
      );
    }
    contextsByPartName.set(context.contextTarPartName, context);
  }

  const form = new FormData();
  form.append(
    "app_version",
    new Blob([JSON.stringify(request)], { type: "application/json" }),
    "app_version",
  );

  for (const image of request.images) {
    const context = contextsByPartName.get(image.contextTarPartName);
    if (!context) {
      throw new Error(
        `missing image context for part '${image.contextTarPartName}'`,
      );
    }
    form.append(
      image.contextTarPartName,
      new Blob([toBlobPart(context.contextTarGz)]),
      `${image.contextTarPartName}.tar.gz`,
    );
  }

  for (const context of imageContexts) {
    if (!request.images.some((image) => image.contextTarPartName === context.contextTarPartName)) {
      throw new Error(
        `unexpected image context for part '${context.contextTarPartName}'`,
      );
    }
  }

  return form;
}

function trimTrailingSlashes(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : value;
}

/** One page of the paginated sandbox-templates list response. */
interface SandboxTemplatesPage {
  items?: Record<string, unknown>[];
  pagination?: { next?: string };
}

/**
 * Reduce a `pagination.next` link to a base-URL-relative request path. The
 * server may return either an absolute URL or an absolute path; the HTTP
 * client always prepends its base URL, so absolute URLs must be reduced to
 * their path+query first.
 */
function nextRequestPath(next: string): string {
  const schemeIndex = next.indexOf("://");
  if (schemeIndex !== -1) {
    const afterScheme = next.slice(schemeIndex + 3);
    const slashIndex = afterScheme.indexOf("/");
    return slashIndex === -1 ? "/" : afterScheme.slice(slashIndex);
  }
  return next.startsWith("/") ? next : `/${next}`;
}

function toBlobPart(data: BinaryPayload): string | Blob | ArrayBuffer {
  if (typeof data === "string" || data instanceof Blob) {
    return data;
  }
  if (data instanceof Uint8Array) {
    return Uint8Array.from(data).buffer;
  }
  return data;
}

function toRequestBody(data: BinaryPayload): string | Blob | ArrayBuffer {
  return toBlobPart(data);
}

async function parseJsonResponse<T>(response: Response): Promise<T> {
  const text = await response.text();
  if (!text) {
    return undefined as T;
  }
  return JSON.parse(text) as T;
}
