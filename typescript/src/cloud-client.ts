import * as defaults from "./defaults.js";
import { HttpClient } from "./http.js";
import { fromSnakeKeys } from "./models.js";
import { parseSSEStream } from "./sse.js";
import type {
  ApiKeyIntrospection,
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
    const form = new FormData();
    form.append(
      "code",
      new Blob([toBlobPart(codeZip)], { type: "application/zip" }),
      "code.zip",
    );
    form.append("code_content_type", "application/zip");
    form.append("application", JSON.stringify(manifest));
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

  async introspectApiKey(): Promise<ApiKeyIntrospection> {
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "POST",
      "/platform/v1/keys/introspect",
    );
    return fromSnakeKeys(raw) as ApiKeyIntrospection;
  }

  async listSecrets(options?: {
    organizationId?: string;
    projectId?: string;
    pageSize?: number;
  }): Promise<SecretsList> {
    const scope = this.resolveScope(options?.organizationId, options?.projectId);
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/platform/v1/organizations/${encodeURIComponent(scope.organizationId)}/projects/${encodeURIComponent(scope.projectId)}/secrets?pageSize=${options?.pageSize ?? 100}`,
    );
    return fromSnakeKeys(raw) as SecretsList;
  }

  async getSecret(
    secretId: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<Secret> {
    const scope = this.resolveScope(options?.organizationId, options?.projectId);
    const raw = await this.http.requestJson<Record<string, unknown>>(
      "GET",
      `/platform/v1/organizations/${encodeURIComponent(scope.organizationId)}/projects/${encodeURIComponent(scope.projectId)}/secrets/${encodeURIComponent(secretId)}`,
    );
    return fromSnakeKeys(raw) as Secret;
  }

  async upsertSecrets(
    secrets: NewSecret | NewSecret[],
    options?: { organizationId?: string; projectId?: string },
  ): Promise<UpsertSecretResponse> {
    const scope = this.resolveScope(options?.organizationId, options?.projectId);
    const raw = await this.http.requestJson<Record<string, unknown> | Record<string, unknown>[]>(
      "PUT",
      `/platform/v1/organizations/${encodeURIComponent(scope.organizationId)}/projects/${encodeURIComponent(scope.projectId)}/secrets`,
      { body: secrets },
    );
    if (Array.isArray(raw)) {
      return raw.map((secret) => fromSnakeKeys(secret) as Secret);
    }
    return fromSnakeKeys(raw) as Secret;
  }

  async deleteSecret(
    secretId: string,
    options?: { organizationId?: string; projectId?: string },
  ): Promise<void> {
    const scope = this.resolveScope(options?.organizationId, options?.projectId);
    await this.http.requestResponse(
      "DELETE",
      `/platform/v1/organizations/${encodeURIComponent(scope.organizationId)}/projects/${encodeURIComponent(scope.projectId)}/secrets/${encodeURIComponent(secretId)}`,
    );
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

  private resolveScope(
    organizationId?: string,
    projectId?: string,
  ): { organizationId: string; projectId: string } {
    const resolvedOrganizationId = organizationId ?? this.organizationId;
    const resolvedProjectId = projectId ?? this.projectId;
    if (!resolvedOrganizationId || !resolvedProjectId) {
      throw new Error(
        "organizationId and projectId are required for this operation",
      );
    }
    return {
      organizationId: resolvedOrganizationId,
      projectId: resolvedProjectId,
    };
  }
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
