import { APIClient } from "../api-client.js";
import type { CloudClientOptions, RequestInput, RequestOutput } from "../cloud-models.js";
import { RequestExecutionError, RequestFailedError } from "../errors.js";
import type { RegisteredFunction } from "./function.js";
import { FunctionError, RequestError, SDKUsageError } from "./errors.js";
import { File } from "./file.js";
import { deserializeJSON, serializeValue } from "./serialization.js";

const CLOUD_CLIENT_OPTION_KEYS = new Set<keyof CloudClientOptions>([
  "apiUrl",
  "apiKey",
  "organizationId",
  "projectId",
  "namespace",
  "maxRetries",
  "retryBackoffMs",
]);

const REMOTE_OPTIONS_BRAND: unique symbol = Symbol.for(
  "tensorlake.applications.remote-options.v1",
);

/** Explicitly marks client options when one or more application arguments are omitted. */
export interface RemoteOptions {
  readonly [REMOTE_OPTIONS_BRAND]: true;
  readonly clientOptions: CloudClientOptions;
}

type RemoteInvocation<Args extends readonly unknown[]> =
  | [...Args]
  | [...Required<Args>, CloudClientOptions]
  | [...Partial<Args>, RemoteOptions];

function isCloudClientOptions(value: unknown): value is CloudClientOptions {
  if (typeof value !== "object" || value == null || Array.isArray(value)) return false;
  const entries = Object.entries(value);
  return entries.every(([key, option]) => {
    if (!CLOUD_CLIENT_OPTION_KEYS.has(key as keyof CloudClientOptions)) return false;
    if (option === undefined) return true;
    if (key === "maxRetries" || key === "retryBackoffMs") {
      return typeof option === "number";
    }
    return typeof option === "string";
  });
}

function isRemoteOptions(value: unknown): value is RemoteOptions {
  if (typeof value !== "object" || value == null) return false;
  const candidate = value as Partial<RemoteOptions>;
  return candidate[REMOTE_OPTIONS_BRAND] === true
    && isCloudClientOptions(candidate.clientOptions);
}

export function remoteOptions(options: CloudClientOptions = {}): RemoteOptions {
  if (!isCloudClientOptions(options)) {
    throw new SDKUsageError("remoteOptions requires valid CloudClientOptions");
  }
  return Object.freeze({
    [REMOTE_OPTIONS_BRAND]: true as const,
    clientOptions: Object.freeze({ ...options }),
  });
}

export class RemoteRequest<T> {
  constructor(
    readonly id: string,
    private readonly applicationName: string,
    private readonly client: APIClient,
    private readonly outputIsFile?: boolean,
  ) {}

  async output(): Promise<T> {
    let output: RequestOutput;
    try {
      await this.client.waitOnRequestCompletion(this.applicationName, this.id);
      output = await this.client.requestOutput(this.applicationName, this.id);
    } catch (error) {
      if (error instanceof RequestExecutionError) {
        throw new RequestError(error.message, { cause: error });
      }
      if (error instanceof RequestFailedError) {
        throw new FunctionError(error.message, { cause: error });
      }
      throw error;
    }
    if (this.outputIsFile === true) {
      return new File(output.serializedValue, output.contentType) as T;
    }
    if (this.outputIsFile === false || output.contentType.toLowerCase().startsWith("application/json")) {
      return deserializeJSON(output.serializedValue) as T;
    }
    return new File(output.serializedValue, output.contentType) as T;
  }
}

export function runRemote<Result = unknown>(
  application: string,
  ...args: unknown[]
): Promise<RemoteRequest<Result>>;
export async function runRemote<Args extends readonly unknown[], Result>(
  application: RegisteredFunction<Args, Result>,
  ...argsAndOptions: RemoteInvocation<Args>
): Promise<RemoteRequest<Result>>;
export async function runRemote(
  application: RegisteredFunction<readonly unknown[], unknown> | string,
  ...argsAndOptions: unknown[]
): Promise<RemoteRequest<unknown>> {
  const definition = typeof application === "string" ? undefined : application.definition;
  if (definition != null && definition.application == null) {
    throw new SDKUsageError(`${definition.name} is not a Tensorlake application`);
  }
  const applicationName = typeof application === "string" ? application : definition!.name;
  let options: CloudClientOptions | undefined;
  const args = [...argsAndOptions];
  const candidate = args.at(-1);
  if (typeof application === "string") {
    if (isRemoteOptions(candidate)) {
      options = candidate.clientOptions;
      args.pop();
    }
  } else {
    const parameters = definition!.parameters;
    const expectedArgs = parameters.length;
    if (isRemoteOptions(candidate)) {
      const suppliedApplicationArgs = args.length - 1;
      const omitted = parameters.slice(suppliedApplicationArgs);
      if (omitted.some((parameter) => parameter.required)) {
        throw new SDKUsageError(
          `${definition!.name} is missing a required application argument before remoteOptions`,
        );
      }
      options = candidate.clientOptions;
      args.pop();
    } else if (args.length === expectedArgs + 1) {
      if (candidate !== undefined && !isCloudClientOptions(candidate)) {
        throw new SDKUsageError("The trailing runRemote argument must be CloudClientOptions");
      }
      options = args.pop() as CloudClientOptions | undefined;
    }
  }
  const client = new APIClient(options);
  const inputs: RequestInput[] = args.map((value, index) => {
    const serialized = serializeValue(value);
    return { name: String(index), data: serialized.data, contentType: serialized.contentType };
  });
  const id = await client.runRequest(applicationName, inputs);
  return new RemoteRequest<unknown>(id, applicationName, client, definition?.returns._file);
}
