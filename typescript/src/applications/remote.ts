import { APIClient } from "../api-client.js";
import type { CloudClientOptions, RequestInput } from "../cloud-models.js";
import type { RegisteredFunction } from "./function.js";
import { SDKUsageError } from "./errors.js";
import { File } from "./file.js";
import { deserializeJSON, serializeValue } from "./serialization.js";

export class RemoteRequest<T> {
  constructor(
    readonly id: string,
    private readonly applicationName: string,
    private readonly client: APIClient,
  ) {}

  async output(): Promise<T> {
    await this.client.waitOnRequestCompletion(this.applicationName, this.id);
    const output = await this.client.requestOutput(this.applicationName, this.id);
    if (output.contentType.startsWith("application/json")) {
      return deserializeJSON(output.serializedValue) as T;
    }
    return new File(output.serializedValue, output.contentType) as T;
  }
}

export async function runRemote<Args extends readonly unknown[], Result>(
  application: RegisteredFunction<Args, Result> | string,
  ...argsAndOptions: [...Args, CloudClientOptions?]
): Promise<RemoteRequest<Result>> {
  const definition = typeof application === "string" ? undefined : application.definition;
  if (definition != null && definition.application == null) {
    throw new SDKUsageError(`${definition.name} is not a Tensorlake application`);
  }
  const applicationName = typeof application === "string" ? application : definition!.name;
  const expectedArgs = definition?.parameters.length;
  let options: CloudClientOptions | undefined;
  let args: unknown[] = [...argsAndOptions];
  if (expectedArgs != null && args.length === expectedArgs + 1) {
    options = args.pop() as CloudClientOptions;
  }
  const client = new APIClient(options);
  const inputs: RequestInput[] = args.map((value, index) => {
    const serialized = serializeValue(value);
    return { name: String(index), data: serialized.data, contentType: serialized.contentType };
  });
  const id = await client.runRequest(applicationName, inputs);
  return new RemoteRequest<Result>(id, applicationName, client);
}
