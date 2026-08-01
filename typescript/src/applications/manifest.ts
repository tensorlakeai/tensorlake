import { Buffer } from "node:buffer";
import type { JSONSchema, Parameter } from "./schema.js";
import { parameterJSONSchema } from "./schema.js";
import { getApplications, getFunctions } from "./registry.js";
import type {
  ApplicationCapability,
  RegisteredDefinition,
} from "./function.js";
import { SDKUsageError } from "./errors.js";

export const TYPESCRIPT_DESCRIPTOR_FORMAT = "tensorlake.typescript.json-schema.v1";

export interface ParameterManifest {
  name: string;
  data_type: JSONSchema;
  description?: string;
  required: boolean;
}

export interface FunctionManifest {
  name: string;
  description: string;
  docstring: string;
  secret_names: string[];
  initialization_timeout_sec: number;
  timeout_sec: number;
  resources: {
    cpus: number;
    memory_mb: number;
    ephemeral_disk_mb: number;
    gpus: { count: number; model: string }[];
  };
  retry_policy: {
    max_retries: number;
    initial_delay_sec: number;
    max_delay_sec: number;
    delay_multiplier: number;
  };
  cache_key: null;
  parameters: ParameterManifest[];
  return_type: JSONSchema | null;
  placement_constraints: { filter_expressions: string[] };
  max_concurrency: number;
  warm_containers?: number;
  min_containers?: number;
  max_containers?: number;
  image?: string;
}

export interface ApplicationManifest {
  [key: string]: unknown;
  name: string;
  description: string;
  tags: Record<string, string>;
  allow: ApplicationCapability[];
  public_endpoint_id?: string;
  version: string;
  functions: Record<string, FunctionManifest>;
  entrypoint: {
    function_name: string;
    input_serializer: "json";
    inputs_base64: string;
    output_serializer: "json";
    output_type_hints_base64: string;
  };
}

function encodeDescriptor(value: unknown): string {
  return Buffer.from(JSON.stringify({ format: TYPESCRIPT_DESCRIPTOR_FORMAT, value }), "utf8").toString("base64");
}

export function decodeDescriptor<T>(encoded: string): T {
  const envelope = JSON.parse(Buffer.from(encoded, "base64").toString("utf8")) as {
    format?: string;
    value?: T;
  };
  if (envelope.format !== TYPESCRIPT_DESCRIPTOR_FORMAT) {
    throw new SDKUsageError("Application manifest does not contain TypeScript schema descriptors");
  }
  return envelope.value as T;
}

function parseGPU(value: string): { count: number; model: string } {
  const [model, countText, ...extra] = value.split(":");
  if (!model || extra.length > 0) throw new SDKUsageError(`Invalid GPU setting '${value}'`);
  const count = countText == null ? 1 : Number(countText);
  if (!Number.isInteger(count) || count <= 0) throw new SDKUsageError(`Invalid GPU count in '${value}'`);
  return { count, model };
}

function parameterManifest(parameter: Parameter<unknown>): ParameterManifest {
  return {
    name: parameter.name,
    data_type: parameterJSONSchema(parameter),
    description: parameter.description,
    required: parameter.required,
  };
}

function functionManifest(
  application: RegisteredDefinition,
  definition: RegisteredDefinition,
): FunctionManifest {
  const retries = definition.options.retries ?? application.application?.retries ?? { maxRetries: 0 };
  const gpu = definition.options.gpu == null
    ? []
    : (Array.isArray(definition.options.gpu) ? definition.options.gpu : [definition.options.gpu]).map(parseGPU);
  const result: FunctionManifest = {
    name: definition.name,
    description: definition.options.description,
    docstring: "",
    secret_names: [...definition.options.secrets],
    initialization_timeout_sec: definition.options.timeout,
    timeout_sec: definition.options.timeout,
    resources: {
      cpus: definition.options.cpu,
      memory_mb: Math.ceil(definition.options.memory * 1024),
      ephemeral_disk_mb: Math.ceil(definition.options.ephemeralDisk * 1024),
      gpus: gpu,
    },
    retry_policy: {
      max_retries: retries.maxRetries,
      initial_delay_sec: 1,
      max_delay_sec: 60,
      delay_multiplier: 2,
    },
    cache_key: null,
    parameters: definition.application == null ? [] : definition.parameters.map(parameterManifest),
    return_type: definition.application == null ? null : { ...definition.returns.jsonSchema, title: "Return value" },
    placement_constraints: {
      filter_expressions: (definition.options.region ?? application.options.region) == null
        ? []
        : [`region==${definition.options.region ?? application.options.region}`],
    },
    max_concurrency: 1,
  };
  if (definition.options.warmContainers != null) result.warm_containers = definition.options.warmContainers;
  if (definition.options.minContainers != null) result.min_containers = definition.options.minContainers;
  if (definition.options.maxContainers != null) result.max_containers = definition.options.maxContainers;
  if (definition.options.image != null) result.image = definition.options.image.name;
  return result;
}

export function createApplicationManifest(application: RegisteredDefinition): ApplicationManifest {
  if (application.application == null) throw new SDKUsageError(`${application.name} is not an application`);
  const functions = Object.fromEntries(
    getFunctions().map((definition) => [definition.name, functionManifest(application, definition)]),
  );
  return {
    name: application.name,
    description: application.options.description,
    tags: { ...application.application.tags },
    allow: [...application.application.allow],
    version: application.application.version,
    functions,
    entrypoint: {
      function_name: application.name,
      input_serializer: "json",
      inputs_base64: encodeDescriptor(application.parameters.map((parameter) => ({
        name: parameter.name,
        schema: parameter.schema.jsonSchema,
        required: parameter.required,
        hasDefault: parameter.hasDefault,
        defaultValue: parameter.defaultValue,
      }))),
      output_serializer: "json",
      output_type_hints_base64: encodeDescriptor(application.returns.jsonSchema),
    },
  };
}

export function createApplicationManifests(): ApplicationManifest[] {
  return getApplications().map(createApplicationManifest);
}

export interface CodeManifest {
  format_version: 2;
  runtime: "typescript";
  minimum_node_major: 24;
  module: string;
  functions: Record<string, { name: string }>;
}

export function createCodeManifest(module = "runtime.mjs"): CodeManifest {
  return {
    format_version: 2,
    runtime: "typescript",
    minimum_node_major: 24,
    module,
    functions: Object.fromEntries(getFunctions().map((definition) => [definition.name, { name: definition.name }])),
  };
}
