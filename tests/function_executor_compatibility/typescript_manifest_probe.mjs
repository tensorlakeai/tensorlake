import {
  Image,
  createApplicationManifest,
  registerApplication,
  registerFunction,
  schema,
} from "../../typescript/dist/applications/index.js";

registerFunction(async (value) => value, {
  name: "manifest_child",
  description: "Processes a customer payload",
  parameters: [schema.parameter("value", schema.integer())],
  returns: schema.integer(),
  cpu: 2.25,
  memory: 2,
  ephemeralDisk: 3,
  gpu: "H100:2",
  timeout: 45,
  image: new Image("compatibility-manifest-image"),
  secrets: ["MANIFEST_SECRET"],
  retries: { maxRetries: 2 },
  region: "eu-west-1",
  warmContainers: 1,
  minContainers: 2,
  maxContainers: 3,
});

const application = registerApplication(async (body) => ({
  size: body.content.byteLength,
}), {
  name: "manifest_application",
  description: "Receives a public HTTP body",
  parameters: [schema.parameter("body", schema.httpBody())],
  returns: schema.object({ size: schema.integer() }),
  timeout: 30,
  tags: { suite: "compatibility" },
  applicationRetries: { maxRetries: 1 },
  region: "us-east-1",
  allow: ["unauthenticated_requests"],
});

function functionView(functionManifest) {
  return {
    name: functionManifest.name,
    description: functionManifest.description,
    secret_names: functionManifest.secret_names,
    initialization_timeout_sec: functionManifest.initialization_timeout_sec,
    timeout_sec: functionManifest.timeout_sec,
    resources: functionManifest.resources,
    retry_policy: functionManifest.retry_policy,
    parameter_types: functionManifest.parameters.map(
      (parameter) => parameter.data_type.type,
    ),
    return_type: functionManifest.return_type?.type ?? null,
    placement_constraints:
      functionManifest.placement_constraints.filter_expressions,
    max_concurrency: functionManifest.max_concurrency,
    warm_containers: functionManifest.warm_containers ?? null,
    min_containers: functionManifest.min_containers ?? null,
    max_containers: functionManifest.max_containers ?? null,
    image: functionManifest.image ?? null,
  };
}

const manifest = createApplicationManifest(application.definition);
console.log(JSON.stringify({
  name: manifest.name,
  description: manifest.description,
  tags: manifest.tags,
  allow: manifest.allow,
  entrypoint: {
    function_name: manifest.entrypoint.function_name,
    input_serializer: manifest.entrypoint.input_serializer,
    output_serializer: manifest.entrypoint.output_serializer,
  },
  functions: Object.fromEntries(
    ["manifest_application", "manifest_child"].map(
      (name) => [name, functionView(manifest.functions[name])],
    ),
  ),
}));
