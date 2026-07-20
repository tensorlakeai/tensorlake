import {
  registerApplication,
  registerFunction,
  schema,
} from "tensorlake/applications";

const greeting = registerFunction(
  async (name: string): Promise<string> => `Hello, ${name}!`,
  {
    name: "greeting",
    parameters: [schema.parameter("name", schema.string())] as const,
    returns: schema.string(),
    description: "Builds one greeting in an isolated function sandbox",
  },
);

export const helloWorld = registerApplication(
  async (names: string[]): Promise<string[]> => greeting.map(names),
  {
    name: "hello_world",
    parameters: [schema.parameter("names", schema.array(schema.string()))] as const,
    returns: schema.array(schema.string()),
    description: "Greets names in parallel",
    tags: { example: "typescript" },
  },
);
