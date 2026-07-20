import { registerApplication, registerFunction } from "tensorlake/applications";

const greeting = registerFunction(
  "greeting",
  async (name: string): Promise<string> => `Hello, ${name}!`,
  {
    description: "Builds one greeting in an isolated function sandbox",
  },
);

export const helloWorld = registerApplication(
  "hello_world",
  async (names: string[]): Promise<string[]> => greeting.map(names),
  {
    description: "Greets names in parallel",
    tags: { example: "typescript" },
  },
);
