import { registerApplication, registerFunction } from "tensorlake/applications";

const greeting = registerFunction(
  "greeting",
  async (name: string) => `Hello, ${name}!`,
);

export const helloWorld = registerApplication(
  "hello_world",
  async (names: string[]) => greeting.map(names),
);
