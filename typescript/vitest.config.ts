import { defineConfig } from "vitest/config";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { version } = require("./package.json") as { version: string };

export default defineConfig({
  define: {
    __SDK_VERSION__: JSON.stringify(version),
  },
});
