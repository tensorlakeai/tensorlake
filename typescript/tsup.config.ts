import { defineConfig } from "tsup";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { version } = require("./package.json");

export default defineConfig({
  entry: [
    "src/index.ts",
    "src/sandbox-image.ts",
    "src/applications/index.ts",
    "src/function-executor/main.ts",
  ],
  format: ["esm"],
  dts: true,
  clean: true,
  target: "node24",
  splitting: false,
  sourcemap: true,
  define: {
    __SDK_VERSION__: JSON.stringify(version),
  },
});
