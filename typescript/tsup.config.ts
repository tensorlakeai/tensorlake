import { defineConfig } from "tsup";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const { version } = require("./package.json");

export default defineConfig({
  entry: ["src/index.ts", "src/sandbox-image.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  target: "node18",
  splitting: false,
  sourcemap: true,
  define: {
    __SDK_VERSION__: JSON.stringify(version),
  },
});
