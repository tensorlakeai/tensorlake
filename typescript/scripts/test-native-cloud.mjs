import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const result = spawnSync(process.execPath, [
  fileURLToPath(new URL("../node_modules/vitest/vitest.mjs", import.meta.url)),
  "run", "tests/native-cloud.test.ts",
], {
  cwd: fileURLToPath(new URL("../", import.meta.url)),
  env: { ...process.env, TENSORLAKE_TEST_NATIVE_CLOUD: "1" },
  stdio: "inherit",
});
process.exit(result.status ?? 1);
