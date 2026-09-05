import { fileURLToPath } from "node:url";
import { build } from "tsup";

/** Build:native is also used without build:sdk by source integration tests. */
export async function buildNativeWorker(outDir = fileURLToPath(new URL("../dist", import.meta.url))) {
  await build({
    entry: { "native-worker": fileURLToPath(new URL("../src/native-worker.ts", import.meta.url)) },
    outDir,
    config: false,
    format: ["cjs"],
    target: "node18",
    shims: true,
    sourcemap: true,
    // In particular, preserve dist/native from this and other target builds.
    clean: false,
  });
}
