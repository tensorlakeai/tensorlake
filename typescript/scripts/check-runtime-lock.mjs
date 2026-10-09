import { fileURLToPath } from "node:url";
import { loadRuntimeLock } from "./runtime-lock.mjs";

loadRuntimeLock(fileURLToPath(new URL("../", import.meta.url)));
console.log("Runtime npm compatibility lock matches the canonical pnpm production graph.");
