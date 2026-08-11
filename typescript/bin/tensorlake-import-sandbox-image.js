#!/usr/bin/env node

import { runImportSandboxImageCli } from "../dist/sandbox-image.js";

runImportSandboxImageCli().catch((error) => {
  const message = error instanceof Error ? error.stack : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
