#!/usr/bin/env node

import { runCreateSandboxImageCli } from "../dist/sandbox-image.js";

runCreateSandboxImageCli().catch((error) => {
  const message = error instanceof Error ? error.stack : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
