#!/usr/bin/env node

const { runImportSandboxImageCli } = require("../dist/sandbox-image.cjs");

runImportSandboxImageCli().catch((error) => {
  const message = error && error.stack ? error.stack : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
