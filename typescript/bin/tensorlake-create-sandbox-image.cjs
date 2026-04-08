#!/usr/bin/env node

const { runCreateSandboxImageCli } = require("../dist/sandbox-image.cjs");

runCreateSandboxImageCli().catch((error) => {
  const message = error && error.stack ? error.stack : String(error);
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
