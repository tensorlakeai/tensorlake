#!/usr/bin/env node

const { runPythonModule } = require("../lib/runtime.cjs");

runPythonModule(
  "tensorlake.cli.create_sandbox_image",
  "Python 3 with the 'tensorlake' package installed is required for tensorlake-create-sandbox-image.",
);
