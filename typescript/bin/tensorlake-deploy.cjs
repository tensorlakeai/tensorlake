#!/usr/bin/env node

const { runPythonModule } = require("../lib/runtime.cjs");

runPythonModule(
  "tensorlake.cli.deploy",
  "Python 3 with the 'tensorlake' package installed is required for tensorlake-deploy.",
);
