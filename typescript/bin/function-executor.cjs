#!/usr/bin/env node

const { runPythonModule } = require("../lib/runtime.cjs");

runPythonModule(
  "tensorlake.function_executor.main",
  "Python 3 with the 'tensorlake' package installed is required for function-executor.",
);
