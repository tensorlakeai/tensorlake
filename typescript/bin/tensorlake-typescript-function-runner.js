#!/usr/bin/env node

import { main } from "../dist/function-agent/main.js";

main(process.argv.slice(2)).catch((error) => {
  process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`);
  process.exitCode = 1;
});
