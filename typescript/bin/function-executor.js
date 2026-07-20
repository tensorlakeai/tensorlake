#!/usr/bin/env node

import { main } from "../dist/function-executor/main.js";

main(process.argv.slice(2)).catch((error) => {
  process.stderr.write(`${error?.stack ?? error}\n`);
  process.exitCode = 1;
});
