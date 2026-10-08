import { appendFileSync, readFileSync, writeFileSync } from "node:fs";
import path from "node:path";

// Reads `npm pack --json` output for one native package, writes the
// {name, version, integrity} record consumed by lock-native-integrity.mjs, and
// exposes the tarball path to the release workflow.
const [packJson, tarballDirectory, recordPath] = process.argv.slice(2);
if (!packJson || !tarballDirectory || !recordPath) {
  throw new Error("usage: record-native-integrity.mjs <pack-json> <tarball-directory> <record-path>");
}

const packed = JSON.parse(readFileSync(packJson, "utf8"));
if (packed.length !== 1) {
  throw new Error(`Expected one packed package, got ${packed.length}`);
}
const [{ name, version, integrity, filename }] = packed;
writeFileSync(recordPath, `${JSON.stringify({ name, version, integrity })}\n`);
if (process.env.GITHUB_OUTPUT) {
  appendFileSync(process.env.GITHUB_OUTPUT, `tarball=${path.join(tarballDirectory, filename)}\n`);
}
process.stdout.write(`${name}@${version} ${integrity}\n`);
