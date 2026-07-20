import { existsSync, readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const typescriptRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const repositoryRoot = path.resolve(typescriptRoot, "..");
const canonicalRoot = path.join(repositoryRoot, "proto", "tensorlake", "function_executor", "proto");

for (const name of ["function_executor.proto", "status.proto"]) {
  const source = path.join(canonicalRoot, name);
  if (!existsSync(source)) throw new Error(`Missing canonical function-executor protocol source: ${source}`);
}

const legacyRoots = [
  path.join(typescriptRoot, "proto"),
  path.join(repositoryRoot, "src", "tensorlake", "function_executor", "proto"),
];
for (const legacyRoot of legacyRoots) {
  for (const source of findProtoFiles(legacyRoot)) {
    throw new Error(
      `Language-specific protocol source ${source} is not allowed; edit the canonical source under ${canonicalRoot}`,
    );
  }
}

function findProtoFiles(directory) {
  if (!existsSync(directory)) return [];
  return readdirSync(directory, { withFileTypes: true }).flatMap((entry) => {
    const entryPath = path.join(directory, entry.name);
    if (entry.isDirectory()) return findProtoFiles(entryPath);
    return entry.isFile() && entry.name.endsWith(".proto") ? [entryPath] : [];
  });
}
