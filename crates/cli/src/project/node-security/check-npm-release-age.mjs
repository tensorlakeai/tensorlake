import { execFileSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { delimiter, join } from "node:path";
import { pathToFileURL } from "node:url";

const DAY_MS = 24 * 60 * 60 * 1000;

export function runNpm(args, options) {
  if (process.platform === "win32") {
    // Run the JS entrypoint directly: .cmd files otherwise require a shell,
    // which would turn registry arguments into shell code.
    for (const directory of (process.env.PATH ?? process.env.Path ?? "").split(delimiter)) {
      const entrypoint = join(directory, "node_modules", "npm", "bin", "npm-cli.js");
      if (existsSync(entrypoint)) {
        return execFileSync(process.execPath, [entrypoint, ...args], options);
      }
    }
    throw new Error("Cannot locate the npm JavaScript entrypoint on Windows");
  }
  return execFileSync("npm", args, options);
}

function packageName(location, entry) {
  const name = entry.name ?? location.split("node_modules/").at(-1);
  if (!/^(@[a-z0-9._-]+\/)?[a-z0-9._-]+$/i.test(name ?? "")) {
    throw new Error(`Cannot identify registry package at ${location}`);
  }
  return name;
}

export async function checkReleaseAge(lockfile, options = {}) {
  const minimumDays = options.minimumDays ?? 1;
  if (!Number.isFinite(minimumDays) || minimumDays < 1) {
    throw new Error("The default release delay must be at least one day");
  }
  const allowedVersions = new Set(options.allowedVersions ?? []);
  const registry = options.registry ?? "https://registry.npmjs.org/";
  const metadata = options.metadata ?? (async (name) => {
    const endpoint = new URL(encodeURIComponent(name), registry);
    // Retry transient transport/rate-limit failures, then fail closed.
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const response = await fetch(endpoint, { signal: AbortSignal.timeout(30_000) });
        if (response.ok) return await response.json();
        if (attempt < 2 && (response.status === 429 || response.status >= 500)) continue;
        throw new Error(`Registry metadata unavailable for ${name}: HTTP ${response.status}`);
      } catch (error) {
        const transient = ["TypeError", "TimeoutError"].includes(error.name);
        if (!transient || attempt === 2) throw error;
      }
    }
    throw new Error(`Registry metadata unavailable for ${name}`);
  });
  const lock = JSON.parse(await readFile(lockfile, "utf8"));
  if (![2, 3].includes(lock.lockfileVersion) || !lock.packages) {
    throw new Error("An npm lockfile with lockfileVersion 2 or 3 is required");
  }
  const packages = new Map();
  for (const [location, entry] of Object.entries(lock.packages)) {
    if (!location.includes("node_modules/")) continue;
    if (entry.link) continue;
    // Local artifacts require a deliberate exception in the invoking workflow.
    if (entry.resolved?.startsWith("file:")) {
      if (options.allowLocalArtifacts) continue;
      throw new Error(`Local artifact requires --allow-local-artifacts: ${location}`);
    }
    const name = packageName(location, entry);
    if (!/^\d+\.\d+\.\d+(?:[-+][a-z0-9.-]+)?$/i.test(entry.version ?? "")) {
      throw new Error(`Missing or invalid registry version for ${name}`);
    }
    const identity = `${name}@${entry.version}`;
    if (identity === "tensorlake@0.5.144") {
      throw new Error(`Known compromised version is forbidden: ${identity}`);
    }
    const resolved = new URL(entry.resolved);
    if (!['http:', 'https:'].includes(resolved.protocol)) {
      throw new Error(`Non-registry dependency is not age-verifiable: ${identity}`);
    }
    const registryHost = new URL(registry).host;
    if (resolved.host !== registryHost && resolved.host !== "registry.npmjs.org") {
      throw new Error(`Unrecognized artifact host for ${identity}`);
    }
    if (!decodeURIComponent(resolved.pathname).startsWith(`/${name}/-/`)) {
      throw new Error(`Locked artifact identity does not match ${identity}`);
    }
    if (!entry.integrity) throw new Error(`Missing locked integrity for ${identity}`);
    packages.set(identity, { name, version: entry.version, integrity: entry.integrity });
  }
  const requests = new Map();
  const entries = [...packages.values()];
  const now = options.now ?? Date.now();
  let cursor = 0;
  async function worker() {
    while (cursor < entries.length) {
      const { name, version, integrity } = entries[cursor++];
      if (!requests.has(name)) requests.set(name, metadata(name));
      const manifest = await requests.get(name);
      const dist = manifest.versions?.[version]?.dist;
      const registryIntegrities = new Set((dist?.integrity ?? "").split(/\s+/));
      if (/^[a-f0-9]{40}$/i.test(dist?.shasum ?? "")) {
        registryIntegrities.add(`sha1-${Buffer.from(dist.shasum, "hex").toString("base64")}`);
      }
      if (!integrity.split(/\s+/).some((value) => registryIntegrities.has(value))) {
        throw new Error(`Locked artifact integrity does not match registry metadata: ${name}@${version}`);
      }
      const published = manifest.time?.[version];
      const isoTimestamp = typeof published === "string" &&
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(published);
      const timestamp = isoTimestamp ? Date.parse(published) : NaN;
      if (!Number.isFinite(timestamp)) {
        throw new Error(`Missing publication timestamp: ${name}@${version}`);
      }
      const calendarDate = new Date(`${published.slice(0, 19)}Z`);
      if (calendarDate.toISOString().slice(0, 19) !== published.slice(0, 19)) {
        throw new Error(`Invalid publication timestamp: ${name}@${version}`);
      }
      if (allowedVersions.has(`${name}@${version}`)) {
        console.log(`Fresh-release test exception: ${name}@${version}`);
        continue;
      }
      if (now - timestamp < minimumDays * DAY_MS) {
        throw new Error(`Release is younger than ${minimumDays} day(s): ${name}@${version}`);
      }
    }
  }
  await Promise.all(Array.from({ length: Math.min(8, entries.length) }, worker));
  console.log(`Validated publication age of ${packages.size} locked registry versions`);
}

async function main() {
  const args = process.argv.slice(2);
  let lockfile = "package-lock.json";
  const allowedVersions = [];
  let allowLocalArtifacts = false;
  let minimumDays = 1;
  for (let index = 0; index < args.length; index++) {
    const arg = args[index];
    if (arg === "--allow-version") {
      const version = args[++index];
      if (!version || !/@\d+\.\d+\.\d+(?:[-+][a-z0-9.-]+)?$/i.test(version)) {
        throw new Error("--allow-version requires an exact package@version");
      }
      allowedVersions.push(version);
    } else if (arg === "--allow-local-artifacts") {
      allowLocalArtifacts = true;
    } else if (arg === "--minimum-days") {
      minimumDays = Number(args[++index]);
    } else if (arg.startsWith("--")) {
      throw new Error(`Unknown option: ${arg}`);
    } else {
      lockfile = arg;
    }
  }
  const registry = runNpm(["config", "get", "registry"], {
    encoding: "utf8", stdio: ["ignore", "pipe", "pipe"],
  }).trim();
  await checkReleaseAge(lockfile, {
    registry, minimumDays, allowedVersions, allowLocalArtifacts,
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(`npm publication-age check failed: ${error.message}`);
    process.exitCode = 1;
  });
}
