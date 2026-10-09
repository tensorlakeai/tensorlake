import { execFileSync } from "node:child_process";
import { mkdir, readFile, writeFile, symlink, unlink, rm } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { checkReleaseAge, runNpm } from "./check-npm-release-age.mjs";

const args = process.argv.slice(2);
const operation = args.shift();
const allowedVersions = [];
let allowLocalArtifacts = false;
let minimumDays = 1;
let temporary = false;
while (args[0]?.startsWith("--")) {
  const flag = args.shift();
  if (flag === "--allow-version") {
    const identity = args.shift();
    if (!identity || !/@\d+\.\d+\.\d+(?:[-+][a-z0-9.-]+)?$/i.test(identity)) {
      throw new Error("Fresh-release exceptions require exact package@version values");
    }
    allowedVersions.push(identity);
  } else if (flag === "--allow-local-artifacts") {
    allowLocalArtifacts = true;
  } else if (flag === "--temporary") {
    temporary = true;
  } else if (flag === "--minimum-days") {
    minimumDays = Number(args.shift());
  } else if (flag === "--") {
    break;
  } else {
    throw new Error(`Unknown guard option: ${flag}; separate npm arguments with --`);
  }
}

function npm(command, cwd, capture = false) {
  return runNpm(command, {
    cwd,
    encoding: "utf8",
    stdio: capture ? ["ignore", "pipe", "pipe"] : "inherit",
    env: { ...process.env, npm_config_ignore_scripts: "true" },
  });
}

async function install() {
  if (!["ci", "install", "global"].includes(operation)) {
    throw new Error("Usage: guarded-npm.mjs ci|install|global [guard options] -- [npm arguments]");
  }
  let directory = process.cwd();
  let requestedPrefix;
  for (let index = 0; index < args.length; index++) {
    if (args[index] === "--prefix") {
      if (!args[index + 1]) throw new Error("--prefix requires a directory");
      directory = resolve(args[index + 1]);
      requestedPrefix = directory;
      args.splice(index, 2);
      index--;
    } else if (args[index].startsWith("--prefix=")) {
      directory = resolve(args[index].slice("--prefix=".length));
      requestedPrefix = directory;
      args.splice(index, 1);
      index--;
    }
  }
  if (args.some((arg) => ["--", "-g", "--global", "--ignore-scripts=false"].includes(arg))) {
    throw new Error("Guarded npm arguments cannot override the installation boundary or hook policy");
  }
  let globalPrefix;
  if (operation === "global") {
    globalPrefix = requestedPrefix ?? npm(["prefix", "--global"], directory, true).trim();
    // Keep npm itself outside the tools lockfile: npm ci removes extraneous modules.
    directory = join(globalPrefix, "lib", "npm-security-tools");
    await mkdir(directory, { recursive: true });
    try {
      await readFile(join(directory, "package.json"));
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      await writeFile(join(directory, "package.json"), JSON.stringify({
        name: "guarded-global-tools", private: true,
      }) + "\n");
    }
  }
  if (temporary && operation !== "install") {
    throw new Error("--temporary is only supported for project installs");
  }
  const originalMetadata = new Map();
  if (temporary) {
    for (const name of ["package.json", "package-lock.json"]) {
      const path = join(directory, name);
      originalMetadata.set(path, await readFile(path));
    }
  }
  try {
    if (operation !== "ci") {
      if (args.some((arg) => /^(--no-save|--package-lock=false|--ignore-scripts=false|-g|--global)$/.test(arg))) {
        throw new Error("Guarded installs require a saved lockfile and disabled hooks");
      }
      const resolutionAge = allowedVersions.length ? 0 : minimumDays;
      npm(["install", "--package-lock-only", "--save-exact",
        ...args, `--min-release-age=${resolutionAge}`, "--ignore-scripts"], directory);
    }
    const registry = npm(["config", "get", "registry"], directory, true).trim();
    await checkReleaseAge(join(directory, "package-lock.json"), {
      registry, minimumDays, allowedVersions, allowLocalArtifacts,
    });
    const installOptions = operation === "ci" ? args : args.filter((arg) =>
      arg === "--no-bin-links" || arg.startsWith("--omit="));
    npm(["ci", ...installOptions, "--ignore-scripts"], directory);
    if (operation === "global") {
      const moduleRoot = npm(["root", "--global", "--prefix", globalPrefix], directory, true).trim();
      const project = JSON.parse(await readFile(join(directory, "package.json"), "utf8"));
      if (project.dependencies?.["@anthropic-ai/claude-code"]) {
        if (project.dependencies["@anthropic-ai/claude-code"] !== "2.1.294") {
          throw new Error("Claude native setup requires review for this exact version");
        }
        // Reviewed 2.1.294 setup copies its already-installed, integrity-checked
        // optional native binary into the wrapper; it does not fetch a binary.
        execFileSync(process.execPath, [join(directory,
          "node_modules/@anthropic-ai/claude-code/install.cjs")], { stdio: "inherit" });
        execFileSync(join(directory, "node_modules", ".bin", "claude"), ["--version"], {
          stdio: "inherit",
        });
      }
      for (const name of Object.keys(project.dependencies ?? {})) {
        if (!/^(@[a-z0-9._-]+\/)?[a-z0-9._-]+$/i.test(name) || ["npm", "pnpm"].includes(name)) {
          throw new Error(`Unsupported global tool identity: ${name}`);
        }
        // Compatibility links preserve existing npm root -g / package lookups.
        const destination = join(moduleRoot, name);
        await mkdir(dirname(destination), { recursive: true });
        await rm(destination, { force: true, recursive: true });
        await symlink(join(directory, "node_modules", name), destination);
      }
      const binaries = join(directory, "node_modules", ".bin");
      const { readdir } = await import("node:fs/promises");
      const binDirectory = process.platform === "win32" ? globalPrefix : join(globalPrefix, "bin");
      await mkdir(binDirectory, { recursive: true });
      let installedBinaries;
      try { installedBinaries = await readdir(binaries); } catch (error) {
        if (error.code !== "ENOENT") throw error;
        installedBinaries = [];
      }
      for (const binary of installedBinaries) {
        const destination = join(binDirectory, binary);
        try { await unlink(destination); } catch (error) {
          if (error.code !== "ENOENT") throw error;
        }
        await symlink(resolve(binaries, binary), destination);
      }
    }
  } finally {
    for (const [path, contents] of originalMetadata) {
      await writeFile(path, contents);
    }
  }
}

install().catch((error) => {
  console.error(`Guarded npm install failed: ${error.message}`);
  process.exitCode = 1;
});
