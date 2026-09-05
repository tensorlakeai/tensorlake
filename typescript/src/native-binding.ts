import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

interface RuntimeReport {
  header?: {
    glibcVersionRuntime?: string;
  };
}

interface TargetOptions {
  libc?: "gnu" | "musl";
  report?: RuntimeReport;
}

interface NativeBindingPathOptions extends TargetOptions {
  packageRoot?: string;
  platform?: NodeJS.Platform;
  arch?: string;
}

const NATIVE_PACKAGES: Readonly<Record<string, string>> = {
  "darwin-arm64": "@tensorlake/native-darwin-arm64",
  "linux-arm64": "@tensorlake/native-linux-arm64-gnu",
  "linux-arm64-musl": "@tensorlake/native-linux-arm64-musl",
  "linux-x64": "@tensorlake/native-linux-x64-gnu",
  "linux-x64-musl": "@tensorlake/native-linux-x64-musl",
  "win32-x64": "@tensorlake/native-win32-x64",
};

function packageRoot(): string {
  const parent = fileURLToPath(new URL("../", import.meta.url));
  if (existsSync(path.join(parent, "package.json"))) return parent;
  const grandparent = fileURLToPath(new URL("../../", import.meta.url));
  if (existsSync(path.join(grandparent, "package.json"))) return grandparent;
  return parent;
}

let cachedLinuxLibc: "gnu" | "musl" | undefined;

function linuxLibcFamily(options: TargetOptions): "gnu" | "musl" {
  if (options.libc != null) return options.libc;
  if (options.report != null) {
    return options.report.header?.glibcVersionRuntime ? "gnu" : "musl";
  }
  if (cachedLinuxLibc != null) return cachedLinuxLibc;

  // libc cannot change during the process lifetime. Generating a diagnostic
  // report can block on network lookups, so omit those and only probe once.
  // Keep this in sync with lib/runtime.cjs, used by the runtime launchers.
  cachedLinuxLibc = "gnu";
  // Available since Node 22, but missing from the current Node type definitions.
  const report = process.report as NodeJS.ProcessReport & { excludeNetwork: boolean };
  if (!report?.getReport) return cachedLinuxLibc;
  const excludeNetwork = report.excludeNetwork;
  try {
    report.excludeNetwork = true;
    const data = report.getReport() as RuntimeReport | undefined;
    if (data) {
      cachedLinuxLibc = data.header?.glibcVersionRuntime ? "gnu" : "musl";
    }
  } catch {
    // Preserve the GNU fallback when diagnostic reports are unavailable.
  } finally {
    report.excludeNetwork = excludeNetwork;
  }
  return cachedLinuxLibc;
}

export function nativeTargetId(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
  options: TargetOptions = {},
): string {
  const target = `${platform}-${arch}`;
  if (platform !== "linux") return target;
  if (options.libc == null && options.report == null && platform !== process.platform) {
    return target;
  }
  return linuxLibcFamily(options) === "musl" ? `${target}-musl` : target;
}

export function nativeBindingPath(options: NativeBindingPathOptions = {}): string {
  const platform = options.platform ?? process.platform;
  const arch = options.arch ?? process.arch;
  return path.join(
    options.packageRoot ?? packageRoot(),
    "dist",
    "native",
    nativeTargetId(platform, arch, options),
    "tensorlake-node.node",
  );
}

export function nativePackageName(
  platform: NodeJS.Platform = process.platform,
  arch: string = process.arch,
  options: TargetOptions = {},
): string | undefined {
  return NATIVE_PACKAGES[nativeTargetId(platform, arch, options)];
}

export function loadNative<T>(): T {
  const target = nativeTargetId();
  const bindingPath = nativeBindingPath();
  const require = createRequire(import.meta.url);

  // Local source builds stage the addon here. Published packages intentionally
  // omit this directory and resolve the platform-specific optional dependency.
  if (existsSync(bindingPath)) return require(bindingPath) as T;

  const packageName = nativePackageName();
  if (!packageName) {
    throw new Error(
      `Tensorlake does not provide a native binding for ${target}.`,
    );
  }

  let packageEntry: string;
  try {
    packageEntry = require.resolve(packageName);
  } catch (cause) {
    throw new Error(
      `Missing native binding package ${packageName} for ${target}. Reinstall tensorlake without omitting optional dependencies, and install dependencies on the machine where they will run.`,
      { cause },
    );
  }
  return require(packageEntry) as T;
}
