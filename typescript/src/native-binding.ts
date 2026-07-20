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

function packageRoot(): string {
  return fileURLToPath(new URL("../", import.meta.url));
}

function linuxLibcFamily(options: TargetOptions): "gnu" | "musl" {
  if (options.libc != null) return options.libc;
  try {
    const report = options.report ?? process.report?.getReport() as RuntimeReport | undefined;
    if (report?.header?.glibcVersionRuntime) return "gnu";
  } catch {
    return "gnu";
  }
  return "musl";
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

export function loadNative<T>(): T {
  const target = nativeTargetId();
  const bindingPath = nativeBindingPath();
  if (!existsSync(bindingPath)) {
    throw new Error(
      `Missing native binding for ${target}. Run 'npm run build' in tensorlake before packaging or install a package published with support for your platform.`,
    );
  }
  return createRequire(import.meta.url)(bindingPath) as T;
}
