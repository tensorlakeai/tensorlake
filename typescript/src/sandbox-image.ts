import { createRequire } from "node:module";
import path from "node:path";
import { existsSync } from "node:fs";
import { parseArgs } from "node:util";
import { CloudClient } from "./cloud-client.js";
import type { SandboxTemplate } from "./cloud-models.js";
import { Image, dockerfileContent } from "./image.js";

/**
 * Sandbox-image build engine.
 *
 * Renders the source to a Dockerfile (for `Image` inputs) and hands the
 * Dockerfile path/text + context to the Rust core via `@tensorlake/native`,
 * which parses, validates, materializes, and registers the image. The Rust
 * core owns parsing and the warnings for instructions that run during the
 * build but have no effect when a sandbox runs from the image
 * (`ONBUILD`/`SHELL`/`EXPOSE`/`HEALTHCHECK`/`LABEL`/`STOPSIGNAL`/`VOLUME`).
 */

export interface CreateSandboxImageOptions {
  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  builderDiskMb?: number;
  isPublic?: boolean;
  contextDir?: string;
  /**
   * Use Docker/BuildKit max compatibility mode (build is slower and uses more
   * memory and disk space on builder sandbox).
   */
  dockerCompat?: boolean;
  /**
   * Print build progress to stderr. Ignored when an explicit `emit` is
   * passed via `deps`. Defaults to false — `createSandboxImage` is silent
   * by default when invoked programmatically (e.g. `Image.build()`).
   */
  verbose?: boolean;
}

export interface ImportSandboxImageOptions {
  /**
   * Name to register the image under. Defaults to the image reference's last
   * path segment with any tag/digest stripped (e.g. `pytorch/pytorch:2.4.1`
   * -> `pytorch`).
   */
  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  builderDiskMb?: number;
  isPublic?: boolean;
  /**
   * Use Docker/BuildKit max compatibility mode (import is slower and uses more
   * memory and disk space on builder sandbox).
   */
  dockerCompat?: boolean;
  /**
   * Print build progress to stderr. Ignored when an explicit `emit` is
   * passed via `deps`. Defaults to false.
   */
  verbose?: boolean;
}

export type SandboxImageSource = string | Image;

export interface CreateSandboxImageDeps {
  emit?: (event: Record<string, unknown>) => void;
}

// --- Native binding loader -------------------------------------------------

/** Auth/context + resource fields shared by the build and import bindings. */
interface NativeBindingCommonOptions {
  apiUrl: string;
  bearerToken: string;
  registeredName?: string | undefined | null;
  diskMb?: number | undefined | null;
  builderDiskMb?: number | undefined | null;
  cpus?: number | undefined | null;
  memoryMb?: number | undefined | null;
  isPublic?: boolean | undefined | null;
  organizationId?: string | undefined | null;
  projectId?: string | undefined | null;
  namespace?: string | undefined | null;
  useScopeHeaders?: boolean | undefined | null;
  userAgent?: string | undefined | null;
  dockerCompat?: boolean | undefined | null;
}

interface NativeBindingOptions extends NativeBindingCommonOptions {
  dockerfilePath: string;
  dockerfileText?: string | undefined | null;
  contextDir?: string | undefined | null;
}

interface NativeBindingImportOptions extends NativeBindingCommonOptions {
  imageReference: string;
}

interface NativeBindingEvent {
  eventType: string;
  stream?: string | undefined | null;
  message: string;
}

interface NativeBinding {
  buildSandboxImage(
    options: NativeBindingOptions,
    emit?: ((event: NativeBindingEvent) => void) | null | undefined,
  ): Promise<string>;
  importSandboxImage(
    options: NativeBindingImportOptions,
    emit?: ((event: NativeBindingEvent) => void) | null | undefined,
  ): Promise<string>;
}

// `require` exists in the CJS bundle but not in ESM; declared here so the
// runtime check below typechecks under "module": "esnext".
declare const require: NodeRequire | undefined;
declare const module: { exports?: unknown } | undefined;

let cachedNativeBinding: NativeBinding | undefined;
let cachedNativeBindingError: Error | undefined;

function resolveRequire(): NodeRequire {
  // tsup rewrites `import.meta.url` to `import_meta.url` (where
  // `import_meta = {}`) in the CJS output, so `createRequire(import.meta.url)`
  // throws if called at all in CJS. CJS bundles get the real `require`
  // injected at runtime; prefer it. ESM bundles fall back to the createRequire
  // path, which only runs when `import.meta.url` is a real file URL.
  if (
    typeof module !== "undefined" &&
    module.exports != null &&
    typeof require !== "undefined"
  ) {
    return require;
  }
  return createRequire(import.meta.url);
}

function loadNativeBinding(): NativeBinding {
  if (cachedNativeBinding) return cachedNativeBinding;
  if (cachedNativeBindingError) throw cachedNativeBindingError;
  try {
    // The native binding is staged per-platform under
    // `typescript/dist/native/<triple>/tensorlake-node.node`. Loading goes
    // through the same helper the CLI binaries use so platform detection
    // stays in one place (`typescript/lib/runtime.cjs`). Deferred require so
    // the CJS bundle doesn't trip the `import.meta.url` → undefined fallback.
    const { loadNative } = resolveRequire()("../lib/runtime.cjs") as {
      loadNative: () => NativeBinding;
    };
    cachedNativeBinding = loadNative();
    return cachedNativeBinding;
  } catch (error) {
    cachedNativeBindingError =
      error instanceof Error ? error : new Error(String(error));
    throw cachedNativeBindingError;
  }
}

/** Test seam: replace the native binding with a stub. */
export function __setNativeBindingForTest(binding: NativeBinding | undefined): void {
  cachedNativeBinding = binding;
  cachedNativeBindingError = undefined;
}

// --- Build context ---------------------------------------------------------

export interface BuildContext {
  apiUrl: string;
  apiKey?: string;
  personalAccessToken?: string;
  namespace: string;
  organizationId?: string;
  projectId?: string;
  debug: boolean;
}

function nonEmpty(value: string | undefined): string | undefined {
  return value && value.length > 0 ? value : undefined;
}

function buildContextFromEnv(): BuildContext {
  const debugRaw = (process.env.TENSORLAKE_DEBUG ?? "").toLowerCase();
  const debug = ["1", "true", "yes", "on"].includes(debugRaw);
  return {
    apiUrl: nonEmpty(process.env.TENSORLAKE_API_URL) ?? "https://api.tensorlake.ai",
    apiKey: nonEmpty(process.env.TENSORLAKE_API_KEY),
    personalAccessToken: nonEmpty(process.env.TENSORLAKE_PAT),
    namespace: nonEmpty(process.env.INDEXIFY_NAMESPACE) ?? "default",
    organizationId: nonEmpty(process.env.TENSORLAKE_ORGANIZATION_ID),
    projectId: nonEmpty(process.env.TENSORLAKE_PROJECT_ID),
    debug,
  };
}

// --- Default-name helper ---------------------------------------------------

const DEFAULT_IMAGE_NAME = "default";

function defaultRegisteredName(dockerfilePath: string): string {
  const parsed = path.parse(dockerfilePath);
  if (parsed.name.toLowerCase() === "dockerfile") {
    const parentName = path.basename(path.dirname(dockerfilePath)).trim();
    return parentName || "sandbox-image";
  }
  return parsed.name || "sandbox-image";
}

/**
 * Mirror the Rust core's default name derivation for image imports: the last
 * path segment of the reference with any tag/digest stripped
 * (e.g. `pytorch/pytorch:2.4.1` -> `pytorch`, `ghcr.io/org/app@sha256:...`
 * -> `app`).
 */
function defaultRegisteredNameFromImage(imageReference: string): string {
  const withoutDigest = imageReference.split("@", 1)[0] ?? imageReference;
  const lastSegment = withoutDigest.split("/").pop() ?? withoutDigest;
  const colonIndex = lastSegment.lastIndexOf(":");
  const name =
    colonIndex > 0 && colonIndex < lastSegment.length - 1
      ? lastSegment.slice(0, colonIndex)
      : lastSegment;
  return name || "imported-image";
}

// --- Emit helpers ----------------------------------------------------------

function noopEmit(_event: Record<string, unknown>): void {}

function stderrEmit(event: Record<string, unknown>): void {
  const message = typeof event.message === "string" ? event.message : "";
  const type = typeof event.type === "string" ? event.type : "";
  if (type === "build_log") {
    const stream = typeof event.stream === "string" ? event.stream : "stdout";
    process.stderr.write(`[${stream}] ${message}\n`);
  } else if (message) {
    process.stderr.write(`[${type}] ${message}\n`);
  }
}

function ndjsonStdoutEmit(event: Record<string, unknown>): void {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

function eventToEmitDict(event: NativeBindingEvent): Record<string, unknown> {
  const out: Record<string, unknown> = {
    type: event.eventType,
    message: event.message,
  };
  if (event.stream != null) out.stream = event.stream;
  return out;
}

// --- Public API ------------------------------------------------------------

export async function createSandboxImage(
  source: SandboxImageSource,
  options: CreateSandboxImageOptions = {},
  deps: CreateSandboxImageDeps = {},
): Promise<Record<string, unknown>> {
  const emit = deps.emit ?? (options.verbose ? stderrEmit : noopEmit);
  const context = buildContextFromEnv();

  let dockerfilePath: string;
  let dockerfileText: string | undefined;
  let nativeContextDir: string | undefined;
  let effectiveName: string;

  if (source instanceof Image) {
    if (!source.baseImage) {
      throw new Error("Image must have a baseImage to build");
    }
    const resolvedContext = path.resolve(options.contextDir ?? process.cwd());
    dockerfilePath = path.join(resolvedContext, "Dockerfile");
    let text = dockerfileContent(source);
    if (!text.endsWith("\n")) text += "\n";
    dockerfileText = text;
    nativeContextDir = resolvedContext;
    effectiveName = options.registeredName ?? source.name;
  } else if (typeof source === "string") {
    const resolvedPath = path.resolve(source);
    if (!existsSync(resolvedPath)) {
      throw new Error(`Dockerfile not found: ${source}`);
    }
    dockerfilePath = resolvedPath;
    dockerfileText = undefined;
    nativeContextDir = undefined;
    effectiveName =
      options.registeredName ?? defaultRegisteredName(resolvedPath);
  } else {
    throw new TypeError(
      `source must be an Image or a Dockerfile path, got ${typeof source}`,
    );
  }

  if (effectiveName === DEFAULT_IMAGE_NAME) {
    // eslint-disable-next-line no-console
    console.warn(
      `Building sandbox image with the default name "${DEFAULT_IMAGE_NAME}". ` +
        "Pass `registeredName` or `Image({ name })` to avoid collisions " +
        "with other default-named images in this project.",
    );
  }

  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }

  emit({ type: "status", message: `Building image '${effectiveName}'...` });

  const binding = loadNativeBinding();
  let emitError: unknown;
  const resultJson = await binding.buildSandboxImage(
    {
      apiUrl: context.apiUrl,
      bearerToken,
      dockerfilePath,
      registeredName: effectiveName,
      diskMb: options.diskMb,
      builderDiskMb: options.builderDiskMb,
      cpus: options.cpus,
      memoryMb: options.memoryMb,
      isPublic: options.isPublic ?? false,
      organizationId: context.organizationId,
      projectId: context.projectId,
      namespace: context.namespace,
      useScopeHeaders:
        context.personalAccessToken != null && context.apiKey == null,
      userAgent: undefined,
      dockerCompat: options.dockerCompat ?? false,
      dockerfileText,
      contextDir: nativeContextDir,
    },
    (event) => {
      try {
        emit(eventToEmitDict(event));
      } catch (error) {
        emitError ??= error;
      }
    },
  );

  if (emitError != null) {
    throw emitError;
  }

  let result: Record<string, unknown> = {};
  if (resultJson.trim().length > 0) {
    result = JSON.parse(resultJson) as Record<string, unknown>;
  }
  emit({
    type: "image_registered",
    name: effectiveName,
    image_id:
      (typeof result.id === "string" && result.id) ||
      (typeof result.templateId === "string" && result.templateId) ||
      "",
  });
  emit({ type: "done" });
  return result;
}

/**
 * Import a registry image directly into a sandbox image — no Docker.
 *
 * Unlike {@link createSandboxImage}, there is no Dockerfile and no build
 * context: the builder pulls the referenced image's layers and applies them
 * straight into the rootfs (via `oci-image-to-ext4`), bypassing the Docker
 * daemon entirely. This is the programmatic backend for the
 * `tl sbx image import` CLI command. The import is always a fresh base from
 * the registry — the reference is never resolved against the template
 * registry.
 */
export async function importSandboxImage(
  imageReference: string,
  options: ImportSandboxImageOptions = {},
  deps: CreateSandboxImageDeps = {},
): Promise<Record<string, unknown>> {
  const emit = deps.emit ?? (options.verbose ? stderrEmit : noopEmit);

  if (typeof imageReference !== "string" || imageReference.trim().length === 0) {
    throw new Error("image reference to import must not be empty");
  }
  const reference = imageReference.trim();

  const context = buildContextFromEnv();
  const effectiveName =
    options.registeredName ?? defaultRegisteredNameFromImage(reference);

  if (effectiveName === DEFAULT_IMAGE_NAME) {
    // eslint-disable-next-line no-console
    console.warn(
      `Importing sandbox image with the default name "${DEFAULT_IMAGE_NAME}". ` +
        "Pass `registeredName` to avoid collisions with other default-named " +
        "images in this project.",
    );
  }

  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }

  emit({
    type: "status",
    message: `Importing image '${reference}' as '${effectiveName}'...`,
  });

  const binding = loadNativeBinding();
  let emitError: unknown;
  const resultJson = await binding.importSandboxImage(
    {
      apiUrl: context.apiUrl,
      bearerToken,
      imageReference: reference,
      registeredName: effectiveName,
      diskMb: options.diskMb,
      builderDiskMb: options.builderDiskMb,
      cpus: options.cpus,
      memoryMb: options.memoryMb,
      isPublic: options.isPublic ?? false,
      organizationId: context.organizationId,
      projectId: context.projectId,
      namespace: context.namespace,
      useScopeHeaders:
        context.personalAccessToken != null && context.apiKey == null,
      userAgent: undefined,
      dockerCompat: options.dockerCompat ?? false,
    },
    (event) => {
      try {
        emit(eventToEmitDict(event));
      } catch (error) {
        emitError ??= error;
      }
    },
  );

  if (emitError != null) {
    throw emitError;
  }

  let result: Record<string, unknown> = {};
  if (resultJson.trim().length > 0) {
    result = JSON.parse(resultJson) as Record<string, unknown>;
  }
  emit({
    type: "image_registered",
    name: effectiveName,
    image_id:
      (typeof result.id === "string" && result.id) ||
      (typeof result.templateId === "string" && result.templateId) ||
      "",
  });
  emit({ type: "done" });
  return result;
}

export async function deleteSandboxImage(imageName: string): Promise<void> {
  if (typeof imageName !== "string" || imageName.length === 0) {
    throw new TypeError("imageName must be a non-empty string");
  }

  const context = buildContextFromEnv();
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }

  const client = new CloudClient({
    apiUrl: context.apiUrl,
    apiKey: bearerToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    namespace: context.namespace,
  });
  try {
    await client.deleteSandboxImage(imageName);
  } finally {
    client.close();
  }
}

/**
 * Look up a registered sandbox image by its registered name.
 *
 * Returns the registered sandbox template, or `null` if no image with that
 * name exists. Uses the same environment-based Tensorlake auth as
 * `createSandboxImage`, and requires organization/project context
 * (`TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID`) since the lookup
 * is routed through the platform sandbox-templates API.
 */
export async function findSandboxImageByName(
  imageName: string,
): Promise<SandboxTemplate | null> {
  if (typeof imageName !== "string" || imageName.length === 0) {
    throw new TypeError("imageName must be a non-empty string");
  }

  const context = buildContextFromEnv();
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }
  if (!context.organizationId || !context.projectId) {
    throw new Error(
      "Looking up a sandbox image by name requires organization and project " +
        "context (TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID).",
    );
  }

  const client = new CloudClient({
    apiUrl: context.apiUrl,
    apiKey: bearerToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    namespace: context.namespace,
  });
  try {
    return await client.findSandboxImageByName(imageName);
  } finally {
    client.close();
  }
}

/**
 * List all registered sandbox images for the current project.
 *
 * Returns the registered sandbox templates (each with `id`, `name`,
 * `snapshotId`, `public`, etc.). Uses the same environment-based Tensorlake
 * auth as `createSandboxImage`, and requires organization/project context
 * (`TENSORLAKE_ORGANIZATION_ID` and `TENSORLAKE_PROJECT_ID`) since the listing
 * is routed through the platform sandbox-templates API.
 */
export async function listSandboxImages(): Promise<SandboxTemplate[]> {
  const context = buildContextFromEnv();
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials.");
  }
  if (!context.organizationId || !context.projectId) {
    throw new Error(
      "Listing sandbox images requires organization and project context " +
        "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID).",
    );
  }

  const client = new CloudClient({
    apiUrl: context.apiUrl,
    apiKey: bearerToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    namespace: context.namespace,
  });
  try {
    return await client.listSandboxImages();
  } finally {
    client.close();
  }
}

export async function runCreateSandboxImageCli(argv = process.argv.slice(2)) {
  const parsed = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      name: { type: "string", short: "n" },
      cpus: { type: "string" },
      memory: { type: "string" },
      disk_mb: { type: "string" },
      builder_disk_mb: { type: "string" },
      docker_compat: { type: "boolean", default: false },
      public: { type: "boolean", default: false },
    },
  });

  const dockerfilePath = parsed.positionals[0];
  if (!dockerfilePath) {
    throw new Error(
      "Usage: tensorlake-create-sandbox-image <dockerfile_path> [--name NAME] [--cpus N] [--memory MB] [--disk_mb MB] [--builder_disk_mb MB] [--docker_compat] [--public]",
    );
  }

  const cpus =
    parsed.values.cpus != null ? Number(parsed.values.cpus) : undefined;
  const memoryMb =
    parsed.values.memory != null ? Number(parsed.values.memory) : undefined;
  const diskMb =
    parsed.values.disk_mb != null ? Number(parsed.values.disk_mb) : undefined;
  const builderDiskMb =
    parsed.values.builder_disk_mb != null
      ? Number(parsed.values.builder_disk_mb)
      : undefined;
  if (cpus != null && !Number.isFinite(cpus)) {
    throw new Error(`Invalid --cpus value: ${parsed.values.cpus}`);
  }
  if (memoryMb != null && !Number.isInteger(memoryMb)) {
    throw new Error(`Invalid --memory value: ${parsed.values.memory}`);
  }
  if (diskMb != null && !Number.isInteger(diskMb)) {
    throw new Error(`Invalid --disk_mb value: ${parsed.values.disk_mb}`);
  }
  if (builderDiskMb != null && !Number.isInteger(builderDiskMb)) {
    throw new Error(
      `Invalid --builder_disk_mb value: ${parsed.values.builder_disk_mb}`,
    );
  }

  await createSandboxImage(
    dockerfilePath,
    {
      registeredName: parsed.values.name,
      cpus,
      memoryMb,
      diskMb,
      builderDiskMb,
      dockerCompat: parsed.values.docker_compat,
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit },
  );
}

export async function runImportSandboxImageCli(argv = process.argv.slice(2)) {
  const parsed = parseArgs({
    args: argv,
    allowPositionals: true,
    options: {
      name: { type: "string", short: "n" },
      cpus: { type: "string" },
      memory: { type: "string" },
      disk_mb: { type: "string" },
      builder_disk_mb: { type: "string" },
      docker_compat: { type: "boolean", default: false },
      public: { type: "boolean", default: false },
    },
  });

  const imageReference = parsed.positionals[0];
  if (!imageReference) {
    throw new Error(
      "Usage: tensorlake-import-sandbox-image <image_reference> [--name NAME] [--cpus N] [--memory MB] [--disk_mb MB] [--builder_disk_mb MB] [--docker_compat] [--public]",
    );
  }

  const cpus =
    parsed.values.cpus != null ? Number(parsed.values.cpus) : undefined;
  const memoryMb =
    parsed.values.memory != null ? Number(parsed.values.memory) : undefined;
  const diskMb =
    parsed.values.disk_mb != null ? Number(parsed.values.disk_mb) : undefined;
  const builderDiskMb =
    parsed.values.builder_disk_mb != null
      ? Number(parsed.values.builder_disk_mb)
      : undefined;
  if (cpus != null && !Number.isFinite(cpus)) {
    throw new Error(`Invalid --cpus value: ${parsed.values.cpus}`);
  }
  if (memoryMb != null && !Number.isInteger(memoryMb)) {
    throw new Error(`Invalid --memory value: ${parsed.values.memory}`);
  }
  if (diskMb != null && !Number.isInteger(diskMb)) {
    throw new Error(`Invalid --disk_mb value: ${parsed.values.disk_mb}`);
  }
  if (builderDiskMb != null && !Number.isInteger(builderDiskMb)) {
    throw new Error(
      `Invalid --builder_disk_mb value: ${parsed.values.builder_disk_mb}`,
    );
  }

  await importSandboxImage(
    imageReference,
    {
      registeredName: parsed.values.name,
      cpus,
      memoryMb,
      diskMb,
      builderDiskMb,
      dockerCompat: parsed.values.docker_compat,
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit },
  );
}
