import path from "node:path";
import os from "node:os";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { parseArgs } from "node:util";
import type { SandboxTemplate } from "./cloud-models.js";
import { Image, ImageBuildOperationType, dockerfileContent } from "./image.js";
import type { ImageBuildOperation } from "./image.js";
import { loadNative } from "./native-binding.js";

/**
 * Sandbox-image build engine.
 *
 * Renders the source to a Dockerfile (for `Image` inputs) and hands the
 * Dockerfile path/text + context to the Rust core via the platform-specific native binding,
 * which parses, validates, materializes, and registers the image. The Rust
 * core owns parsing and the warnings for instructions that run during the
 * build but have no effect when a sandbox runs from the image
 * (`ONBUILD`/`SHELL`/`EXPOSE`/`HEALTHCHECK`/`LABEL`/`STOPSIGNAL`/`VOLUME`).
 */

export interface CreateSandboxImageOptions {
  /**
   * Docker config JSON contents for this call, including credential helpers.
   * Omit to discover local Docker config at the start of the operation.
   * Pass "{}" to disable discovery and use no registry credentials.
   */
  dockerConfigJson?: string;

  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  builderDiskMb?: number;
  isPublic?: boolean;
  /**
   * Build context directory for an `Image` source, used exactly like
   * `docker build <contextDir>`: it is uploaded as-is and COPY/ADD sources
   * resolve relative to it. Required when the image has COPY/ADD ops that read
   * host files — building without it then throws. When the image has no such
   * ops it may be omitted, and an empty context (just the generated Dockerfile)
   * is uploaded so cwd is not archived. Ignored when the source is a Dockerfile
   * path (the Dockerfile's parent directory is used instead).
   */
  contextDir?: string;
  /**
   * Use Docker/BuildKit max compatibility mode (build is slower and uses more
   * memory and disk space on builder sandbox).
   */
  dockerCompat?: boolean;
  /**
   * Build a CAS (content-addressed streaming) image through the Image
   * Service instead of a legacy template. Off by default.
   */
  cas?: boolean;
  /**
   * Dockerfile build args (`--build-arg KEY=VALUE`), forwarded to BuildKit
   * and expanded in `FROM` references. CAS builds only; a legacy build with
   * build args is rejected.
   */
  buildArgs?: Record<string, string>;
  /**
   * Print build progress to stderr. Ignored when an explicit `emit` is
   * passed via `deps`. Defaults to false — `createSandboxImage` is silent
   * by default when invoked programmatically (e.g. `Image.build()`).
   */
  verbose?: boolean;
}

export interface ImportSandboxImageOptions {
  /**
   * Docker config JSON contents for this call, including credential helpers.
   * Omit to discover local Docker config at the start of the operation.
   * Pass "{}" to disable discovery and use no registry credentials.
   */
  dockerConfigJson?: string;

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
   * Import as a CAS (content-addressed streaming) image through the Image
   * Service instead of a legacy template. Off by default.
   */
  cas?: boolean;
  /**
   * Print build progress to stderr. Ignored when an explicit `emit` is
   * passed via `deps`. Defaults to false.
   */
  verbose?: boolean;
}

export type SandboxImageSource = string | Image;

export interface CreateSandboxImageDeps {
  emit?: (event: Record<string, unknown>) => void;
  /** @internal Standalone CLI entry points allow PAT auth; SDK callers do not. */
  _allowPat?: boolean;
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
  dockerConfigJson?: string | undefined | null;
}

interface NativeBindingOptions extends NativeBindingCommonOptions {
  dockerfilePath: string;
  dockerfileText?: string | undefined | null;
  contextDir?: string | undefined | null;
  cas?: boolean | undefined | null;
  buildArgs?: Record<string, string> | undefined | null;
}

interface NativeBindingImportOptions extends NativeBindingCommonOptions {
  imageReference: string;
  cas?: boolean | undefined | null;
}

/** Credentials and scope for the merged image catalog. */
interface NativeBindingCatalogOptions {
  apiUrl: string;
  bearerToken: string;
  organizationId?: string | undefined | null;
  projectId?: string | undefined | null;
  useScopeHeaders?: boolean | undefined | null;
  userAgent?: string | undefined | null;
}

interface NativeBindingEvent {
  eventType: string;
  stream?: string | undefined | null;
  message: string;
}

interface NativeBinding {
  loadDockerConfigJson(): string;
  buildSandboxImage(
    options: NativeBindingOptions,
    emit?: ((event: NativeBindingEvent) => void) | null | undefined,
  ): Promise<string>;
  importSandboxImage(
    options: NativeBindingImportOptions,
    emit?: ((event: NativeBindingEvent) => void) | null | undefined,
  ): Promise<string>;
  /** JSON array of every image of both families, newest first. */
  listSandboxImages(options: NativeBindingCatalogOptions): Promise<string>;
  /** JSON record of one image by name or id, or null when unknown. */
  findSandboxImage(
    options: NativeBindingCatalogOptions,
    reference: string,
  ): Promise<string | null>;
  /** Delete one image by name or id; unknown in both catalogs rejects. */
  deleteSandboxImage(
    options: NativeBindingCatalogOptions,
    reference: string,
  ): Promise<void>;
}

let cachedNativeBinding: NativeBinding | undefined;
let cachedNativeBindingError: Error | undefined;

function loadNativeBinding(): NativeBinding {
  if (cachedNativeBinding) return cachedNativeBinding;
  if (cachedNativeBindingError) throw cachedNativeBindingError;
  try {
    cachedNativeBinding = loadNative<NativeBinding>();
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

export function buildContextFromEnv(): BuildContext {
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

/**
 * True when a `RUN` op bind-mounts the build context (reads host files).
 * A BuildKit `RUN --mount=type=bind` reads from the build context unless it
 * mounts `from=<stage/image>`. `bind` is the default mount type, so a mount
 * with no explicit `type` counts too. Other mount types
 * (`cache`/`tmpfs`/`secret`/`ssh`) don't read the build context.
 */
function runOpMountsContext(op: ImageBuildOperation): boolean {
  if (op.type !== ImageBuildOperationType.RUN) {
    return false;
  }
  const mount = op.options.mount;
  if (mount == null || mount === "") {
    return false;
  }
  const fields: Record<string, string> = {};
  for (const token of mount.split(",")) {
    const eq = token.indexOf("=");
    const key = (eq === -1 ? token : token.slice(0, eq)).trim();
    const value = eq === -1 ? "" : token.slice(eq + 1).trim();
    fields[key] = value;
  }
  if ((fields.type ?? "bind") !== "bind") {
    return false;
  }
  return !("from" in fields);
}

/**
 * True when a build op reads files from the host build context.
 * `COPY`/`ADD` with host sources need a context; a `RUN` with a
 * `--mount=type=bind` (the default type) reads it too. `COPY --from=<stage>`,
 * remote `ADD <url>`, `from=` bind mounts, and non-bind mounts don't.
 */
function opReferencesHostFiles(op: ImageBuildOperation): boolean {
  if (op.type === ImageBuildOperationType.RUN) {
    return runOpMountsContext(op);
  }
  if (
    op.type !== ImageBuildOperationType.COPY &&
    op.type !== ImageBuildOperationType.ADD
  ) {
    return false;
  }
  if (op.options.from != null) {
    return false;
  }
  if (
    op.type === ImageBuildOperationType.ADD &&
    (op.args[0].startsWith("http://") || op.args[0].startsWith("https://"))
  ) {
    return false;
  }
  return true;
}

/**
 * True if the image has ops that read files from the host (COPY/ADD host
 * sources, or a RUN bind mount). Such builds need a build context to resolve
 * those sources, so the caller must pass `contextDir` (mirroring
 * `docker build <context>`).
 */
function imageRequiresContext(image: Image): boolean {
  return image.buildOperations.some(opReferencesHostFiles);
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
  // Throwaway empty build context created when an Image build has no explicit
  // contextDir. Removed in the finally below.
  let tempContextDir: string | undefined;

  if (source instanceof Image) {
    if (!source.baseImage) {
      throw new Error("Image must have a baseImage to build");
    }
    // With an explicit contextDir we upload that directory as-is, like
    // `docker build <dir>`. Without one, an image that reads host files via
    // COPY/ADD can't be built (there is no context to resolve them against);
    // images without such ops get an empty context (just the Dockerfile) so
    // the whole cwd is not archived.
    let resolvedContext: string;
    if (options.contextDir != null) {
      resolvedContext = path.resolve(options.contextDir);
    } else if (imageRequiresContext(source)) {
      throw new Error(
        "This image reads files from the host (via COPY/ADD or a RUN bind " +
          "mount), so it needs a build context. Pass `contextDir` pointing at " +
          "the directory that contains those sources; they resolve relative " +
          "to it, like `docker build <contextDir>`.",
      );
    } else {
      tempContextDir = mkdtempSync(path.join(os.tmpdir(), "tl-build-context-"));
      resolvedContext = tempContextDir;
    }
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

  try {
    if (effectiveName === DEFAULT_IMAGE_NAME) {
      console.warn(
        `Building sandbox image with the default name "${DEFAULT_IMAGE_NAME}". ` +
          "Pass `registeredName` or `Image({ name })` to avoid collisions " +
          "with other default-named images in this project.",
      );
    }

    const usePat = deps._allowPat === true && context.apiKey == null;
    const bearerToken = context.apiKey ?? (usePat ? context.personalAccessToken : undefined);
    if (!bearerToken) {
      if (context.personalAccessToken && !deps._allowPat) {
        throw new Error(
          "Sandbox image SDKs require TENSORLAKE_API_KEY. Personal access tokens are CLI-only.",
        );
      }
      throw new Error(
        deps._allowPat
          ? "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
          : "Missing TENSORLAKE_API_KEY credentials.",
      );
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
        organizationId: usePat ? context.organizationId : undefined,
        projectId: usePat ? context.projectId : undefined,
        namespace: context.namespace,
        useScopeHeaders: usePat,
        userAgent: undefined,
        dockerCompat: options.dockerCompat ?? false,
        dockerConfigJson: options.dockerConfigJson,
        dockerfileText,
        contextDir: nativeContextDir,
        cas: options.cas ?? false,
        buildArgs: options.buildArgs,
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
  } finally {
    if (tempContextDir != null) {
      rmSync(tempContextDir, { recursive: true, force: true });
    }
  }
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
    console.warn(
      `Importing sandbox image with the default name "${DEFAULT_IMAGE_NAME}". ` +
        "Pass `registeredName` to avoid collisions with other default-named " +
        "images in this project.",
    );
  }

  const usePat = deps._allowPat === true && context.apiKey == null;
  const bearerToken = context.apiKey ?? (usePat ? context.personalAccessToken : undefined);
  if (!bearerToken) {
    if (context.personalAccessToken && !deps._allowPat) {
      throw new Error(
        "Sandbox image SDKs require TENSORLAKE_API_KEY. Personal access tokens are CLI-only.",
      );
    }
    throw new Error(
      deps._allowPat
        ? "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        : "Missing TENSORLAKE_API_KEY credentials.",
    );
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
      organizationId: usePat ? context.organizationId : undefined,
      projectId: usePat ? context.projectId : undefined,
      namespace: context.namespace,
      useScopeHeaders: usePat,
      userAgent: undefined,
      dockerCompat: options.dockerCompat ?? false,
      dockerConfigJson: options.dockerConfigJson,
      cas: options.cas ?? false,
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

/** Credentials the merged-catalog operations run with. */
export interface SandboxImageCatalogCredentials {
  apiUrl?: string;
  apiKey?: string;
  organizationId?: string;
  projectId?: string;
}

function catalogOptionsFromEnv(): NativeBindingCatalogOptions {
  const context = buildContextFromEnv();
  const bearerToken = context.apiKey;
  if (!bearerToken) {
    if (context.personalAccessToken) {
      throw new Error(
        "Sandbox image SDKs require TENSORLAKE_API_KEY. Personal access tokens are CLI-only.",
      );
    }
    throw new Error("Missing TENSORLAKE_API_KEY credentials.");
  }
  return { apiUrl: context.apiUrl, bearerToken, useScopeHeaders: false };
}

function catalogOptionsFromCredentials(
  credentials: SandboxImageCatalogCredentials,
): NativeBindingCatalogOptions {
  const context = buildContextFromEnv();
  const bearerToken = credentials.apiKey ?? context.apiKey;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY credentials.");
  }
  return {
    apiUrl: credentials.apiUrl ?? context.apiUrl,
    bearerToken,
    organizationId: credentials.organizationId,
    projectId: credentials.projectId,
    useScopeHeaders: false,
  };
}

function parseTemplate(json: string): SandboxTemplate {
  return JSON.parse(json) as SandboxTemplate;
}

/**
 * Delete a registered sandbox image by name or id. Both families are
 * covered: an `image_` id or a name registered in the Image Service deletes
 * a CAS image, a `sandbox_template_` id or a name registered only in the
 * platform registry deletes a legacy image. Rejects when neither catalog
 * knows the reference.
 */
export async function deleteSandboxImage(imageName: string): Promise<void> {
  if (typeof imageName !== "string" || imageName.length === 0) {
    throw new TypeError("imageName must be a non-empty string");
  }
  await loadNativeBinding().deleteSandboxImage(catalogOptionsFromEnv(), imageName);
}

/** @internal `APIClient.deleteSandboxImage` with the client's credentials. */
export async function deleteSandboxImageWithCredentials(
  credentials: SandboxImageCatalogCredentials,
  imageName: string,
): Promise<void> {
  if (typeof imageName !== "string" || imageName.length === 0) {
    throw new TypeError("imageName must be a non-empty string");
  }
  await loadNativeBinding().deleteSandboxImage(
    catalogOptionsFromCredentials(credentials),
    imageName,
  );
}

/**
 * Look up a registered sandbox image by its registered name or id.
 *
 * Returns the image in the sandbox-template shape (CAS images carry
 * `snapshotFormatVersion === "content_addressed_streaming_v1"` plus `imageId`
 * and `buildId`), or `null` if neither the Image Service nor the platform
 * registry knows the reference. Uses the same environment-based Tensorlake
 * auth as `createSandboxImage`. Ingress derives project scope from the API key.
 */
export async function findSandboxImageByName(
  imageName: string,
): Promise<SandboxTemplate | null> {
  if (typeof imageName !== "string" || imageName.length === 0) {
    throw new TypeError("imageName must be a non-empty string");
  }
  const json = await loadNativeBinding().findSandboxImage(
    catalogOptionsFromEnv(),
    imageName,
  );
  return json == null ? null : parseTemplate(json);
}

/**
 * List all registered sandbox images for the current project: every CAS and
 * legacy image in the sandbox-template shape, newest first. Uses the same
 * environment-based Tensorlake auth as `createSandboxImage`. Ingress derives
 * project scope from the API key.
 */
export async function listSandboxImages(): Promise<SandboxTemplate[]> {
  const json = await loadNativeBinding().listSandboxImages(catalogOptionsFromEnv());
  return JSON.parse(json) as SandboxTemplate[];
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
      "Usage: tensorlake-create-sandbox-image <dockerfile_path> [--name NAME] [--cpus N] [--memory MiB] [--disk_mb MiB] [--builder_disk_mb MiB] [--docker_compat] [--public]",
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

  const dockerConfigJson = loadNativeBinding().loadDockerConfigJson();
  await createSandboxImage(
    dockerfilePath,
    {
      registeredName: parsed.values.name,
      dockerConfigJson,
      cpus,
      memoryMb,
      diskMb,
      builderDiskMb,
      dockerCompat: parsed.values.docker_compat,
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit, _allowPat: true },
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
      "Usage: tensorlake-import-sandbox-image <image_reference> [--name NAME] [--cpus N] [--memory MiB] [--disk_mb MiB] [--builder_disk_mb MiB] [--docker_compat] [--public]",
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

  const dockerConfigJson = loadNativeBinding().loadDockerConfigJson();
  await importSandboxImage(
    imageReference,
    {
      registeredName: parsed.values.name,
      dockerConfigJson,
      cpus,
      memoryMb,
      diskMb,
      builderDiskMb,
      dockerCompat: parsed.values.docker_compat,
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit, _allowPat: true },
  );
}
