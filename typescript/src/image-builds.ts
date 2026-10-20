import path from "node:path";
import os from "node:os";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import type { SandboxTemplate } from "./cloud-models.js";
import { Image, dockerfileContent } from "./image.js";
import {
  __imageBuildInternals,
  type CreateSandboxImageDeps,
} from "./sandbox-image.js";

/**
 * Image Service build operations for CAS sandbox images.
 *
 * A CAS (content-addressed streaming) image is built or imported by an Image
 * Service *build operation*. This module exposes that operation directly:
 * {@link startImageBuild} / {@link startImageImport} admit a build (and upload
 * and seal its context) and return an {@link ImageBuildHandle} without
 * waiting; {@link getImageBuild} looks a build up by id; the handle's
 * `refresh`, `wait` and `cancel` read, wait for and cancel it.
 *
 * **CAS only.** Everything here addresses the Image Service. Legacy images
 * (`createSandboxImage` without `cas`, `sandbox_template_` ids, `.tlsnap`
 * snapshots) have no build operation: a `sandbox_template_` or `image_`
 * reference passed as a build id is refused locally, before any request, and
 * the legacy functions in `sandbox-image.ts` keep their own requests,
 * results, errors and interruption behavior.
 *
 * **Build ids are the immutable record.** An image name and its `image_` id
 * name the *current* image; a rebuild replaces what they point at. A build id
 * and its result never change, so `wait` returns this build's own result even
 * after the image was rebuilt or deleted.
 *
 * **Cancellation by build id is sufficient.** Admission returns it, the CLI
 * prints it, and the `image_build_in_progress` conflict names it as
 * `activeBuildId`. The client request id (the admission's `Idempotency-Key`)
 * is kept on the handle only as a recovery aid for a lost admission answer.
 *
 * **AbortSignal versus timeout.** An `AbortSignal` passed to the new APIs (and
 * to `createSandboxImage`/`importSandboxImage` with `cas: true`) *requests
 * remote cancellation*: the Rust core runs a bounded (30 s) handshake and the
 * promise rejects with {@link ImageBuildInterruptedError} carrying the
 * outcome. A wait `timeoutMs` merely stops waiting: it rejects with
 * {@link ImageBuildTimeoutError} and leaves the build running. Abandoning a
 * promise, losing the process or dropping a handle never cancels the build.
 */

// --- Errors ------------------------------------------------------------------

/** Structured payload the native binding attaches to image-build failures. */
export interface ImageBuildErrorDetails {
  code: string;
  message: string;
  buildId?: string | null;
  clientRequestId?: string | null;
  imageName?: string | null;
  activeBuildId?: string | null;
  activeKind?: string | null;
  activeStatus?: string | null;
  error?: string | null;
  info?: Record<string, unknown> | null;
  outcome?: string | null;
  confirmed?: boolean | null;
  result?: Record<string, unknown> | null;
  waitedSecs?: number | null;
  status?: number | null;
}

/** An Image Service build operation failed. `code` is stable. */
export class ImageBuildError extends Error {
  readonly code: string;
  readonly buildId?: string;
  readonly clientRequestId?: string;
  readonly details: ImageBuildErrorDetails;

  constructor(details: ImageBuildErrorDetails) {
    super(details.message);
    this.name = "ImageBuildError";
    this.code = details.code;
    this.details = details;
    if (details.buildId) this.buildId = details.buildId;
    if (details.clientRequestId) this.clientRequestId = details.clientRequestId;
  }
}

/** No such build is visible to the project (unknown or foreign). */
export class ImageBuildNotFoundError extends ImageBuildError {
  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildNotFoundError";
  }
}

/**
 * The image name already has an unfinished build or import. Cancel
 * `activeBuildId` before starting another; the SDK never does so itself.
 */
export class ImageBuildInProgressError extends ImageBuildError {
  readonly imageName?: string;
  readonly activeBuildId?: string;
  readonly activeKind?: string;
  readonly activeStatus?: string;

  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildInProgressError";
    if (details.imageName) this.imageName = details.imageName;
    if (details.activeBuildId) this.activeBuildId = details.activeBuildId;
    if (details.activeKind) this.activeKind = details.activeKind;
    if (details.activeStatus) this.activeStatus = details.activeStatus;
  }
}

/** The service reports the build failed; `error` is its message. */
export class ImageBuildFailedError extends ImageBuildError {
  readonly error: string;
  readonly info?: ImageBuildInfo;

  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildFailedError";
    this.error = details.error ?? "no error recorded";
    if (details.info) this.info = parseBuildInfo(details.info);
  }
}

/** The service reports the build was cancelled (a distinct outcome). */
export class ImageBuildCancelledError extends ImageBuildError {
  readonly info?: ImageBuildInfo;

  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildCancelledError";
    if (details.info) this.info = parseBuildInfo(details.info);
  }
}

/** The caller stopped waiting. The build may still be running. */
export class ImageBuildTimeoutError extends ImageBuildError {
  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildTimeoutError";
  }
}

/**
 * An `AbortSignal` ran the cancellation handshake. `outcome` is `cancelled`,
 * `already_completed`, `already_cancelled`, `already_failed`, `not_admitted`
 * or `unconfirmed`; `confirmed` is true only when the remote build is known
 * not to publish. `not_admitted` is an observation, not a cancellation.
 */
export class ImageBuildInterruptedError extends ImageBuildError {
  readonly outcome: string;
  readonly confirmed: boolean;
  readonly result?: Record<string, unknown>;

  constructor(details: ImageBuildErrorDetails) {
    super(details);
    this.name = "ImageBuildInterruptedError";
    this.outcome = details.outcome ?? "unconfirmed";
    this.confirmed = details.confirmed === true;
    if (details.result) this.result = details.result;
  }
}

function camelize(key: string): string {
  return key.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

/**
 * Translate a native `{category: "image_build", ...}` rejection into the
 * typed error classes. Other errors pass through unchanged.
 */
export function toImageBuildError(error: unknown): unknown {
  if (!(error instanceof Error)) return error;
  let payload: Record<string, unknown>;
  try {
    payload = JSON.parse(error.message) as Record<string, unknown>;
  } catch {
    return error;
  }
  if (!payload || payload.category !== "image_build") return error;
  const details: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(payload)) {
    if (key === "category") continue;
    details[camelize(key)] = value;
  }
  const typed = details as unknown as ImageBuildErrorDetails;
  switch (typed.code) {
    case "not_found":
      return new ImageBuildNotFoundError(typed);
    case "image_build_in_progress":
      return new ImageBuildInProgressError(typed);
    case "failed":
      return new ImageBuildFailedError(typed);
    case "cancelled":
      return new ImageBuildCancelledError(typed);
    case "timeout":
      return new ImageBuildTimeoutError(typed);
    case "interrupted":
      return new ImageBuildInterruptedError(typed);
    default:
      return new ImageBuildError(typed);
  }
}

// --- Models ------------------------------------------------------------------

export type ImageBuildStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled" | string;
export type ImageBuildPhase =
  | "awaiting_context"
  | "starting_builder"
  | "executing"
  | "snapshotting"
  | "publishing"
  | "finished"
  | string;

/**
 * The published image of a succeeded build, in the sandbox-template shape
 * the merged catalog answers (derived by the core from the Image Service's
 * trimmed record): one image representation for `findSandboxImageByName`,
 * `listSandboxImages` and build results.
 */
export type ImageBuildResult = SandboxTemplate;

/**
 * One build or import as the Image Service reports it (the twelve-field
 * `BuildInfo`; the service exposes no cleanup state, phase deadline,
 * content digest or upload capability on a build).
 */
export interface ImageBuildInfo {
  buildId: string;
  clientRequestId?: string | null;
  kind: string;
  name?: string | null;
  status: ImageBuildStatus;
  phase?: ImageBuildPhase | null;
  attemptNo: number;
  builderSandboxId?: string | null;
  error?: string | null;
  result?: ImageBuildResult | null;
  operationDeadlineMs?: number | null;
  createdAtMs?: number | null;
  updatedAtMs?: number | null;
  /** The service record as received, snake_case, with unknown fields. */
  raw: Record<string, unknown>;
}

/**
 * The service's answer to a cancellation. `outcome` is `cancelled`
 * (durable), `already_cancelled`, `already_completed` (publication won),
 * `already_failed` or `not_admitted` (nothing indexed under the client
 * request id: not a cancellation and no promise about a delayed admission).
 */
export interface CancelImageBuildOutcome {
  outcome: string;
  build: ImageBuildInfo | null;
  /** Whether the build is known not to publish. */
  confirmed: boolean;
}

function camelizeRecord(value: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, inner] of Object.entries(value)) out[camelize(key)] = inner;
  return out;
}

function parseBuildInfo(raw: Record<string, unknown>): ImageBuildInfo {
  const camel = camelizeRecord(raw);
  const result =
    raw.result && typeof raw.result === "object"
      ? (camelizeRecord(raw.result as Record<string, unknown>) as unknown as ImageBuildResult)
      : null;
  return {
    buildId: String(raw.build_id ?? ""),
    clientRequestId: (camel.clientRequestId as string | null | undefined) ?? null,
    kind: String(raw.kind ?? ""),
    name: (raw.name as string | null | undefined) ?? null,
    status: String(raw.status ?? ""),
    phase: (raw.phase as string | null | undefined) ?? null,
    attemptNo: Number(raw.attempt_no ?? 0),
    builderSandboxId: (camel.builderSandboxId as string | null | undefined) ?? null,
    error: (raw.error as string | null | undefined) ?? null,
    result,
    operationDeadlineMs: (camel.operationDeadlineMs as number | null | undefined) ?? null,
    createdAtMs: (camel.createdAtMs as number | null | undefined) ?? null,
    updatedAtMs: (camel.updatedAtMs as number | null | undefined) ?? null,
    raw,
  };
}

function parseResult(json: string): ImageBuildResult {
  return camelizeRecord(JSON.parse(json) as Record<string, unknown>) as unknown as ImageBuildResult;
}

// --- Native plumbing -----------------------------------------------------------

export interface ImageBuildEvent {
  type: string;
  message?: string;
  stream?: string;
  build_id?: string;
  client_request_id?: string | null;
  status?: string;
  phase?: string | null;
  attempt?: number;
  outcome?: string;
  confirmed?: boolean;
  [key: string]: unknown;
}

export interface ImageBuildWaitOptions {
  /**
   * Caller bound in milliseconds on top of the service-informed deadline.
   * Expiry rejects with {@link ImageBuildTimeoutError} and leaves the build
   * running; it does not cancel.
   */
  timeoutMs?: number;
  /** Stream `tl-image-builder` output as `build_log` events. */
  followLogs?: boolean;
  /** Requests remote cancellation (bounded handshake), unlike `timeoutMs`. */
  signal?: AbortSignal;
  /**
   * Event callback. A throwing callback is recorded and rethrown once the
   * native call returns; it does not cancel the build.
   */
  emit?: (event: ImageBuildEvent) => void;
  verbose?: boolean;
}

interface Credentials {
  apiUrl: string;
  bearerToken: string;
  namespace: string;
}

function credentialsFromEnv(): Credentials {
  const context = __imageBuildInternals.buildContextFromEnv();
  if (!context.apiKey) {
    if (context.personalAccessToken) {
      throw new ImageBuildError({
        code: "auth",
        message:
          "Sandbox image SDKs require TENSORLAKE_API_KEY. Personal access tokens are CLI-only.",
      });
    }
    throw new ImageBuildError({ code: "auth", message: "Missing TENSORLAKE_API_KEY credentials." });
  }
  return { apiUrl: context.apiUrl, bearerToken: context.apiKey, namespace: context.namespace };
}

function clientOptions(credentials: Credentials) {
  return {
    apiUrl: credentials.apiUrl,
    bearerToken: credentials.bearerToken,
    namespace: credentials.namespace,
    useScopeHeaders: false,
  };
}

/**
 * New operations never reinterpret legacy references: fail before any
 * request so no Image Service or platform call is made.
 */
function rejectLegacyReference(buildId: string): void {
  if (typeof buildId !== "string" || buildId.trim().length === 0) {
    throw new ImageBuildError({ code: "usage", message: "buildId must be a non-empty string" });
  }
  if (buildId.startsWith("sandbox_template_")) {
    throw new ImageBuildError({
      code: "usage",
      message: `'${buildId}' is a legacy sandbox template id; legacy images have no build operation to get, wait for or cancel`,
      buildId,
    });
  }
  if (buildId.startsWith("image_")) {
    throw new ImageBuildError({
      code: "usage",
      message: `'${buildId}' is an image id, not a build id; look the image up with findSandboxImageByName or use the build id admission returned`,
      buildId,
    });
  }
}

/**
 * Run a native CAS operation with an optional AbortSignal wired to the Rust
 * interrupt registry. Only the new operation API and explicitly selected CAS
 * paths call this; legacy calls never reach it.
 */
export async function __runWithInterrupt<T>(
  signal: AbortSignal | undefined,
  run: (interruptId: number | undefined) => Promise<T>,
): Promise<T> {
  const binding = __imageBuildInternals.loadNativeBinding();
  if (signal === undefined) {
    try {
      return await run(undefined);
    } catch (error) {
      throw toImageBuildError(error);
    }
  }
  const interruptId = binding.createImageBuildInterrupt();
  const onAbort = () => binding.triggerImageBuildInterrupt(interruptId);
  if (signal.aborted) onAbort();
  else signal.addEventListener("abort", onAbort, { once: true });
  try {
    return await run(interruptId);
  } catch (error) {
    throw toImageBuildError(error);
  } finally {
    signal.removeEventListener("abort", onAbort);
    binding.releaseImageBuildInterrupt(interruptId);
  }
}

function makeEmitter(options: { emit?: (event: ImageBuildEvent) => void; verbose?: boolean }) {
  const emit =
    options.emit ??
    (options.verbose ? __imageBuildInternals.stderrEmit : __imageBuildInternals.noopEmit);
  let emitError: unknown;
  const forward = (event: {
    eventType: string;
    stream?: string | null;
    message: string;
    buildId?: string | null;
    clientRequestId?: string | null;
    status?: string | null;
    phase?: string | null;
    attempt?: number | null;
    outcome?: string | null;
    confirmed?: boolean | null;
  }) => {
    try {
      emit(__imageBuildInternals.eventToEmitDict(event) as ImageBuildEvent);
    } catch (error) {
      emitError ??= error;
    }
  };
  return {
    forward,
    emit,
    rethrow() {
      if (emitError != null) throw emitError;
    },
  };
}

// --- Handle --------------------------------------------------------------------

/**
 * One admitted CAS build. Holds the build identity and the caller's
 * credentials; creating or dropping it performs no network I/O.
 */
export class ImageBuildHandle {
  readonly buildId: string;
  /**
   * The admission's request id, when this handle started the build. Never
   * needed to cancel; a recovery aid for a lost admission answer.
   */
  readonly clientRequestId: string | null;
  #info: ImageBuildInfo;
  #credentials: Credentials;

  /** @internal */
  constructor(info: ImageBuildInfo, credentials: Credentials, clientRequestId?: string | null) {
    this.buildId = info.buildId;
    this.clientRequestId = clientRequestId ?? info.clientRequestId ?? null;
    this.#info = info;
    this.#credentials = credentials;
  }

  /** The last observed state. */
  get info(): ImageBuildInfo {
    return this.#info;
  }

  /** Re-read the current state. */
  async refresh(): Promise<ImageBuildInfo> {
    const binding = __imageBuildInternals.loadNativeBinding();
    let json: string;
    try {
      json = await binding.getImageBuild(clientOptions(this.#credentials), this.buildId);
    } catch (error) {
      throw toImageBuildError(error);
    }
    this.#info = parseBuildInfo(JSON.parse(json) as Record<string, unknown>);
    return this.#info;
  }

  /**
   * Wait for publication and return this build's immutable result. See
   * {@link ImageBuildWaitOptions} for the timeout/abort distinction.
   */
  async wait(options: ImageBuildWaitOptions = {}): Promise<ImageBuildResult> {
    const binding = __imageBuildInternals.loadNativeBinding();
    const emitter = makeEmitter(options);
    const json = await __runWithInterrupt(options.signal, (interruptId) =>
      binding.waitImageBuild(
        clientOptions(this.#credentials),
        this.buildId,
        { timeoutMs: options.timeoutMs, followLogs: options.followLogs ?? false },
        emitter.forward,
        interruptId,
      ),
    );
    emitter.rethrow();
    const result = parseResult(json);
    this.#info = { ...this.#info, status: "succeeded", result };
    return result;
  }

  /** Cancel this build by its id (the sufficient selector). */
  async cancel(): Promise<CancelImageBuildOutcome> {
    return cancelImageBuildWith(this.#credentials, { buildId: this.buildId });
  }
}

// --- Operations ------------------------------------------------------------------

export interface StartImageBuildOptions {
  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  isPublic?: boolean;
  contextDir?: string;
  buildArgs?: Record<string, string>;
  dockerConfigJson?: string;
  /** Requests remote cancellation of the admission/upload in progress. */
  signal?: AbortSignal;
  emit?: (event: ImageBuildEvent) => void;
  verbose?: boolean;
}

export interface StartImageImportOptions {
  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  isPublic?: boolean;
  dockerConfigJson?: string;
  signal?: AbortSignal;
  emit?: (event: ImageBuildEvent) => void;
  verbose?: boolean;
}

function decodeStarted(json: string, credentials: Credentials): ImageBuildHandle {
  const payload = JSON.parse(json) as { build: Record<string, unknown>; client_request_id?: string | null };
  return new ImageBuildHandle(parseBuildInfo(payload.build ?? {}), credentials, payload.client_request_id);
}

/**
 * Admit a CAS image build and return without waiting. Parses the source like
 * `createSandboxImage` with `cas: true`, admits under a fresh client request
 * id, uploads and seals the context, and returns a handle whose `wait`
 * finishes the operation. The build keeps running if this process exits.
 */
export async function startImageBuild(
  source: string | Image,
  options: StartImageBuildOptions = {},
  deps: CreateSandboxImageDeps = {},
): Promise<ImageBuildHandle> {
  const emitter = makeEmitter({ emit: deps.emit ?? options.emit, verbose: options.verbose });
  let dockerfilePath: string;
  let dockerfileText: string | undefined;
  let contextDir: string | undefined;
  let name: string;
  let tempContextDir: string | undefined;
  if (source instanceof Image) {
    if (!source.baseImage) throw new Error("Image must have a baseImage to build");
    if (options.contextDir != null) {
      contextDir = path.resolve(options.contextDir);
    } else if (__imageBuildInternals.imageRequiresContext(source)) {
      throw new Error(
        "This image reads files from the host (via COPY/ADD or a RUN bind mount), so it needs a " +
          "build context. Pass `contextDir` pointing at the directory that contains those " +
          "sources; they resolve relative to it, like `docker build <contextDir>`.",
      );
    } else {
      tempContextDir = mkdtempSync(path.join(os.tmpdir(), "tl-build-context-"));
      contextDir = tempContextDir;
    }
    dockerfilePath = path.join(contextDir, "Dockerfile");
    let text = dockerfileContent(source);
    if (!text.endsWith("\n")) text += "\n";
    dockerfileText = text;
    name = options.registeredName ?? source.name;
  } else if (typeof source === "string") {
    const resolved = path.resolve(source);
    if (!existsSync(resolved)) throw new Error(`Dockerfile not found: ${source}`);
    dockerfilePath = resolved;
    name = options.registeredName ?? __imageBuildInternals.defaultRegisteredName(resolved);
  } else {
    throw new TypeError(`source must be an Image or a Dockerfile path, got ${typeof source}`);
  }
  try {
    const credentials = credentialsFromEnv();
    const binding = __imageBuildInternals.loadNativeBinding();
    emitter.emit({ type: "status", message: `Building image '${name}'...` });
    const json = await __runWithInterrupt(options.signal, (interruptId) =>
      binding.startImageBuild(
        {
          apiUrl: credentials.apiUrl,
          bearerToken: credentials.bearerToken,
          dockerfilePath,
          registeredName: name,
          diskMb: options.diskMb,
          cpus: options.cpus,
          memoryMb: options.memoryMb,
          isPublic: options.isPublic ?? false,
          namespace: credentials.namespace,
          useScopeHeaders: false,
          dockerConfigJson: options.dockerConfigJson,
          dockerfileText,
          contextDir,
          cas: true,
          buildArgs: options.buildArgs,
        },
        emitter.forward,
        interruptId,
      ),
    );
    emitter.rethrow();
    return decodeStarted(json, credentials);
  } finally {
    if (tempContextDir != null) rmSync(tempContextDir, { recursive: true, force: true });
  }
}

/** Admit a CAS registry import and return without waiting. */
export async function startImageImport(
  imageReference: string,
  options: StartImageImportOptions = {},
  deps: CreateSandboxImageDeps = {},
): Promise<ImageBuildHandle> {
  if (typeof imageReference !== "string" || imageReference.trim().length === 0) {
    throw new Error("image reference to import must not be empty");
  }
  const reference = imageReference.trim();
  const name =
    options.registeredName ?? __imageBuildInternals.defaultRegisteredNameFromImage(reference);
  const emitter = makeEmitter({ emit: deps.emit ?? options.emit, verbose: options.verbose });
  const credentials = credentialsFromEnv();
  const binding = __imageBuildInternals.loadNativeBinding();
  emitter.emit({ type: "status", message: `Importing image '${reference}' as '${name}'...` });
  const json = await __runWithInterrupt(options.signal, (interruptId) =>
    binding.startImageImport(
      {
        apiUrl: credentials.apiUrl,
        bearerToken: credentials.bearerToken,
        imageReference: reference,
        registeredName: name,
        diskMb: options.diskMb,
        cpus: options.cpus,
        memoryMb: options.memoryMb,
        isPublic: options.isPublic ?? false,
        namespace: credentials.namespace,
        useScopeHeaders: false,
        dockerConfigJson: options.dockerConfigJson,
        cas: true,
      },
      emitter.forward,
      interruptId,
    ),
  );
  emitter.rethrow();
  return decodeStarted(json, credentials);
}

/**
 * Look a build up by its service-generated id. Unknown and foreign builds
 * reject with {@link ImageBuildNotFoundError}; a legacy `sandbox_template_`
 * or an `image_` reference is refused before any request.
 */
export async function getImageBuild(buildId: string): Promise<ImageBuildHandle> {
  rejectLegacyReference(buildId);
  const credentials = credentialsFromEnv();
  const binding = __imageBuildInternals.loadNativeBinding();
  let json: string;
  try {
    json = await binding.getImageBuild(clientOptions(credentials), buildId);
  } catch (error) {
    throw toImageBuildError(error);
  }
  return new ImageBuildHandle(parseBuildInfo(JSON.parse(json) as Record<string, unknown>), credentials);
}

/** `getImageBuild(buildId)` then `wait(options)`. */
export async function waitImageBuild(
  buildId: string,
  options: ImageBuildWaitOptions = {},
): Promise<ImageBuildResult> {
  const handle = await getImageBuild(buildId);
  return handle.wait(options);
}

export interface CancelImageBuildSelector {
  /** The sufficient, documented selector. */
  buildId?: string;
  /**
   * Only to recover an admission whose response was lost. Its `not_admitted`
   * outcome is an observation of absence, not a cancellation, and does not
   * prevent a delayed admission (cancel again by request id then).
   */
  clientRequestId?: string;
}

async function cancelImageBuildWith(
  credentials: Credentials,
  selector: CancelImageBuildSelector,
): Promise<CancelImageBuildOutcome> {
  const binding = __imageBuildInternals.loadNativeBinding();
  let json: string;
  try {
    json = await binding.cancelImageBuild(clientOptions(credentials), {
      buildId: selector.buildId,
      clientRequestId: selector.clientRequestId,
    });
  } catch (error) {
    throw toImageBuildError(error);
  }
  const payload = JSON.parse(json) as { outcome: string; build?: Record<string, unknown> | null };
  const outcome = String(payload.outcome ?? "");
  return {
    outcome,
    build: payload.build ? parseBuildInfo(payload.build) : null,
    confirmed: ["cancelled", "already_cancelled", "already_failed"].includes(outcome),
  };
}

/** Cancel a build. Exactly one selector; the build id alone is sufficient. */
export async function cancelImageBuild(
  selector: CancelImageBuildSelector,
): Promise<CancelImageBuildOutcome> {
  const hasBuild = selector.buildId != null;
  const hasRequest = selector.clientRequestId != null;
  if (hasBuild === hasRequest) {
    throw new ImageBuildError({
      code: "usage",
      message: "pass exactly one of buildId or clientRequestId",
    });
  }
  if (selector.buildId != null) rejectLegacyReference(selector.buildId);
  return cancelImageBuildWith(credentialsFromEnv(), selector);
}

export type { SandboxTemplate };
