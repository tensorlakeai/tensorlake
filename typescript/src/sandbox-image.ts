import { readFile, readdir, stat } from "node:fs/promises";
import { homedir } from "node:os";
import path from "node:path";
import { parseArgs } from "node:util";
import { type CommandResult, type ProcessInfo } from "./models.js";
import { type OutputResponse, ProcessStatus } from "./models.js";
import { SandboxClient } from "./client.js";
import { Image, dockerfileContent } from "./image.js";

const DEFAULT_ROOTFS_DISK_MB = 10 * 1024;
const REMOTE_BUILD_DIR = "/var/lib/tensorlake/rootfs-builder/build";
const REMOTE_CONTEXT_DIR = "/var/lib/tensorlake/rootfs-builder/build/context";
const REMOTE_SPEC_PATH = "/var/lib/tensorlake/rootfs-builder/build/spec.json";
const REMOTE_METADATA_PATH = "/var/lib/tensorlake/rootfs-builder/build/metadata.json";
const ROOTFS_BUILDER_BIN_DIR = "/usr/local/bin";
const ROOTFS_BUILDER_PATH = "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
const ROOTFS_BUILDER_COMMAND = "tl-rootfs-build";
const UNSUPPORTED_DOCKERFILE_INSTRUCTIONS = new Set([
  "ARG",
  "ONBUILD",
  "SHELL",
  "USER",
]);

export interface DockerfileInstruction {
  keyword: string;
  value: string;
  lineNumber: number;
}

export interface DockerfileBuildPlan {
  dockerfilePath: string;
  contextDir: string;
  registeredName: string;
  dockerfileText: string;
  baseImage?: string;
  instructions: DockerfileInstruction[];
}

export interface CreateSandboxImageOptions {
  registeredName?: string;
  cpus?: number;
  memoryMb?: number;
  diskMb?: number;
  builderDiskMb?: number;
  isPublic?: boolean;
  contextDir?: string;
  /**
   * Print build progress to stderr. Ignored when an explicit `emit` is passed
   * via `deps`. Defaults to false — `createSandboxImage` is silent by default
   * when invoked programmatically (e.g. `Image.build()`).
   */
  verbose?: boolean;
}

export type SandboxImageSource = string | Image;

export interface BuildContext {
  apiUrl: string;
  apiKey?: string;
  personalAccessToken?: string;
  namespace: string;
  organizationId?: string;
  projectId?: string;
  debug: boolean;
}

interface BuildSandbox {
  sandboxId: string;
  run(
    command: string,
    options?: {
      args?: string[];
      env?: Record<string, string>;
      workingDir?: string;
      timeout?: number;
    },
  ): Promise<CommandResult>;
  startProcess(
    command: string,
    options?: {
      args?: string[];
      env?: Record<string, string>;
      workingDir?: string;
    },
  ): Promise<ProcessInfo>;
  getStdout(pid: number): Promise<OutputResponse>;
  getStderr(pid: number): Promise<OutputResponse>;
  getProcess(pid: number): Promise<ProcessInfo>;
  writeFile(path: string, content: Uint8Array): Promise<void>;
  readFile(path: string): Promise<Uint8Array>;
  terminate(): Promise<void>;
}

interface BuildClient {
  createAndConnect(options: {
    image?: string;
    cpus?: number;
    memoryMb?: number;
    diskMb?: number;
  }): Promise<BuildSandbox>;
  close(): void;
}

interface PreparedSandboxTemplateBuild {
  buildId: string;
  snapshotId: string;
  snapshotUri: string;
  rootfsNodeKind: string;
  builder: PreparedRootfsBuilder;
  parent?: PreparedRootfsParent | null;
  [key: string]: unknown;
}

interface PreparedRootfsBuilder {
  image: string;
  command: string;
  cpus: number;
  memoryMb: number;
  diskMb: number;
}

interface PreparedRootfsParent {
  parentManifestUri: string;
  rootfsDiskBytes?: number | null;
}

interface ResolvedBuildContext extends BuildContext {
  organizationId: string;
  projectId: string;
  bearerToken: string;
  useScopeHeaders: boolean;
}

interface CompleteSandboxTemplateBuildRequest {
  snapshotId: string;
  snapshotUri: string;
  snapshotFormatVersion: string;
  snapshotSizeBytes: number;
  rootfsDiskBytes: number;
  rootfsNodeKind: string;
  parentManifestUri?: string;
}

interface CreateSandboxImageDeps {
  emit?: (event: Record<string, unknown>) => void;
  createClient?: (context: BuildContext) => BuildClient;
  sleep?: (ms: number) => Promise<void>;
}

export function defaultRegisteredName(dockerfilePath: string): string {
  const parsed = path.parse(dockerfilePath);
  if (parsed.name.toLowerCase() === "dockerfile") {
    const parentName = path.basename(path.dirname(dockerfilePath)).trim();
    return parentName || "sandbox-image";
  }
  return parsed.name || "sandbox-image";
}

export function logicalDockerfileLines(
  dockerfileText: string,
): Array<{ lineNumber: number; line: string }> {
  const logicalLines: Array<{ lineNumber: number; line: string }> = [];
  let parts: string[] = [];
  let startLine: number | null = null;

  for (const [index, rawLine] of dockerfileText.split(/\r?\n/).entries()) {
    const lineNumber = index + 1;
    const stripped = rawLine.trim();
    if (parts.length === 0 && (!stripped || stripped.startsWith("#"))) {
      continue;
    }

    if (startLine == null) {
      startLine = lineNumber;
    }

    let line = rawLine.replace(/\s+$/, "");
    const continued = line.endsWith("\\");
    if (continued) {
      line = line.slice(0, -1);
    }

    const normalized = line.trim();
    if (normalized && !normalized.startsWith("#")) {
      parts.push(normalized);
    }

    if (continued) {
      continue;
    }

    if (parts.length > 0) {
      logicalLines.push({
        lineNumber: startLine ?? lineNumber,
        line: parts.join(" "),
      });
    }
    parts = [];
    startLine = null;
  }

  if (parts.length > 0) {
    logicalLines.push({
      lineNumber: startLine ?? 1,
      line: parts.join(" "),
    });
  }

  return logicalLines;
}

function splitInstruction(
  line: string,
  lineNumber: number,
): { keyword: string; value: string } {
  const trimmed = line.trim();
  if (!trimmed) {
    throw new Error(`line ${lineNumber}: empty Dockerfile instruction`);
  }
  const match = trimmed.match(/^(\S+)(?:\s+(.*))?$/);
  if (!match) {
    throw new Error(`line ${lineNumber}: invalid Dockerfile instruction`);
  }
  return {
    keyword: match[1].toUpperCase(),
    value: (match[2] ?? "").trim(),
  };
}

function shellSplit(input: string): string[] {
  const tokens: string[] = [];
  let current = "";
  let quote: "'" | '"' | null = null;
  let escape = false;

  for (let i = 0; i < input.length; i++) {
    const char = input[i];

    if (escape) {
      current += char;
      escape = false;
      continue;
    }

    if (quote == null) {
      if (/\s/.test(char)) {
        if (current) {
          tokens.push(current);
          current = "";
        }
        continue;
      }
      if (char === "'" || char === '"') {
        quote = char;
        continue;
      }
      if (char === "\\") {
        escape = true;
        continue;
      }
      current += char;
      continue;
    }

    if (quote === "'") {
      if (char === "'") {
        quote = null;
      } else {
        current += char;
      }
      continue;
    }

    if (char === '"') {
      quote = null;
      continue;
    }
    if (char === "\\") {
      const next = input[++i];
      if (next == null) {
        throw new Error(`unterminated escape sequence in '${input}'`);
      }
      current += next;
      continue;
    }
    current += char;
  }

  if (escape) {
    throw new Error(`unterminated escape sequence in '${input}'`);
  }
  if (quote != null) {
    throw new Error(`unterminated quoted string in '${input}'`);
  }
  if (current) {
    tokens.push(current);
  }
  return tokens;
}

function stripLeadingFlags(value: string): {
  flags: Record<string, string>;
  remaining: string;
} {
  const flags: Record<string, string> = {};
  let remaining = value.trimStart();

  while (remaining.startsWith("--")) {
    const firstSpace = remaining.indexOf(" ");
    if (firstSpace === -1) {
      throw new Error(`invalid Dockerfile flag syntax: ${value}`);
    }

    const token = remaining.slice(0, firstSpace);
    const rest = remaining.slice(firstSpace + 1).trimStart();
    const flagBody = token.slice(2);

    if (flagBody.includes("=")) {
      const [key, flagValue] = flagBody.split(/=(.*)/s, 2);
      flags[key] = flagValue;
      remaining = rest;
      continue;
    }

    const [flagValue, ...restTokens] = shellSplit(rest);
    if (flagValue == null) {
      throw new Error(`missing value for Dockerfile flag '${token}'`);
    }
    flags[flagBody] = flagValue;
    remaining = restTokens.join(" ");
  }

  return { flags, remaining };
}

function parseFromValue(value: string, lineNumber: number): string {
  const { remaining } = stripLeadingFlags(value);
  const tokens = shellSplit(remaining);
  if (tokens.length === 0) {
    throw new Error(`line ${lineNumber}: FROM must include a base image`);
  }
  if (tokens.length > 1 && tokens[1].toLowerCase() !== "as") {
    throw new Error(`line ${lineNumber}: unsupported FROM syntax '${value}'`);
  }
  return tokens[0];
}

function buildPlanFromDockerfileText(
  dockerfileText: string,
  dockerfilePath: string,
  contextDir: string,
  registeredName?: string,
): DockerfileBuildPlan {
  let baseImage: string | undefined;
  const instructions: DockerfileInstruction[] = [];

  for (const logicalLine of logicalDockerfileLines(dockerfileText)) {
    const { keyword, value } = splitInstruction(
      logicalLine.line,
      logicalLine.lineNumber,
    );
    if (keyword === "FROM") {
      if (baseImage != null) {
        throw new Error(
          `line ${logicalLine.lineNumber}: multi-stage Dockerfiles are not supported for sandbox image creation`,
        );
      }
      baseImage = parseFromValue(value, logicalLine.lineNumber);
      continue;
    }

    if (UNSUPPORTED_DOCKERFILE_INSTRUCTIONS.has(keyword)) {
      throw new Error(
        `line ${logicalLine.lineNumber}: Dockerfile instruction '${keyword}' is not supported for sandbox image creation`,
      );
    }

    instructions.push({
      keyword,
      value,
      lineNumber: logicalLine.lineNumber,
    });
  }

  if (!baseImage) {
    throw new Error("Dockerfile must contain a FROM instruction");
  }

  return {
    dockerfilePath,
    contextDir,
    registeredName: registeredName ?? defaultRegisteredName(dockerfilePath),
    dockerfileText,
    baseImage,
    instructions,
  };
}

export async function loadDockerfilePlan(
  dockerfilePath: string,
  registeredName?: string,
): Promise<DockerfileBuildPlan> {
  const resolvedPath = path.resolve(dockerfilePath);
  const fileStats = await stat(resolvedPath).catch(() => null);
  if (!fileStats?.isFile()) {
    throw new Error(`Dockerfile not found: ${dockerfilePath}`);
  }

  const dockerfileText = await readFile(resolvedPath, "utf8");
  return buildPlanFromDockerfileText(
    dockerfileText,
    resolvedPath,
    path.dirname(resolvedPath),
    registeredName,
  );
}

export function loadImagePlan(
  image: Image,
  options: Pick<CreateSandboxImageOptions, "registeredName" | "contextDir"> = {},
): DockerfileBuildPlan {
  const contextDir = path.resolve(options.contextDir ?? process.cwd());
  const dockerfileText = dockerfileContent(image);
  const logicalLines = logicalDockerfileLines(dockerfileText);
  const instructions = image.baseImage == null ? logicalLines : logicalLines.slice(1);

  return {
    dockerfilePath: path.join(contextDir, "Dockerfile"),
    contextDir,
    registeredName: options.registeredName ?? image.name,
    dockerfileText,
    baseImage: image.baseImage ?? undefined,
    instructions: instructions.map(({ line, lineNumber }) => {
      const parsed = splitInstruction(line, lineNumber);
      return {
        keyword: parsed.keyword,
        value: parsed.value,
        lineNumber,
      };
    }),
  };
}

function ndjsonStdoutEmit(event: Record<string, unknown>) {
  process.stdout.write(`${JSON.stringify(event)}\n`);
}

function noopEmit(_event: Record<string, unknown>) {}

function stderrEmit(event: Record<string, unknown>) {
  const type = typeof event.type === "string" ? event.type : "";
  const message = typeof event.message === "string" ? event.message : "";
  if (type === "build_log") {
    const stream = typeof event.stream === "string" ? event.stream : "stdout";
    process.stderr.write(`[${stream}] ${message}\n`);
  } else if (message) {
    process.stderr.write(`[${type}] ${message}\n`);
  }
}

function debugEnabled(): boolean {
  return ["1", "true", "yes", "on"].includes(
    (process.env.TENSORLAKE_DEBUG ?? "").toLowerCase(),
  );
}

function buildContextFromEnv(): BuildContext {
  return {
    apiUrl: process.env.TENSORLAKE_API_URL ?? "https://api.tensorlake.ai",
    apiKey: process.env.TENSORLAKE_API_KEY,
    personalAccessToken: process.env.TENSORLAKE_PAT,
    namespace: process.env.INDEXIFY_NAMESPACE ?? "default",
    organizationId: process.env.TENSORLAKE_ORGANIZATION_ID,
    projectId: process.env.TENSORLAKE_PROJECT_ID,
    debug: debugEnabled(),
  };
}

function createDefaultClient(context: BuildContext): BuildClient {
  const useScopeHeaders = context.personalAccessToken != null && context.apiKey == null;
  return new SandboxClient({
    apiUrl: context.apiUrl,
    apiKey: context.apiKey ?? context.personalAccessToken,
    organizationId: useScopeHeaders ? context.organizationId : undefined,
    projectId: useScopeHeaders ? context.projectId : undefined,
    namespace: context.namespace,
  });
}

function baseApiUrl(context: Pick<BuildContext, "apiUrl">): string {
  return context.apiUrl.replace(/\/+$/, "");
}

function scopedBuildsPath(context: ResolvedBuildContext): string {
  return (
    `/platform/v1/organizations/${encodeURIComponent(context.organizationId)}` +
    `/projects/${encodeURIComponent(context.projectId)}/sandbox-template-builds`
  );
}

function platformHeaders(
  context: Pick<
    ResolvedBuildContext,
    "bearerToken" | "useScopeHeaders" | "organizationId" | "projectId"
  >,
): Record<string, string> {
  const headers: Record<string, string> = {
    Authorization: `Bearer ${context.bearerToken}`,
    "Content-Type": "application/json",
  };
  if (context.useScopeHeaders) {
    headers["X-Forwarded-Organization-Id"] = context.organizationId;
    headers["X-Forwarded-Project-Id"] = context.projectId;
  }
  return headers;
}

async function requestJson<T>(
  url: string,
  init: RequestInit,
  errorPrefix: string,
): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) {
    throw new Error(
      `${errorPrefix} (HTTP ${response.status}): ${await response.text()}`,
    );
  }

  const text = await response.text();
  return (text ? JSON.parse(text) : {}) as T;
}

async function resolveBuildContext(context: BuildContext): Promise<ResolvedBuildContext> {
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT.");
  }

  if (context.apiKey) {
    const scope = await requestJson<{
      organizationId: string;
      projectId: string;
    }>(
      `${baseApiUrl(context)}/platform/v1/keys/introspect`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${bearerToken}`,
          "Content-Type": "application/json",
        },
      },
      "API key introspection failed",
    );
    if (!scope.organizationId || !scope.projectId) {
      throw new Error("API key introspection response is missing organizationId or projectId");
    }
    return {
      ...context,
      bearerToken,
      organizationId: scope.organizationId,
      projectId: scope.projectId,
      useScopeHeaders: false,
    };
  }

  if (!context.organizationId || !context.projectId) {
    throw new Error(
      "Personal Access Token authentication requires " +
        "TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID to be set " +
        "(e.g. via 'tl login && tl init'). To skip this requirement, " +
        "authenticate with TENSORLAKE_API_KEY instead — API keys are " +
        "bound to a single project at creation.",
    );
  }

  return {
    ...context,
    bearerToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    useScopeHeaders: true,
  };
}

async function prepareRootfsBuild(
  context: ResolvedBuildContext,
  plan: DockerfileBuildPlan,
  isPublic: boolean,
): Promise<{ prepared: PreparedSandboxTemplateBuild; spec: Record<string, unknown> }> {
  if (!plan.baseImage) {
    throw new Error("Sandbox image builds require a Dockerfile FROM image or Image baseImage");
  }

  const spec = await requestJson<Record<string, unknown>>(
    `${baseApiUrl(context)}${scopedBuildsPath(context)}`,
    {
      method: "POST",
      headers: platformHeaders(context),
      body: JSON.stringify({
        name: plan.registeredName,
        dockerfile: plan.dockerfileText,
        baseImage: plan.baseImage,
        public: isPublic,
      }),
    },
    "failed to prepare sandbox image build",
  );

  return { prepared: parsePreparedBuild(spec), spec };
}

function parsePreparedBuild(raw: Record<string, unknown>): PreparedSandboxTemplateBuild {
  const builder = raw.builder as Record<string, unknown> | undefined;
  if (!builder) {
    throw new Error("platform API response is missing rootfs builder configuration");
  }
  const prepared: PreparedSandboxTemplateBuild = {
    ...raw,
    buildId: requiredString(raw, "buildId"),
    snapshotId: requiredString(raw, "snapshotId"),
    snapshotUri: requiredString(raw, "snapshotUri"),
    rootfsNodeKind: requiredString(raw, "rootfsNodeKind"),
    builder: {
      image: requiredString(builder, "image"),
      command: requiredString(builder, "command"),
      cpus: requiredNumber(builder, "cpus"),
      memoryMb: requiredNumber(builder, "memoryMb"),
      diskMb: requiredNumber(builder, "diskMb"),
    },
  };
  const parent = raw.parent as Record<string, unknown> | undefined;
  if (parent != null) {
    prepared.parent = {
      parentManifestUri: requiredString(parent, "parentManifestUri"),
      rootfsDiskBytes: optionalNumber(parent, "rootfsDiskBytes"),
    };
  }
  return prepared;
}

async function completeRootfsBuild(
  context: ResolvedBuildContext,
  buildId: string,
  request: CompleteSandboxTemplateBuildRequest,
): Promise<Record<string, unknown>> {
  return requestJson<Record<string, unknown>>(
    `${baseApiUrl(context)}${scopedBuildsPath(context)}/${encodeURIComponent(buildId)}/complete`,
    {
      method: "POST",
      headers: platformHeaders(context),
      body: JSON.stringify(request),
    },
    "failed to complete sandbox image build",
  );
}

async function resolvedDockerConfigJson(): Promise<string | undefined> {
  const configDir = process.env.DOCKER_CONFIG ?? path.join(homedir(), ".docker");
  const configPath = path.join(configDir, "config.json");
  try {
    const content = await readFile(configPath, "utf8");
    const parsed = JSON.parse(content) as Record<string, unknown>;
    const auths = parsed.auths as Record<string, unknown> | undefined;
    if (auths != null && Object.keys(auths).length > 0) {
      return JSON.stringify({ auths });
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    throw error;
  }
  return undefined;
}

function rootfsDiskBytes(
  diskMb: number | undefined,
  prepared: PreparedSandboxTemplateBuild,
): number {
  if (diskMb != null) {
    return diskMb * 1024 * 1024;
  }
  if (prepared.parent != null) {
    if (prepared.parent.rootfsDiskBytes == null) {
      throw new Error(
        "platform API did not return parent rootfsDiskBytes for diff build; pass diskMb explicitly or update Platform API",
      );
    }
    return prepared.parent.rootfsDiskBytes;
  }
  return DEFAULT_ROOTFS_DISK_MB * 1024 * 1024;
}

function rootfsDiskBytesToMb(bytes: number): number {
  return Math.ceil(bytes / (1024 * 1024));
}

async function buildRootfsSpec(
  preparedSpec: Record<string, unknown>,
  prepared: PreparedSandboxTemplateBuild,
  plan: DockerfileBuildPlan,
  diskMb: number | undefined,
): Promise<Record<string, unknown>> {
  const spec: Record<string, unknown> = {
    ...preparedSpec,
    dockerfile: plan.dockerfileText,
    contextDir: REMOTE_CONTEXT_DIR,
    baseImage: plan.baseImage,
    rootfsDiskBytes: rootfsDiskBytes(diskMb, prepared),
  };
  const dockerConfigJson = await resolvedDockerConfigJson();
  if (dockerConfigJson != null) {
    spec.dockerConfigJson = dockerConfigJson;
  }
  return spec;
}

function rootfsBuilderExecutable(executable: string): string {
  return executable === ROOTFS_BUILDER_COMMAND
    ? `${ROOTFS_BUILDER_BIN_DIR}/${ROOTFS_BUILDER_COMMAND}`
    : executable;
}

function rootfsBuilderEnv(): Record<string, string> {
  return { PATH: ROOTFS_BUILDER_PATH };
}

async function runRootfsBuilder(
  sandbox: BuildSandbox,
  command: string,
  emit: (event: Record<string, unknown>) => void,
  sleep: (ms: number) => Promise<void>,
) {
  const parts = shellSplit(command);
  const [executable, ...commandArgs] = parts;
  if (!executable) {
    throw new Error("empty rootfs builder command returned by platform API");
  }
  await runStreaming(
    sandbox,
    emit,
    sleep,
    rootfsBuilderExecutable(executable),
    [...commandArgs, "--spec", REMOTE_SPEC_PATH, "--metadata-out", REMOTE_METADATA_PATH],
    rootfsBuilderEnv(),
    REMOTE_BUILD_DIR,
  );
}

function metadataString(
  metadata: Record<string, unknown>,
  snakeKey: string,
  camelKey: string,
): string | undefined {
  const value = metadata[snakeKey] ?? metadata[camelKey];
  return typeof value === "string" ? value : undefined;
}

function metadataNumber(
  metadata: Record<string, unknown>,
  snakeKey: string,
  camelKey: string,
): number | undefined {
  const value = metadata[snakeKey] ?? metadata[camelKey];
  if (typeof value === "number") {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function completeRequestFromMetadata(
  prepared: PreparedSandboxTemplateBuild,
  metadata: Record<string, unknown>,
): CompleteSandboxTemplateBuildRequest {
  const rootfsNodeKind =
    metadataString(metadata, "rootfs_node_kind", "rootfsNodeKind") ??
    prepared.rootfsNodeKind;
  const parentManifestUri =
    metadataString(metadata, "parent_manifest_uri", "parentManifestUri") ??
    (rootfsNodeKind === "diff" ? prepared.parent?.parentManifestUri : undefined);
  if (rootfsNodeKind === "diff" && parentManifestUri == null) {
    throw new Error("rootfs diff build completed without parent_manifest_uri");
  }

  const snapshotFormatVersion = metadataString(
    metadata,
    "snapshot_format_version",
    "snapshotFormatVersion",
  );
  const snapshotSizeBytes = metadataNumber(
    metadata,
    "snapshot_size_bytes",
    "snapshotSizeBytes",
  );
  const rootfsDiskBytesValue = metadataNumber(
    metadata,
    "rootfs_disk_bytes",
    "rootfsDiskBytes",
  );
  if (!snapshotFormatVersion) {
    throw new Error("rootfs builder metadata is missing snapshot_format_version");
  }
  if (snapshotSizeBytes == null) {
    throw new Error("rootfs builder metadata is missing numeric snapshot_size_bytes");
  }
  if (rootfsDiskBytesValue == null) {
    throw new Error("rootfs builder metadata is missing numeric rootfs_disk_bytes");
  }

  return {
    snapshotId:
      metadataString(metadata, "snapshot_id", "snapshotId") ?? prepared.snapshotId,
    snapshotUri:
      metadataString(metadata, "snapshot_uri", "snapshotUri") ?? prepared.snapshotUri,
    snapshotFormatVersion,
    snapshotSizeBytes,
    rootfsDiskBytes: rootfsDiskBytesValue,
    rootfsNodeKind,
    ...(parentManifestUri ? { parentManifestUri } : {}),
  };
}

function requiredString(object: Record<string, unknown>, key: string): string {
  const value = object[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`expected '${key}' to be a non-empty string`);
  }
  return value;
}

function requiredNumber(object: Record<string, unknown>, key: string): number {
  const value = object[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`expected '${key}' to be a finite number`);
  }
  return value;
}

function optionalNumber(
  object: Record<string, unknown>,
  key: string,
): number | undefined {
  const value = object[key];
  if (value == null) {
    return undefined;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`expected '${key}' to be a finite number`);
  }
  return value;
}

async function runChecked(
  sandbox: BuildSandbox,
  command: string,
  args: string[],
  env?: Record<string, string>,
  workingDir?: string,
): Promise<CommandResult> {
  const result = await sandbox.run(command, {
    args,
    env,
    workingDir,
  });
  if (result.exitCode !== 0) {
    throw new Error(
      `Command '${command} ${args.join(" ")}' failed with exit code ${result.exitCode}`,
    );
  }
  return result;
}

async function runStreaming(
  sandbox: BuildSandbox,
  emit: (event: Record<string, unknown>) => void,
  sleep: (ms: number) => Promise<void>,
  command: string,
  args: string[] = [],
  env?: Record<string, string>,
  workingDir?: string,
) {
  const proc = await sandbox.startProcess(command, {
    args,
    env,
    workingDir,
  });

  let stdoutSeen = 0;
  let stderrSeen = 0;
  let info: ProcessInfo;

  while (true) {
    const stdoutResp = await sandbox.getStdout(proc.pid);
    emitOutputLines(emit, "stdout", stdoutResp, stdoutSeen);
    stdoutSeen = stdoutResp.lines.length;

    const stderrResp = await sandbox.getStderr(proc.pid);
    emitOutputLines(emit, "stderr", stderrResp, stderrSeen);
    stderrSeen = stderrResp.lines.length;

    info = await sandbox.getProcess(proc.pid);
    if (info.status !== ProcessStatus.RUNNING) {
      const finalStdout = await sandbox.getStdout(proc.pid);
      emitOutputLines(emit, "stdout", finalStdout, stdoutSeen);
      stdoutSeen = finalStdout.lines.length;

      const finalStderr = await sandbox.getStderr(proc.pid);
      emitOutputLines(emit, "stderr", finalStderr, stderrSeen);
      break;
    }

    await sleep(300);
  }

  for (let i = 0; i < 10; i++) {
    if (info.exitCode != null || info.signal != null) {
      break;
    }
    await sleep(200);
    info = await sandbox.getProcess(proc.pid);
  }

  const exitCode =
    info.exitCode != null ? info.exitCode : info.signal != null ? -info.signal : 0;
  if (exitCode !== 0) {
    throw new Error(
      `Command '${command} ${args.join(" ")}' failed with exit code ${exitCode}`,
    );
  }
}

function emitOutputLines(
  emit: (event: Record<string, unknown>) => void,
  stream: "stdout" | "stderr",
  response: OutputResponse,
  seen: number,
) {
  for (const line of response.lines.slice(seen)) {
    emit({ type: "build_log", stream, message: line });
  }
}

async function copyLocalPathToSandbox(
  sandbox: BuildSandbox,
  localPath: string,
  remotePath: string,
) {
  const fileStats = await stat(localPath).catch(() => null);
  if (!fileStats) {
    throw new Error(`Local path not found: ${localPath}`);
  }

  if (fileStats.isFile()) {
    await runChecked(sandbox, "mkdir", ["-p", path.posix.dirname(remotePath)]);
    await sandbox.writeFile(remotePath, await readFile(localPath));
    return;
  }

  if (!fileStats.isDirectory()) {
    throw new Error(`Local path not found: ${localPath}`);
  }

  const entries = await readdir(localPath, { withFileTypes: true });
  for (const entry of entries) {
    const sourcePath = path.join(localPath, entry.name);
    const destinationPath = path.posix.join(remotePath, entry.name);
    if (entry.isDirectory()) {
      await runChecked(sandbox, "mkdir", ["-p", destinationPath]);
      await copyLocalPathToSandbox(sandbox, sourcePath, destinationPath);
    } else if (entry.isFile()) {
      await runChecked(
        sandbox,
        "mkdir",
        ["-p", path.posix.dirname(destinationPath)],
      );
      await sandbox.writeFile(destinationPath, await readFile(sourcePath));
    }
  }
}

export async function registerImage(
  context: BuildContext,
  name: string,
  dockerfile: string,
  snapshotId: string,
  snapshotSandboxId: string,
  snapshotUri: string,
  snapshotSizeBytes: number,
  rootfsDiskBytes: number,
  isPublic: boolean,
  snapshotFormatVersion?: string,
): Promise<Record<string, unknown>> {
  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT.");
  }

  const baseUrl = context.apiUrl.replace(/\/+$/, "");

  // API key auth: platform-api resolves org/project from the key itself, so
  // we hit the scope-less route and skip the env var requirement.
  // PAT auth isn't project-scoped — keep the explicit IDs and X-Forwarded
  // headers for that path.
  const headers: Record<string, string> = {
    Authorization: `Bearer ${bearerToken}`,
    "Content-Type": "application/json",
  };
  let url: string;
  if (context.apiKey) {
    url = `${baseUrl}/platform/v1/sandbox-templates`;
  } else {
    if (!context.organizationId || !context.projectId) {
      throw new Error(
        "Personal Access Token authentication requires " +
          "TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID to be set " +
          "(e.g. via 'tl login && tl init'). To skip this requirement, " +
          "authenticate with TENSORLAKE_API_KEY instead — API keys are " +
          "bound to a single project at creation.",
      );
    }
    url =
      `${baseUrl}/platform/v1/organizations/` +
      `${encodeURIComponent(context.organizationId)}/projects/` +
      `${encodeURIComponent(context.projectId)}/sandbox-templates`;
    headers["X-Forwarded-Organization-Id"] = context.organizationId;
    headers["X-Forwarded-Project-Id"] = context.projectId;
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({
      name,
      dockerfile,
      snapshotId,
      snapshotSandboxId,
      snapshotUri,
      ...(snapshotFormatVersion ? { snapshotFormatVersion } : {}),
      snapshotSizeBytes,
      rootfsDiskBytes,
      public: isPublic,
    }),
  });

  if (!response.ok) {
    throw new Error(
      `${response.status} ${response.statusText}: ${await response.text()}`,
    );
  }

  const text = await response.text();
  return text ? (JSON.parse(text) as Record<string, unknown>) : {};
}

export async function createSandboxImage(
  source: SandboxImageSource,
  options: CreateSandboxImageOptions = {},
  deps: CreateSandboxImageDeps = {},
) {
  const emit = deps.emit ?? (options.verbose ? stderrEmit : noopEmit);
  const sleep = deps.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const context = buildContextFromEnv();
  const clientFactory = deps.createClient ?? createDefaultClient;

  const sourceLabel =
    typeof source === "string" ? source : `Image(${source.name})`;
  emit({ type: "status", message: `Loading ${sourceLabel}...` });
  const plan =
    typeof source === "string"
      ? await loadDockerfilePlan(source, options.registeredName)
      : loadImagePlan(source, options);
  emit({
    type: "status",
    message: `Selected image name: ${plan.registeredName}`,
  });

  emit({ type: "status", message: "Preparing rootfs build..." });
  const resolvedContext = await resolveBuildContext(context);
  const { prepared, spec: preparedSpec } = await prepareRootfsBuild(
    resolvedContext,
    plan,
    options.isPublic ?? false,
  );
  emit({
    type: "status",
    message:
      prepared.rootfsNodeKind === "diff"
        ? "Build mode: RootfsDiff"
        : "Build mode: RootfsBase",
  });

  const client = clientFactory(context);
  let sandbox: BuildSandbox | undefined;

  try {
    const outputRootfsDiskBytes = rootfsDiskBytes(options.diskMb, prepared);
    const builderDiskMb = Math.max(
      rootfsDiskBytesToMb(outputRootfsDiskBytes),
      options.builderDiskMb ?? prepared.builder.diskMb,
    );

    emit({
      type: "status",
      message: `Creating rootfs builder sandbox from ${prepared.builder.image}...`,
    });
    sandbox = await client.createAndConnect({
      image: prepared.builder.image,
      cpus: options.cpus ?? prepared.builder.cpus,
      memoryMb: options.memoryMb ?? prepared.builder.memoryMb,
      diskMb: builderDiskMb,
    });
    emit({
      type: "status",
      message: `Rootfs builder sandbox ${sandbox.sandboxId} is running`,
    });
    emit({ type: "status", message: "Uploading build context..." });
    await copyLocalPathToSandbox(sandbox, plan.contextDir, REMOTE_CONTEXT_DIR);
    const spec = await buildRootfsSpec(
      preparedSpec,
      prepared,
      plan,
      options.diskMb,
    );
    await runChecked(sandbox, "mkdir", ["-p", path.posix.dirname(REMOTE_SPEC_PATH)]);
    await sandbox.writeFile(
      REMOTE_SPEC_PATH,
      new TextEncoder().encode(JSON.stringify(spec, null, 2)),
    );

    emit({ type: "status", message: "Running offline rootfs builder..." });
    await runRootfsBuilder(sandbox, prepared.builder.command, emit, sleep);

    const metadataBytes = await sandbox.readFile(REMOTE_METADATA_PATH);
    const metadata = JSON.parse(
      new TextDecoder().decode(metadataBytes),
    ) as Record<string, unknown>;
    const completeRequest = completeRequestFromMetadata(prepared, metadata);

    emit({ type: "status", message: "Completing image registration..." });
    const result = await completeRootfsBuild(
      resolvedContext,
      prepared.buildId,
      completeRequest,
    );

    emit({
      type: "image_registered",
      name: plan.registeredName,
      image_id:
        (typeof result.id === "string" && result.id) ||
        (typeof result.templateId === "string" && result.templateId) ||
        "",
    });
    emit({ type: "done" });
    return result;
  } finally {
    if (sandbox) {
      try {
        await sandbox.terminate();
      } catch {}
    }
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
      public: { type: "boolean", default: false },
    },
  });

  const dockerfilePath = parsed.positionals[0];
  if (!dockerfilePath) {
    throw new Error("Usage: tensorlake-create-sandbox-image <dockerfile_path> [--name NAME] [--cpus N] [--memory MB] [--disk_mb MB] [--builder_disk_mb MB] [--public]");
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
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit },
  );
}
