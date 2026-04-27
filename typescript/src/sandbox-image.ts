import { readFile, readdir, stat } from "node:fs/promises";
import path from "node:path";
import { parseArgs } from "node:util";
import {
  type CommandResult,
  type ProcessInfo,
  type SnapshotContentMode,
  type SnapshotInfo,
} from "./models.js";
import { type OutputResponse, ProcessStatus } from "./models.js";
import { SandboxClient } from "./client.js";
import { Image, dockerfileContent } from "./image.js";

const BUILD_SANDBOX_PIP_ENV = { PIP_BREAK_SYSTEM_PACKAGES: "1" } as const;
const IGNORED_DOCKERFILE_INSTRUCTIONS = new Set([
  "CMD",
  "ENTRYPOINT",
  "EXPOSE",
  "HEALTHCHECK",
  "LABEL",
  "STOPSIGNAL",
  "VOLUME",
]);
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

interface BuildContext {
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
  terminate(): Promise<void>;
}

interface BuildClient {
  createAndConnect(options: {
    image?: string;
    cpus?: number;
    memoryMb?: number;
    diskMb?: number;
  }): Promise<BuildSandbox>;
  snapshotAndWait(
    sandboxId: string,
    options?: {
      timeout?: number;
      pollInterval?: number;
      contentMode?: SnapshotContentMode;
    },
  ): Promise<SnapshotInfo>;
  close(): void;
}

interface CreateSandboxImageDeps {
  emit?: (event: Record<string, unknown>) => void;
  createClient?: (context: BuildContext) => BuildClient;
  registerImage?: (
    context: BuildContext,
    name: string,
    dockerfile: string,
    snapshotId: string,
    snapshotSandboxId: string,
    snapshotUri: string,
    snapshotSizeBytes: number,
    rootfsDiskBytes: number,
    isPublic: boolean,
  ) => Promise<Record<string, unknown>>;
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

function shellQuote(value: string): string {
  if (!value) {
    return "''";
  }
  return `'${value.replace(/'/g, `'\\''`)}'`;
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

function parseCopyLikeValues(
  value: string,
  lineNumber: number,
  keyword: string,
): {
  flags: Record<string, string>;
  sources: string[];
  destination: string;
} {
  const { flags, remaining } = stripLeadingFlags(value);
  if ("from" in flags) {
    throw new Error(
      `line ${lineNumber}: ${keyword} --from is not supported for sandbox image creation`,
    );
  }

  const payload = remaining.trim();
  if (!payload) {
    throw new Error(
      `line ${lineNumber}: ${keyword} must include source and destination`,
    );
  }

  let parts: string[];
  if (payload.startsWith("[")) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(payload);
    } catch (error) {
      throw new Error(
        `line ${lineNumber}: invalid JSON array syntax for ${keyword}: ${(error as Error).message}`,
      );
    }
    if (
      !Array.isArray(parsed) ||
      parsed.length < 2 ||
      parsed.some((item) => typeof item !== "string")
    ) {
      throw new Error(
        `line ${lineNumber}: ${keyword} JSON array form requires at least two string values`,
      );
    }
    parts = parsed as string[];
  } else {
    parts = shellSplit(payload);
    if (parts.length < 2) {
      throw new Error(
        `line ${lineNumber}: ${keyword} must include at least one source and one destination`,
      );
    }
  }

  return {
    flags,
    sources: parts.slice(0, -1),
    destination: parts[parts.length - 1],
  };
}

function parseEnvPairs(value: string, lineNumber: number): Array<[string, string]> {
  const tokens = shellSplit(value);
  if (tokens.length === 0) {
    throw new Error(`line ${lineNumber}: ENV must include a key and value`);
  }

  if (tokens.every((token) => token.includes("="))) {
    return tokens.map((token) => {
      const [key, envValue] = token.split(/=(.*)/s, 2);
      if (!key) {
        throw new Error(`line ${lineNumber}: invalid ENV token '${token}'`);
      }
      return [key, envValue] as [string, string];
    });
  }

  if (tokens.length < 2) {
    throw new Error(`line ${lineNumber}: ENV must include a key and value`);
  }

  return [[tokens[0], tokens.slice(1).join(" ")]];
}

function resolveContainerPath(containerPath: string, workingDir: string): string {
  if (!containerPath) {
    return workingDir;
  }
  const normalized = containerPath.startsWith("/")
    ? path.posix.normalize(containerPath)
    : path.posix.normalize(path.posix.join(workingDir, containerPath));
  return normalized.startsWith("/") ? normalized : `/${normalized}`;
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
  return new SandboxClient({
    apiUrl: context.apiUrl,
    apiKey: context.apiKey ?? context.personalAccessToken,
    organizationId: context.organizationId,
    projectId: context.projectId,
    namespace: context.namespace,
  });
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

function isPathWithinContext(contextDir: string, localPath: string): boolean {
  const relative = path.relative(contextDir, localPath);
  return relative === "" || (!relative.startsWith("..") && !path.isAbsolute(relative));
}

function resolveContextSourcePath(contextDir: string, source: string): string {
  const resolvedContextDir = path.resolve(contextDir);
  const resolvedSource = path.resolve(resolvedContextDir, source);
  if (!isPathWithinContext(resolvedContextDir, resolvedSource)) {
    throw new Error(`Local path escapes the build context: ${source}`);
  }
  return resolvedSource;
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

async function persistEnvVar(
  sandbox: BuildSandbox,
  processEnv: Record<string, string>,
  key: string,
  value: string,
) {
  const exportLine = `export ${key}=${shellQuote(value)}`;
  await runChecked(
    sandbox,
    "sh",
    ["-c", `printf '%s\\n' ${shellQuote(exportLine)} >> /etc/environment`],
    processEnv,
  );
}

async function copyFromContext(
  sandbox: BuildSandbox,
  emit: (event: Record<string, unknown>) => void,
  contextDir: string,
  sources: string[],
  destination: string,
  workingDir: string,
  keyword: string,
) {
  const destinationPath = resolveContainerPath(destination, workingDir);
  if (sources.length > 1 && !destinationPath.endsWith("/")) {
    throw new Error(
      `${keyword} with multiple sources requires a directory destination ending in '/'`,
    );
  }

  for (const source of sources) {
    const localSource = resolveContextSourcePath(contextDir, source);
    const localStats = await stat(localSource).catch(() => null);
    if (!localStats) {
      throw new Error(`Local path not found: ${localSource}`);
    }

    let remoteDestination = destinationPath;
    if (sources.length > 1) {
      remoteDestination = path.posix.join(
        destinationPath.replace(/\/$/, ""),
        path.posix.basename(source.replace(/\/$/, "")),
      );
    } else if (localStats.isFile() && destinationPath.endsWith("/")) {
      remoteDestination = path.posix.join(
        destinationPath.replace(/\/$/, ""),
        path.basename(source),
      );
    }

    emit({
      type: "status",
      message: `${keyword} ${source} -> ${remoteDestination}`,
    });
    await copyLocalPathToSandbox(sandbox, localSource, remoteDestination);
  }
}

async function addUrlToSandbox(
  sandbox: BuildSandbox,
  emit: (event: Record<string, unknown>) => void,
  url: string,
  destination: string,
  workingDir: string,
  processEnv: Record<string, string>,
  sleep: (ms: number) => Promise<void>,
) {
  let destinationPath = resolveContainerPath(destination, workingDir);
  const parsedUrl = new URL(url);
  const fileName = path.posix.basename(parsedUrl.pathname.replace(/\/$/, "")) || "downloaded";
  if (destinationPath.endsWith("/")) {
    destinationPath = path.posix.join(destinationPath.replace(/\/$/, ""), fileName);
  }

  const parentDir = path.posix.dirname(destinationPath) || "/";
  emit({
    type: "status",
    message: `ADD ${url} -> ${destinationPath}`,
  });
  await runChecked(sandbox, "mkdir", ["-p", parentDir], processEnv);
  await runStreaming(
    sandbox,
    emit,
    sleep,
    "sh",
    [
      "-c",
      `curl -fsSL --location ${shellQuote(url)} -o ${shellQuote(destinationPath)}`,
    ],
    processEnv,
    workingDir,
  );
}

async function executeDockerfilePlan(
  sandbox: BuildSandbox,
  plan: DockerfileBuildPlan,
  emit: (event: Record<string, unknown>) => void,
  sleep: (ms: number) => Promise<void>,
) {
  const processEnv: Record<string, string> = { ...BUILD_SANDBOX_PIP_ENV };
  let workingDir = "/";

  for (const instruction of plan.instructions) {
    const { keyword, value, lineNumber } = instruction;

    if (keyword === "RUN") {
      emit({ type: "status", message: `RUN ${value}` });
      await runStreaming(
        sandbox,
        emit,
        sleep,
        "sh",
        ["-c", value],
        processEnv,
        workingDir,
      );
      continue;
    }

    if (keyword === "WORKDIR") {
      const tokens = shellSplit(value);
      if (tokens.length !== 1) {
        throw new Error(`line ${lineNumber}: WORKDIR must include exactly one path`);
      }
      workingDir = resolveContainerPath(tokens[0], workingDir);
      emit({ type: "status", message: `WORKDIR ${workingDir}` });
      await runChecked(sandbox, "mkdir", ["-p", workingDir], processEnv);
      continue;
    }

    if (keyword === "ENV") {
      for (const [key, envValue] of parseEnvPairs(value, lineNumber)) {
        emit({ type: "status", message: `ENV ${key}=${envValue}` });
        processEnv[key] = envValue;
        await persistEnvVar(sandbox, processEnv, key, envValue);
      }
      continue;
    }

    if (keyword === "COPY") {
      const { sources, destination } = parseCopyLikeValues(
        value,
        lineNumber,
        keyword,
      );
      await copyFromContext(
        sandbox,
        emit,
        plan.contextDir,
        sources,
        destination,
        workingDir,
        keyword,
      );
      continue;
    }

    if (keyword === "ADD") {
      const { sources, destination } = parseCopyLikeValues(
        value,
        lineNumber,
        keyword,
      );
      if (
        sources.length === 1 &&
        /^https?:\/\//.test(sources[0])
      ) {
        await addUrlToSandbox(
          sandbox,
          emit,
          sources[0],
          destination,
          workingDir,
          processEnv,
          sleep,
        );
      } else {
        await copyFromContext(
          sandbox,
          emit,
          plan.contextDir,
          sources,
          destination,
          workingDir,
          keyword,
        );
      }
      continue;
    }

    if (IGNORED_DOCKERFILE_INSTRUCTIONS.has(keyword)) {
      emit({
        type: "warning",
        message: `Skipping Dockerfile instruction '${keyword}' during snapshot materialization. It is still preserved in the registered Dockerfile.`,
      });
      continue;
    }

    throw new Error(
      `line ${lineNumber}: Dockerfile instruction '${keyword}' is not supported for sandbox image creation`,
    );
  }
}

async function registerImage(
  context: BuildContext,
  name: string,
  dockerfile: string,
  snapshotId: string,
  snapshotSandboxId: string,
  snapshotUri: string,
  snapshotSizeBytes: number,
  rootfsDiskBytes: number,
  isPublic: boolean,
): Promise<Record<string, unknown>> {
  if (!context.organizationId || !context.projectId) {
    throw new Error(
      "Organization ID and Project ID are required. Run 'tl login' and 'tl init'.",
    );
  }

  const bearerToken = context.apiKey ?? context.personalAccessToken;
  if (!bearerToken) {
    throw new Error("Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT.");
  }

  const baseUrl = context.apiUrl.replace(/\/+$/, "");
  const url =
    `${baseUrl}/platform/v1/organizations/` +
    `${encodeURIComponent(context.organizationId)}/projects/` +
    `${encodeURIComponent(context.projectId)}/sandbox-templates`;

  const headers: Record<string, string> = {
    Authorization: `Bearer ${bearerToken}`,
    "Content-Type": "application/json",
  };
  if (context.personalAccessToken && !context.apiKey) {
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
  const register =
    deps.registerImage ??
    ((...args) => registerImage(...args));

  const sourceLabel =
    typeof source === "string" ? source : `Image(${source.name})`;
  emit({ type: "status", message: `Loading ${sourceLabel}...` });
  const plan =
    typeof source === "string"
      ? await loadDockerfilePlan(source, options.registeredName)
      : loadImagePlan(source, options);
  emit({
    type: "status",
    message:
      plan.baseImage == null
        ? "Starting build sandbox with the default server image..."
        : `Starting build sandbox from ${plan.baseImage}...`,
  });

  const client = clientFactory(context);
  let sandbox: BuildSandbox | undefined;

  try {
    sandbox = await client.createAndConnect({
      ...(plan.baseImage == null ? {} : { image: plan.baseImage }),
      cpus: options.cpus ?? 2.0,
      memoryMb: options.memoryMb ?? 4096,
      ...(options.diskMb != null ? { diskMb: options.diskMb } : {}),
    });

    emit({
      type: "status",
      message: `Materializing image in sandbox ${sandbox.sandboxId}...`,
    });
    await executeDockerfilePlan(sandbox, plan, emit, sleep);

    emit({ type: "status", message: "Creating snapshot..." });
    const snapshot = await client.snapshotAndWait(sandbox.sandboxId, {
      contentMode: "filesystem_only",
    });
    emit({
      type: "snapshot_created",
      snapshot_id: snapshot.snapshotId,
    });

    if (!snapshot.snapshotUri) {
      throw new Error(
        `Snapshot ${snapshot.snapshotId} is missing snapshotUri and cannot be registered as a sandbox image.`,
      );
    }
    if (snapshot.sizeBytes == null) {
      throw new Error(
        `Snapshot ${snapshot.snapshotId} is missing sizeBytes and cannot be registered as a sandbox image.`,
      );
    }
    if (snapshot.rootfsDiskBytes == null) {
      throw new Error(
        `Snapshot ${snapshot.snapshotId} is missing rootfsDiskBytes and cannot be registered as a sandbox image.`,
      );
    }

    emit({
      type: "status",
      message: `Registering image '${plan.registeredName}'...`,
    });
    const result = await register(
      context,
      plan.registeredName,
      plan.dockerfileText,
      snapshot.snapshotId,
      snapshot.sandboxId,
      snapshot.snapshotUri,
      snapshot.sizeBytes,
      snapshot.rootfsDiskBytes,
      options.isPublic ?? false,
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
      disk: { type: "string" },
      public: { type: "boolean", default: false },
    },
  });

  const dockerfilePath = parsed.positionals[0];
  if (!dockerfilePath) {
    throw new Error("Usage: tensorlake-create-sandbox-image <dockerfile_path> [--name NAME] [--cpus N] [--memory MB] [--disk GB] [--public]");
  }

  const cpus =
    parsed.values.cpus != null ? Number(parsed.values.cpus) : undefined;
  const memoryMb =
    parsed.values.memory != null ? Number(parsed.values.memory) : undefined;
  const diskGb =
    parsed.values.disk != null ? Number(parsed.values.disk) : undefined;
  if (cpus != null && !Number.isFinite(cpus)) {
    throw new Error(`Invalid --cpus value: ${parsed.values.cpus}`);
  }
  if (memoryMb != null && !Number.isInteger(memoryMb)) {
    throw new Error(`Invalid --memory value: ${parsed.values.memory}`);
  }
  if (diskGb != null && !Number.isInteger(diskGb)) {
    throw new Error(`Invalid --disk value: ${parsed.values.disk}`);
  }

  await createSandboxImage(
    dockerfilePath,
    {
      registeredName: parsed.values.name,
      cpus,
      memoryMb,
      diskMb: diskGb != null ? diskGb * 1024 : undefined,
      isPublic: parsed.values.public,
    },
    { emit: ndjsonStdoutEmit },
  );
}
