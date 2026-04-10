// Public API
export { SandboxClient } from "./client.js";
export { Sandbox } from "./sandbox.js";
export { Pty } from "./sandbox.js";
export { Desktop } from "./desktop.js";
export { TcpTunnel } from "./tunnel.js";
export { CloudClient } from "./cloud-client.js";
export { APIClient } from "./api-client.js";
export { createSandboxImage } from "./sandbox-image.js";
export { Image, dockerfileContent, ImageBuildOperationType } from "./image.js";

export type {
  PtyDataHandler,
  PtyExitHandler,
  PtyConnectionOptions,
  CreatePtyOptions,
} from "./sandbox.js";
export type {
  ConnectDesktopOptions,
  DesktopDoubleClickOptions,
  DesktopPointerOptions,
  MouseButton,
} from "./desktop.js";
export type {
  CreateTunnelOptions,
  TunnelAddress,
} from "./tunnel.js";

// Errors
export {
  SandboxException,
  SandboxError,
  SandboxConnectionError,
  SandboxNotFoundError,
  PoolNotFoundError,
  PoolInUseError,
  RemoteAPIError,
  RequestNotFinishedError,
  RequestFailedError,
  RequestExecutionError,
} from "./errors.js";

// Models & enums
export {
  SandboxStatus,
  SnapshotStatus,
  ProcessStatus,
  StdinMode,
  OutputMode,
  ContainerState,
} from "./models.js";

export type { SnapshotContentMode, SnapshotOptions } from "./models.js";

export type {
  BinaryPayload,
  CloudClientOptions,
  RequestInput,
  ApplicationSummary,
  ApplicationManifest,
  RequestErrorInfo,
  RequestMetadata,
  RequestOutput,
  ApiKeyIntrospection,
  NewSecret,
  Secret,
  SecretsPagination,
  SecretsList,
  UpsertSecretResponse,
  BuildInfo,
  BuildLogEntry,
  StartImageBuildRequest,
  CreateApplicationBuildImageRequest,
  CreateApplicationBuildRequest,
  ApplicationBuildContext,
  ApplicationBuildImageResult,
  ApplicationBuildResponse,
} from "./cloud-models.js";

export type {
  ContainerResourcesInfo,
  NetworkConfig,
  CreateSandboxOptions,
  CreateSandboxResponse,
  SandboxInfo,
  SandboxPortAccess,
  CreateSnapshotResponse,
  SnapshotInfo,
  SnapshotAndWaitOptions,
  CreatePoolOptions,
  UpdatePoolOptions,
  CreateSandboxPoolResponse,
  PoolContainerInfo,
  SandboxPoolInfo,
  StartProcessOptions,
  ProcessInfo,
  SendSignalResponse,
  OutputResponse,
  OutputEvent,
  RunOptions,
  CommandResult,
  DirectoryEntry,
  ListDirectoryResponse,
  CreatePtySessionOptions,
  PtySessionInfo,
  HealthResponse,
  DaemonInfo,
  SandboxClientOptions,
  SandboxOptions,
  CreateAndConnectOptions,
} from "./models.js";

export type {
  DockerfileInstruction,
  DockerfileBuildPlan,
  CreateSandboxImageOptions,
  SandboxImageSource,
} from "./sandbox-image.js";

export type {
  ImageOptions,
  ImageBuildOperation,
  ImageBuildOperationType as ImageBuildOperationTypeValue,
} from "./image.js";
