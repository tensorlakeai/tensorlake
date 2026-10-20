import { enumValue } from "./enum.js";

export enum ImageBuildStatus {
  PENDING = "pending",
  RUNNING = "running",
  SUCCEEDED = "succeeded",
  FAILED = "failed",
  CANCELLED = "cancelled",
  UNKNOWN = "unknown",
}

export enum ImageBuildPhase {
  AWAITING_CONTEXT = "awaiting_context",
  STARTING_BUILDER = "starting_builder",
  EXECUTING = "executing",
  SNAPSHOTTING = "snapshotting",
  PUBLISHING = "publishing",
  FINISHED = "finished",
  UNKNOWN = "unknown",
}

export enum ImageBuildKind {
  DOCKERFILE = "dockerfile",
  IMPORT = "import",
  DIRECT = "direct",
  UNKNOWN = "unknown",
}

export enum ImageBuildCancellationOutcome {
  CANCELLED = "cancelled",
  ALREADY_CANCELLED = "already_cancelled",
  ALREADY_COMPLETED = "already_completed",
  ALREADY_FAILED = "already_failed",
  NOT_ADMITTED = "not_admitted",
  UNCONFIRMED = "unconfirmed",
  UNKNOWN = "unknown",
}

export function isConfirmedCancellation(outcome: ImageBuildCancellationOutcome): boolean {
  return outcome === ImageBuildCancellationOutcome.CANCELLED ||
    outcome === ImageBuildCancellationOutcome.ALREADY_CANCELLED ||
    outcome === ImageBuildCancellationOutcome.ALREADY_FAILED;
}

export enum ImageBuildErrorCode {
  USAGE = "usage",
  AUTH = "auth",
  NOT_FOUND = "not_found",
  IMAGE_BUILD_IN_PROGRESS = "image_build_in_progress",
  CLIENT_REQUEST_ID_MISMATCH = "client_request_id_mismatch",
  FAILED = "failed",
  CANCELLED = "cancelled",
  TIMEOUT = "timeout",
  INTERRUPTED = "interrupted",
  SERVICE = "service",
  INTERNAL = "internal",
  UNKNOWN = "unknown",
}

export enum ImageBuildEventType {
  STATUS = "status",
  BUILD_LOG = "build_log",
  WARNING = "warning",
  BUILD_ACCEPTED = "build_accepted",
  BUILD_PROGRESS = "build_progress",
  BUILD_TERMINAL = "build_terminal",
  INTERRUPTED = "interrupted",
  IMAGE_REGISTERED = "image_registered",
  DONE = "done",
  UNKNOWN = "unknown",
}

export function normalizeBuildEvent(event: Record<string, unknown>): Record<string, unknown> {
  const result: Record<string, unknown> = { ...event, raw: { ...event } };
  if ("type" in event) result.type = enumValue(ImageBuildEventType, event.type);
  if (event.status != null) result.status = enumValue(ImageBuildStatus, event.status);
  if (event.phase != null) result.phase = enumValue(ImageBuildPhase, event.phase);
  if (event.outcome != null) {
    const outcome = enumValue(ImageBuildCancellationOutcome, event.outcome);
    result.outcome = outcome;
  }
  if ("confirmed" in event) {
    result.confirmed = event.confirmed === true &&
      isConfirmedCancellation(result.outcome as ImageBuildCancellationOutcome);
  }
  return result;
}
