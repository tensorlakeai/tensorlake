/* tslint:disable */
/* eslint-disable */

/**
 * Generated-style type declarations for the Tensorlake native bindings.
 * Schema mirrors the napi-rs `#[napi(object)]` structs in `src/lib.rs`;
 * keep in sync when the Rust signatures change.
 */

export interface SandboxImageBuildOptionsJs {
  apiUrl: string
  bearerToken: string
  dockerfilePath: string
  registeredName?: string | undefined | null
  /** Root disk size for the generated sandbox image in MB. */
  diskMb?: number | undefined | null
  builderDiskMb?: number | undefined | null
  cpus?: number | undefined | null
  memoryMb?: number | undefined | null
  isPublic?: boolean | undefined | null
  organizationId?: string | undefined | null
  projectId?: string | undefined | null
  namespace?: string | undefined | null
  useScopeHeaders?: boolean | undefined | null
  userAgent?: string | undefined | null
  dockerfileText?: string | undefined | null
  contextDir?: string | undefined | null
}

export interface SandboxImageBuildEventJs {
  /** `"status" | "build_log" | "warning"` */
  eventType: string
  /** Only set for `build_log` events. */
  stream?: string | undefined | null
  message: string
}

/**
 * Build a sandbox image. The returned string is JSON-encoded; the caller is
 * expected to parse it. `emit`, if provided, is invoked for each progress
 * event.
 */
export function buildSandboxImage(
  options: SandboxImageBuildOptionsJs,
  emit?: ((event: SandboxImageBuildEventJs) => void) | undefined | null,
): Promise<string>
