"""Pydantic models for sandbox operations."""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated, Any
from urllib.parse import urlparse, urlunparse

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

_SANDBOX_MANAGEMENT_PORT = 9501


def sandbox_url_from_ingress_endpoint(
    ingress_endpoint: str,
    sandbox_id: str,
    port: int | None = None,
) -> str:
    """Build a public sandbox URL from a server-provided ingress endpoint.

    ``port`` omitted or ``9501`` returns the sandbox management URL:
    ``https://<sandbox-id>.<ingress-host>``. User ports use the public
    ``<port>-<sandbox-id>.<ingress-host>`` form.
    """

    parsed = urlparse(ingress_endpoint)
    if parsed.scheme not in ("http", "https") or parsed.hostname is None:
        raise ValueError("ingress_endpoint must be an absolute http(s) URL")

    label = (
        sandbox_id
        if port is None or port == _SANDBOX_MANAGEMENT_PORT
        else f"{port}-{sandbox_id}"
    )
    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    netloc = f"{label}.{host}"
    if parsed.port is not None:
        netloc = f"{netloc}:{parsed.port}"
    return urlunparse((parsed.scheme, netloc, "", "", "", ""))


def _parse_timestamp(v: int | float | datetime | None) -> datetime | None:
    """Convert a numeric timestamp to a UTC datetime.

    Handles seconds, milliseconds, and microseconds by checking magnitude.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    ts = float(v)
    if ts > 1e15:
        # Microseconds
        ts = ts / 1_000_000
    elif ts > 1e12:
        # Milliseconds
        ts = ts / 1_000
    return datetime.fromtimestamp(ts, tz=timezone.utc)


OptionalTimestamp = Annotated[datetime | None, BeforeValidator(_parse_timestamp)]
Timestamp = Annotated[datetime, BeforeValidator(_parse_timestamp)]


class SandboxStatus(str, Enum):
    """Status of a sandbox."""

    PENDING = "pending"
    RUNNING = "running"
    SNAPSHOTTING = "snapshotting"
    SUSPENDING = "suspending"
    SUSPENDED = "suspended"
    TERMINATED = "terminated"
    TIMEOUT = "timeout"


class SnapshotStatus(str, Enum):
    """Status of a snapshot."""

    IN_PROGRESS = "in_progress"
    LOCAL_READY = "local_ready"
    COMPLETED = "completed"
    FAILED = "failed"


class SnapshotWaitCondition(str, Enum):
    """Snapshot readiness condition to wait for."""

    LOCAL_READY = "local_ready"
    COMPLETED = "completed"


def snapshot_satisfies_wait_condition(
    status: SnapshotStatus | str,
    wait_until: SnapshotWaitCondition | str,
) -> bool:
    """Return whether a snapshot status satisfies the requested wait condition."""

    status_value = status.value if isinstance(status, SnapshotStatus) else str(status)
    wait_value = (
        wait_until.value
        if isinstance(wait_until, SnapshotWaitCondition)
        else SnapshotWaitCondition(wait_until).value
    )

    if wait_value == SnapshotWaitCondition.LOCAL_READY.value:
        return status_value in (
            SnapshotStatus.LOCAL_READY.value,
            SnapshotStatus.COMPLETED.value,
        )
    return status_value == SnapshotStatus.COMPLETED.value


class SnapshotType(str, Enum):
    """User-facing snapshot type for sandbox snapshot creation.

    - ``MEMORY``: Capture VM memory + filesystem state. Sandboxes
      restored from this snapshot warm-restore VM memory.
    - ``FILESYSTEM``: Capture filesystem state only. Sandboxes restored
      from this snapshot cold-boot from the snapshot tarball instead of
      warm-restoring VM state. Use this for sandbox image builds so that
      the restored sandbox bypasses Firecracker's overlay-path constraints.
    """

    MEMORY = "memory"
    FILESYSTEM = "filesystem"


class CheckpointType(str, Enum):
    """Checkpoint type for :meth:`Sandbox.checkpoint`.

    - ``MEMORY``: Capture VM memory + filesystem state. Sandboxes
      restored from this checkpoint warm-restore VM memory and running
      processes.
    - ``FILESYSTEM``: Capture filesystem state only. Sandboxes restored
      from this checkpoint cold-boot from the snapshot tarball.
    """

    MEMORY = "memory"
    FILESYSTEM = "filesystem"


class ContainerResourcesInfo(BaseModel):
    """Container resource configuration."""

    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int


class CreateSandboxResources(BaseModel):
    """Resource overrides accepted when creating a sandbox."""

    cpus: float
    memory_mb: int
    disk_mb: int | None = None


class NetworkConfig(BaseModel):
    """Network access control policy for sandbox containers.

    Rules are enforced via host-level iptables on the DOCKER-USER chain.
    Each container gets its own chain with rules applied before any user
    code runs.

    ``allow_out`` rules are evaluated before ``deny_out`` rules, so allow
    takes precedence over deny.  Established/related connections are always
    permitted (stateful firewall).

    When ``allow_internet_access`` is ``True`` (the default), all outbound
    traffic is allowed unless explicitly denied by ``deny_out``.  When
    ``False``, all outbound traffic is blocked unless explicitly allowed by
    ``allow_out``.
    """

    allow_internet_access: bool = True
    allow_out: list[str] = Field(
        default_factory=list,
        description=(
            "Destination IPs or CIDRs to allow "
            '(e.g. ["8.8.8.8", "10.0.0.0/8"]). '
            "Evaluated before deny_out; takes precedence."
        ),
    )
    deny_out: list[str] = Field(
        default_factory=list,
        description=(
            "Destination IPs or CIDRs to deny "
            '(e.g. ["192.168.1.0/24"]). '
            "Evaluated after allow_out."
        ),
    )


# --- Request models ---


class CreateSandboxRequest(BaseModel):
    """Request payload for creating a sandbox."""

    image: str | None = None
    resources: CreateSandboxResources
    timeout_secs: int | None = None
    entrypoint: list[str] | None = None
    network: NetworkConfig | None = None
    snapshot_id: str | None = None
    name: str | None = None


class UpdateSandboxRequest(BaseModel):
    """Request payload for updating a sandbox."""

    name: str | None = None
    allow_unauthenticated_access: bool | None = None
    exposed_ports: list[int] | None = None


class SandboxPoolRequest(BaseModel):
    """Request payload for creating or updating a sandbox pool."""

    image: str
    resources: ContainerResourcesInfo
    timeout_secs: int = 0
    entrypoint: list[str] | None = None
    max_containers: int | None = None
    warm_containers: int | None = None


# --- Response models ---


class CreateSandboxResponse(BaseModel):
    """Response from creating a sandbox."""

    sandbox_id: str
    status: SandboxStatus
    reason: str | None = None
    routing_hint: str | None = None
    ingress_endpoint: str | None = None
    name: str | None = None
    termination_reason: str | None = None
    error_details: Any | None = None


class CopiedSandboxResponse(BaseModel):
    """One sandbox returned by a live sandbox copy request.

    Partial copy responses can include failed sandboxes, so ``status`` is kept
    as a raw server string instead of the normal ``SandboxStatus`` enum.
    """

    sandbox_id: str
    status: str
    reason: str | None = None
    routing_hint: str | None = None
    ingress_endpoint: str | None = None
    name: str | None = None
    termination_reason: str | None = None
    error_details: Any | None = None


class CopySandboxResponse(BaseModel):
    """Response from live-copying a running sandbox."""

    source_sandbox_id: str
    sandboxes: list[CopiedSandboxResponse]


class SandboxInfo(BaseModel):
    """Full sandbox information."""

    sandbox_id: str = Field(alias="id")
    namespace: str
    status: SandboxStatus
    image: str | None = None
    resources: ContainerResourcesInfo
    timeout_secs: int | None = None
    entrypoint: list[str] | None = None
    network: NetworkConfig | None = None
    pool_id: str | None = None
    outcome: str | None = None
    termination_reason: str | None = None
    error_details: Any | None = None
    created_at: OptionalTimestamp = None
    terminated_at: OptionalTimestamp = None
    name: str | None = None
    allow_unauthenticated_access: bool = False
    exposed_ports: list[int] | None = None
    ingress_endpoint: str | None = None
    sandbox_url: str | None = None
    routing_hint: str | None = None

    model_config = {"populate_by_name": True}

    def url_for_port(self, port: int = _SANDBOX_MANAGEMENT_PORT) -> str | None:
        """Return the public URL for the management API or an exposed user port."""

        if self.ingress_endpoint is not None:
            return sandbox_url_from_ingress_endpoint(
                self.ingress_endpoint,
                self.sandbox_id,
                port,
            )
        if port == _SANDBOX_MANAGEMENT_PORT:
            return self.sandbox_url
        return None


class SandboxPortAccess(BaseModel):
    """Current proxy access configuration for a sandbox."""

    allow_unauthenticated_access: bool = False
    exposed_ports: list[int] = Field(default_factory=list)
    ingress_endpoint: str | None = None
    sandbox_url: str | None = None


class ListSandboxesResponse(BaseModel):
    """Response from listing sandboxes (internal use)."""

    sandboxes: list[SandboxInfo]


class ArchivedSandboxInfo(SandboxInfo):
    """A terminated sandbox parked in the server's archived sandboxes store.

    Inherits every field from :class:`SandboxInfo` and adds the archival
    timestamp.
    """

    archived_at: Timestamp


class ListArchivedSandboxesResponse(BaseModel):
    """Response from listing archived sandboxes (internal use)."""

    sandboxes: list[ArchivedSandboxInfo]
    prev_cursor: str | None = None
    next_cursor: str | None = None


class CreateSandboxPoolResponse(BaseModel):
    """Response from creating a sandbox pool."""

    pool_id: str
    namespace: str


class ContainerState(str, Enum):
    """State of a container in a pool."""

    IDLE = "Idle"
    RUNNING = "Running"


class PoolContainerInfo(BaseModel):
    """Information about a container in a sandbox pool."""

    id: str
    state: str
    sandbox_id: str | None = None
    executor_id: str


class SandboxPoolInfo(BaseModel):
    """Full sandbox pool information.

    When retrieved via ``get_pool``, the ``containers`` field is populated
    with the list of containers in the pool. It is ``None`` when returned
    from ``list_pools`` or ``update_pool``.
    """

    pool_id: str = Field(alias="id")
    namespace: str
    image: str
    resources: ContainerResourcesInfo
    timeout_secs: int = 0
    entrypoint: list[str] | None = None
    max_containers: int | None = None
    warm_containers: int | None = None
    containers: list[PoolContainerInfo] | None = None
    created_at: OptionalTimestamp = None
    updated_at: OptionalTimestamp = None

    model_config = {"populate_by_name": True}


class ListSandboxPoolsResponse(BaseModel):
    """Response from listing sandbox pools (internal use)."""

    pools: list[SandboxPoolInfo]


# --- Snapshot models ---


class CreateSnapshotResponse(BaseModel):
    """Response from creating a snapshot."""

    snapshot_id: str
    status: SnapshotStatus


class SnapshotInfo(BaseModel):
    """Full snapshot information."""

    snapshot_id: str = Field(alias="id")
    namespace: str
    sandbox_id: str
    base_image: str | None = None
    status: SnapshotStatus
    snapshot_type: SnapshotType | None = None
    error: str | None = None
    snapshot_uri: str | None = None
    snapshot_format_version: str | None = None
    size_bytes: int | None = None
    rootfs_disk_bytes: int | None = None
    created_at: OptionalTimestamp = None

    model_config = {"populate_by_name": True}


class ListSnapshotsResponse(BaseModel):
    """Response from listing snapshots (internal use)."""

    snapshots: list[SnapshotInfo]


# --- Container daemon models (process management, file ops, I/O) ---


class ProcessStatus(str, Enum):
    """Status of a process running in a sandbox."""

    RUNNING = "running"
    EXITED = "exited"
    SIGNALED = "signaled"
    OOM_KILLED = "oom_killed"


class StdinMode(str, Enum):
    """Stdin mode for a process."""

    CLOSED = "closed"
    PIPE = "pipe"


class OutputMode(str, Enum):
    """Output capture mode for stdout/stderr."""

    CAPTURE = "capture"
    DISCARD = "discard"


class ProcessUserSpec(BaseModel):
    """Structured POSIX user identity for process execution inside a sandbox."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    uid: int | None = None
    gid: int | None = None


ProcessUser = str | ProcessUserSpec | dict[str, str | int | None]


class RestartPolicy(str, Enum):
    """Restart behavior for a managed sandbox process."""

    NEVER = "never"
    ON_FAILURE = "on_failure"
    ALWAYS = "always"


class RestartPolicyConfig(BaseModel):
    """Restart policy and backoff settings for a managed process."""

    policy: RestartPolicy = RestartPolicy.ON_FAILURE
    max_restarts: int | None = None
    initial_backoff_ms: int = 500
    max_backoff_ms: int = 30_000


class ProcessHealthCheckType(str, Enum):
    """Managed-process health check type."""

    HTTP = "http"
    TCP = "tcp"


class ProcessHealthCheck(BaseModel):
    """Local health check for a managed process."""

    type: ProcessHealthCheckType
    port: int
    path: str | None = None
    initial_delay_ms: int = 5_000
    interval_ms: int = 1_000
    timeout_ms: int = 500
    failure_threshold: int = 3


class ManagedProcessStatus(str, Enum):
    """Supervisor lifecycle status for a managed process."""

    STARTING = "starting"
    RUNNING = "running"
    BACKING_OFF = "backing_off"
    STOPPED = "stopped"


class ManagedProcessHealthStatus(str, Enum):
    """Latest managed-process health status."""

    DISABLED = "disabled"
    STARTING = "starting"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class ManagedProcessExit(BaseModel):
    """Terminal status of a previous managed process run."""

    exit_code: int | None = None
    signal: int | None = None
    oom_killed: bool = False
    ended_at: Timestamp


class ManagedProcessInfo(BaseModel):
    """Managed-process metadata embedded into ProcessInfo."""

    id: str
    name: str | None = None
    status: ManagedProcessStatus
    restart_count: int
    restart: RestartPolicyConfig
    health_check: ProcessHealthCheck | None = None
    health_status: ManagedProcessHealthStatus
    consecutive_health_failures: int
    last_exit: ManagedProcessExit | None = None
    last_error: str | None = None
    next_restart_at: OptionalTimestamp = None


class ProcessInfo(BaseModel):
    """Information about a process running in a sandbox."""

    handle: int | None = None
    pid: int
    status: ProcessStatus
    exit_code: int | None = None
    signal: int | None = None
    stdin_writable: bool = False
    command: str
    args: list[str] = Field(default_factory=list)
    started_at: Timestamp
    ended_at: OptionalTimestamp = None
    managed: ManagedProcessInfo | None = None


class ListProcessesResponse(BaseModel):
    """Response from listing processes (internal use)."""

    processes: list[ProcessInfo]


class SendSignalResponse(BaseModel):
    """Response from sending a signal to a process."""

    success: bool


class OutputResponse(BaseModel):
    """Response containing process output lines."""

    pid: int
    lines: list[str]
    line_count: int


class OutputEvent(BaseModel):
    """A single output event from an SSE stream."""

    line: str
    timestamp: Timestamp
    stream: str | None = None


class DaemonInfo(BaseModel):
    """Information about the container daemon."""

    version: str
    uptime_secs: int
    running_processes: int
    total_processes: int


class HealthResponse(BaseModel):
    """Health check response from the container daemon."""

    healthy: bool


class DirectoryEntry(BaseModel):
    """An entry in a directory listing."""

    name: str
    is_dir: bool
    size: int | None = None
    modified_at: OptionalTimestamp = None


class ListDirectoryResponse(BaseModel):
    """Response from listing a directory in a sandbox."""

    path: str
    entries: list[DirectoryEntry]


class CommandResult(BaseModel):
    """Result of running a command to completion."""

    exit_code: int
    stdout: str
    stderr: str
