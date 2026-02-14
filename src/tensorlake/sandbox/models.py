"""Pydantic models for sandbox operations."""

from datetime import datetime, timezone
from enum import Enum
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field


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

    PENDING = "Pending"
    RUNNING = "Running"
    TERMINATED = "Terminated"


class ContainerResourcesInfo(BaseModel):
    """Container resource configuration."""

    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int


class NetworkConfig(BaseModel):
    """Network configuration for sandbox.

    Controls outbound network access for the sandbox container.
    ``allow_out`` and ``deny_out`` accept host or host:port strings
    (e.g. ``"api.example.com"`` or ``"10.0.0.1:443"``).
    """

    allow_internet_access: bool = True
    allow_out: list[str] = Field(default_factory=list)
    deny_out: list[str] = Field(default_factory=list)


# --- Request models ---


class CreateSandboxRequest(BaseModel):
    """Request payload for creating a sandbox."""

    image: str | None = None
    resources: ContainerResourcesInfo
    secret_names: list[str] | None = None
    timeout_secs: int | None = None
    entrypoint: list[str] | None = None
    network: NetworkConfig | None = None
    pool_id: str | None = None


class SandboxPoolRequest(BaseModel):
    """Request payload for creating or updating a sandbox pool."""

    image: str
    resources: ContainerResourcesInfo
    secret_names: list[str] | None = None
    timeout_secs: int = 0
    entrypoint: list[str] | None = None
    max_containers: int | None = None
    warm_containers: int | None = None


# --- Response models ---


class CreateSandboxResponse(BaseModel):
    """Response from creating a sandbox."""

    sandbox_id: str
    status: SandboxStatus


class SandboxInfo(BaseModel):
    """Full sandbox information."""

    sandbox_id: str = Field(alias="id")
    namespace: str
    status: SandboxStatus
    image: str | None = None
    resources: ContainerResourcesInfo
    secret_names: list[str] = Field(default_factory=list)
    timeout_secs: int | None = None
    entrypoint: list[str] | None = None
    network: NetworkConfig | None = None
    pool_id: str | None = None
    created_at: OptionalTimestamp = None
    terminated_at: OptionalTimestamp = None

    model_config = {"populate_by_name": True}


class ListSandboxesResponse(BaseModel):
    """Response from listing sandboxes (internal use)."""

    sandboxes: list[SandboxInfo]


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
    secret_names: list[str] = Field(default_factory=list)
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


# --- Container daemon models (process management, file ops, I/O) ---


class ProcessStatus(str, Enum):
    """Status of a process running in a sandbox."""

    RUNNING = "running"
    EXITED = "exited"
    SIGNALED = "signaled"


class StdinMode(str, Enum):
    """Stdin mode for a process."""

    CLOSED = "closed"
    PIPE = "pipe"


class OutputMode(str, Enum):
    """Output capture mode for stdout/stderr."""

    CAPTURE = "capture"
    DISCARD = "discard"


class ProcessInfo(BaseModel):
    """Information about a process running in a sandbox."""

    pid: int
    status: ProcessStatus
    exit_code: int | None = None
    signal: int | None = None
    stdin_writable: bool = False
    command: str
    args: list[str] = Field(default_factory=list)
    started_at: Timestamp
    ended_at: OptionalTimestamp = None


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
