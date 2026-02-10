"""Pydantic models for sandbox operations."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


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
    """Network configuration for sandbox."""

    allow_internet_access: bool = True
    allow_out: List[str] = Field(default_factory=list)
    deny_out: List[str] = Field(default_factory=list)


class CreateSandboxResponse(BaseModel):
    """Response from creating a sandbox."""

    sandbox_id: str
    status: SandboxStatus


class SandboxInfo(BaseModel):
    """Full sandbox information."""

    sandbox_id: str = Field(alias="id")
    namespace: str
    status: SandboxStatus
    image: Optional[str] = None
    resources: ContainerResourcesInfo
    secret_names: List[str] = Field(default_factory=list)
    timeout_secs: Optional[int] = None
    entrypoint: Optional[List[str]] = None
    network: Optional[NetworkConfig] = None
    pool_id: Optional[str] = None
    created_at: Optional[int] = None
    terminated_at: Optional[int] = None

    model_config = {"populate_by_name": True}


class ListSandboxesResponse(BaseModel):
    """Response from listing sandboxes (internal use)."""

    sandboxes: List[SandboxInfo]


class CreateSandboxPoolResponse(BaseModel):
    """Response from creating a sandbox pool."""

    pool_id: str
    namespace: str


class SandboxPoolInfo(BaseModel):
    """Full sandbox pool information."""

    pool_id: str = Field(alias="id")
    namespace: str
    image: str
    resources: ContainerResourcesInfo
    secret_names: List[str] = Field(default_factory=list)
    timeout_secs: int = 0
    entrypoint: Optional[List[str]] = None
    max_containers: Optional[int] = None
    warm_containers: Optional[int] = None
    created_at: Optional[int] = None
    updated_at: Optional[int] = None

    model_config = {"populate_by_name": True}


class ListSandboxPoolsResponse(BaseModel):
    """Response from listing sandbox pools (internal use)."""

    pools: List[SandboxPoolInfo]


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
    exit_code: Optional[int] = None
    signal: Optional[int] = None
    stdin_writable: bool = False
    command: str
    args: List[str] = Field(default_factory=list)
    started_at: int
    ended_at: Optional[int] = None


class ListProcessesResponse(BaseModel):
    """Response from listing processes (internal use)."""

    processes: List[ProcessInfo]


class SendSignalResponse(BaseModel):
    """Response from sending a signal to a process."""

    success: bool


class OutputResponse(BaseModel):
    """Response containing process output lines."""

    pid: int
    lines: List[str]
    line_count: int


class OutputEvent(BaseModel):
    """A single output event from an SSE stream."""

    line: str
    timestamp: int
    stream: Optional[str] = None


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
    size: Optional[int] = None
    modified_at: Optional[int] = None


class ListDirectoryResponse(BaseModel):
    """Response from listing a directory in a sandbox."""

    path: str
    entries: List[DirectoryEntry]


class CommandResult(BaseModel):
    """Result of running a command to completion."""

    exit_code: int
    stdout: str
    stderr: str
