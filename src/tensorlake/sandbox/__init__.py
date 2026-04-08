"""Tensorlake Sandbox SDK - Client for managing and interacting with sandboxes."""

from .client import SandboxClient
from .desktop import Desktop
from .exceptions import (
    PoolInUseError,
    PoolNotFoundError,
    RemoteAPIError,
    SandboxConnectionError,
    SandboxError,
    SandboxException,
    SandboxNotFoundError,
)
from .models import (
    CommandResult,
    ContainerResourcesInfo,
    CreateSandboxPoolResponse,
    CreateSandboxResponse,
    CreateSnapshotResponse,
    DaemonInfo,
    DirectoryEntry,
    HealthResponse,
    ListDirectoryResponse,
    NetworkConfig,
    OutputEvent,
    OutputMode,
    OutputResponse,
    PoolContainerInfo,
    ProcessInfo,
    ProcessStatus,
    SandboxInfo,
    SandboxPortAccess,
    SandboxPoolInfo,
    SandboxStatus,
    SendSignalResponse,
    SnapshotInfo,
    SnapshotStatus,
    StdinMode,
)
from .pty import Pty
from .sandbox import Sandbox

__all__ = [
    # Lifecycle management
    "SandboxClient",
    # Sandbox interaction
    "Sandbox",
    "Pty",
    "Desktop",
    # Lifecycle models
    "SandboxStatus",
    "SandboxInfo",
    "SandboxPortAccess",
    "CreateSandboxResponse",
    "SandboxPoolInfo",
    "PoolContainerInfo",
    "CreateSandboxPoolResponse",
    "ContainerResourcesInfo",
    "NetworkConfig",
    # Snapshot models
    "SnapshotStatus",
    "SnapshotInfo",
    "CreateSnapshotResponse",
    # Command result
    "CommandResult",
    # Process models
    "ProcessStatus",
    "ProcessInfo",
    "StdinMode",
    "OutputMode",
    "SendSignalResponse",
    "OutputResponse",
    "OutputEvent",
    # File models
    "DirectoryEntry",
    "ListDirectoryResponse",
    # Daemon models
    "DaemonInfo",
    "HealthResponse",
    # Exceptions
    "SandboxException",
    "SandboxError",
    "SandboxConnectionError",
    "SandboxNotFoundError",
    "PoolNotFoundError",
    "PoolInUseError",
    "RemoteAPIError",
]
