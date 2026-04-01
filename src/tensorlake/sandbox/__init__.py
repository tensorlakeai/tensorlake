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
    SandboxPoolInfo,
    SandboxStatus,
    SendSignalResponse,
    SnapshotInfo,
    SnapshotStatus,
    StdinMode,
)
from .sandbox import Sandbox

__all__ = [
    # Lifecycle management
    "SandboxClient",
    # Sandbox interaction
    "Sandbox",
    "Desktop",
    # Lifecycle models
    "SandboxStatus",
    "SandboxInfo",
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
