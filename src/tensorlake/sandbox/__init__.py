"""Tensorlake Sandbox SDK - Client for managing and interacting with sandboxes."""

from .client import SandboxClient
from .exceptions import (
    PoolInUseError,
    PoolNotFoundError,
    RemoteAPIError,
    SandboxError,
    SandboxException,
    SandboxNotFoundError,
)
from .models import (
    CommandResult,
    ContainerResourcesInfo,
    CreateSandboxPoolResponse,
    CreateSandboxResponse,
    DaemonInfo,
    DirectoryEntry,
    HealthResponse,
    ListDirectoryResponse,
    OutputEvent,
    OutputMode,
    OutputResponse,
    ProcessInfo,
    ProcessStatus,
    SandboxInfo,
    SandboxPoolInfo,
    SandboxStatus,
    SendSignalResponse,
    StdinMode,
)
from .sandbox import Sandbox

__all__ = [
    # Lifecycle management
    "SandboxClient",
    # Sandbox interaction
    "Sandbox",
    # Lifecycle models
    "SandboxStatus",
    "SandboxInfo",
    "CreateSandboxResponse",
    "SandboxPoolInfo",
    "CreateSandboxPoolResponse",
    "ContainerResourcesInfo",
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
    "SandboxNotFoundError",
    "PoolNotFoundError",
    "PoolInUseError",
    "RemoteAPIError",
]
