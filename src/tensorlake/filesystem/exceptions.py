"""Exception hierarchy for filesystem operations."""


class FilesystemException(Exception):
    """Base exception for all filesystem-related errors."""

    pass


class FilesystemError(FilesystemException):
    """General filesystem operation error."""

    pass


class FilesystemNotFoundError(FilesystemError):
    """Raised when a filesystem is not found."""

    def __init__(self, name: str):
        self._name = name
        super().__init__(f"Filesystem not found: {name}")

    @property
    def name(self) -> str:
        return self._name


class FileNotFoundInFilesystemError(FilesystemError):
    """Raised when a file path does not exist in the filesystem."""

    def __init__(self, filesystem: str, path: str):
        self._filesystem = filesystem
        self._path = path
        super().__init__(f"File not found in filesystem {filesystem}: {path}")

    @property
    def filesystem(self) -> str:
        return self._filesystem

    @property
    def path(self) -> str:
        return self._path


class FilesystemAPIError(FilesystemError):
    """Raised when the remote filesystem API returns an error."""

    def __init__(self, status_code: int, message: str):
        self._status_code = status_code
        self._message = message
        super().__init__(f"API error (status {status_code}): {message}")

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def message(self) -> str:
        return self._message


class MountError(FilesystemError):
    """Raised when a local mount/unmount operation fails."""

    pass


class CliNotFoundError(MountError):
    """Raised when the `tl` CLI required for mount operations is unavailable."""

    def __init__(self, detail: str):
        super().__init__(
            "Mount operations require the Tensorlake CLI (`tl`) with `tl fs` "
            f"support: {detail}. Install or upgrade it with: "
            "curl -fsSL https://tensorlake.ai/install.sh | sh"
        )
