import sys
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

_LOCAL_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
_DEFAULT_BASE_IMAGE_NAME = f"python:{_LOCAL_PYTHON_VERSION}-slim-bookworm"


class _ImageBuildOperationType(Enum):
    ADD = 1
    COPY = 2
    ENV = 3
    RUN = 4
    WORKDIR = 5


@dataclass
class _ImageBuildOperation:
    type: _ImageBuildOperationType
    args: List[str]
    options: Dict[str, str]


class Image:
    def __init__(
        self,
        name: str = "default",
        tag: str = "latest",
        base_image: str = _DEFAULT_BASE_IMAGE_NAME,
    ):
        # Used by ImageBuilder service to identify when different application
        # functions are using the same Image object.
        self._id: str = nanoid_generate()
        self._name: str = name
        self._tag: str = tag
        self._base_image: str = base_image
        self._build_operations: List[_ImageBuildOperation] = []

    @property
    def name(self) -> str:
        return self._name

    @property
    def tag(self) -> str:
        return self._tag

    def add(
        self, src: str, dest: str, options: Dict[str, str] | None = None
    ) -> "Image":
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.ADD,
                args=[src, dest],
                options={} if options is None else options,
            )
        )

    def copy(
        self, src: str, dest: str, options: Dict[str, str] | None = None
    ) -> "Image":
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.COPY,
                args=[src, dest],
                options={} if options is None else options,
            )
        )

    def env(self, key: str, value: str) -> "Image":
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.ENV, args=[key, value], options={}
            )
        )

    def run(
        self, commands: str | List[str], options: Dict[str, str] | None = None
    ) -> "Image":
        args = commands if isinstance(commands, list) else [commands]
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.RUN,
                args=args,
                options={} if options is None else options,
            )
        )

    def workdir(self, directory: str) -> "Image":
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.WORKDIR,
                args=[directory],
                options={},
            )
        )

    def build(
        self,
        *,
        registered_name: str | None = None,
        cpus: float = 2.0,
        memory_mb: int = 4096,
        is_public: bool = False,
        context_dir: str | None = None,
        verbose: bool = False,
    ) -> dict:
        """Build this image as a sandbox template and register it.

        Materializes the image in a build sandbox, snapshots the filesystem,
        and registers the snapshot as a named sandbox template.

        Args:
            registered_name: Name to register the image under. Defaults to ``self.name``.
            cpus: CPUs for the build sandbox (default 2.0).
            memory_mb: Memory for the build sandbox in MB (default 4096).
            is_public: Make the registered image publicly accessible.
            context_dir: Directory used to resolve relative COPY/ADD paths.
                Defaults to the current working directory.
            verbose: If True, print build progress to stderr.

        Returns:
            The registered sandbox template response as a dict.
        """
        from tensorlake.cli.create_sandbox_image import build_sandbox_image

        return build_sandbox_image(
            self,
            registered_name=registered_name,
            cpus=cpus,
            memory_mb=memory_mb,
            is_public=is_public,
            context_dir=context_dir,
            verbose=verbose,
        )

    def _add_operation(self, op: _ImageBuildOperation) -> "Image":
        self._build_operations.append(op)
        return self
