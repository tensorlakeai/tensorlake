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
    CMD = 5
    ENTRYPOINT = 6


@dataclass
class _ImageBuildOperation:
    type: _ImageBuildOperationType
    args: List[str]
    options: Dict[str, str]


class Image:
    """
    Represents a Docker image to be used for application functions.
    """

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
        """
        The name of the image.
        """
        return self._name

    @property
    def tag(self) -> str:
        """
        The tag of the image.
        """
        return self._tag

    def add(
        self, src: str, dest: str, options: Dict[str, str] | None = None
    ) -> "Image":
        """
        Add a file or directory to the image. Equivalent to the Dockerfile ADD command.

        Args:
            src (str): The source file or directory path.
            dest (str): The destination path inside the image.
            options (Dict[str, str], optional): Additional options for the ADD operation.
        """
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
        """
        Copy a file or directory to the image. Equivalent to the Dockerfile COPY command.

        Args:
            src (str): The source file or directory path.
            dest (str): The destination path inside the image.
            options (Dict[str, str], optional): Additional options for the COPY operation.
        """
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.COPY,
                args=[src, dest],
                options={} if options is None else options,
            )
        )

    def env(self, key: str, value: str) -> "Image":
        """
        Set an environment variable in the image. Equivalent to the Dockerfile ENV command.

        Args:
            key (str): The environment variable name.
            value (str): The environment variable value.
        """

        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.ENV, args=[key, value], options={}
            )
        )

    def run(
        self, commands: str | List[str], options: Dict[str, str] | None = None
    ) -> "Image":
        """
        Run a command in the image. Equivalent to the Dockerfile RUN command.

        Args:
            commands (str | List[str]): The command or list of commands to run.
            options (Dict[str, str], optional): Additional options for the RUN operation.
        """
        args = commands if isinstance(commands, list) else [commands]
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.RUN,
                args=args,
                options={} if options is None else options,
            )
        )

    def cmd(self, command: str | List[str]) -> "Image":
        """
        Set the default command for the image. Equivalent to the Dockerfile CMD command.

        Setting the CMD in the image is not necessary when using Tensorlake, as the application
        function will override it. However, it can be useful for local testing, or custom images.

        Args:
            command (str | List[str]): The command or list of commands to set as the default command.
        """

        args = command if isinstance(command, list) else [command]
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.CMD, args=args, options={}
            )
        )

    def entrypoint(self, command: str | List[str]) -> "Image":
        """
        Set the entrypoint for the image. Equivalent to the Dockerfile ENTRYPOINT command.

        Setting the ENTRYPOINT in the image is not necessary when using Tensorlake, as the application
        function will override it. However, it can be useful for local testing, or custom images

        Args:
            command (str | List[str]): The command or list of commands to set as the entrypoint.
        """

        args = command if isinstance(command, list) else [command]
        return self._add_operation(
            _ImageBuildOperation(
                type=_ImageBuildOperationType.ENTRYPOINT, args=args, options={}
            )
        )

    def _add_operation(self, op: _ImageBuildOperation) -> "Image":
        self._build_operations.append(op)
        return self
