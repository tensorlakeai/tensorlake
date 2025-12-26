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

    def pip_install(self, packages: str | List[str]) -> "Image":
        """
        Install Python packages using pip, handling externally-managed environments.

        Args:
            packages: A single package name/specifier or a list of packages to install.
                     Supports version specifiers, e.g., "requests>=2.28" or ["numpy", "pandas==2.0"]

        Returns:
            Self for method chaining.

        Example:
            image = (
                Image(base_image="ubuntu:24.04", name="my-image")
                .pip_install("requests")
                .pip_install(["numpy", "pandas>=2.0", "scikit-learn"])
            )
        """
        pkgs = packages if isinstance(packages, str) else " ".join(packages)
        # Use env var instead of --break-system-packages flag for compatibility
        # with older pip versions. The env var is ignored if there's no
        # EXTERNALLY-MANAGED marker (PEP 668).
        return self.run(f"PIP_BREAK_SYSTEM_PACKAGES=1 pip install {pkgs}")

    def setup_venv(self, path: str = "/venv") -> "Image":
        """
        Set up a Python virtual environment for pip installs.

        This is an alternative to pip_install() for users who prefer using virtual
        environments instead of --break-system-packages. After calling this method,
        subsequent pip install commands will use the virtual environment.

        Note: The base image must have python3-venv installed. For Ubuntu/Debian,
        you may need to run: .run("apt-get update && apt-get install -y python3-venv")

        Args:
            path: The path where the virtual environment will be created.
                  Defaults to "/venv".

        Returns:
            Self for method chaining.

        Example:
            image = (
                Image(base_image="ubuntu:24.04", name="my-image")
                .run("apt-get update && apt-get install -y python3-venv")
                .setup_venv()  # Creates /venv and adds to PATH
                .run("pip install requests numpy")  # Now works without --break-system-packages
            )
        """
        return (
            self.run(f"python3 -m venv {path}")
            .env("PATH", f"{path}/bin:$PATH")
            .run("pip install --upgrade pip wheel setuptools")
        )

    def _add_operation(self, op: _ImageBuildOperation) -> "Image":
        self._build_operations.append(op)
        return self
