import sys
from enum import Enum
from typing import Dict, List

from pydantic import BaseModel, Field

_LOCAL_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"
_DEFAULT_BASE_IMAGE_NAME = f"python:{_LOCAL_PYTHON_VERSION}-slim-bookworm"


class _ImageBuildOperationType(Enum):
    ADD = 1
    COPY = 2
    ENV = 3
    RUN = 4


class _ImageBuildOperation(BaseModel):
    """Image build operation with validation."""

    type: _ImageBuildOperationType
    args: List[str]
    options: Dict[str, str] = Field(default_factory=dict)

    class Config:
        arbitrary_types_allowed = True


class Image:
    """Container image configuration.

    Note: This class maintains compatibility with existing code by not directly
    inheriting from BaseModel, but its internal data is structured for easy conversion.
    """

    def __init__(
        self,
        name: str = "default",
        tag: str = "latest",
        base_image: str = _DEFAULT_BASE_IMAGE_NAME,
    ):
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

    def _add_operation(self, op: _ImageBuildOperation) -> "Image":
        self._build_operations.append(op)
        return self
