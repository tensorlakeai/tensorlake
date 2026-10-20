from dataclasses import dataclass
from enum import Enum
from typing import Dict, List

from tensorlake.vendor.nanoid.nanoid import generate as nanoid_generate

_DEFAULT_BASE_IMAGE_NAME = "python:3.14-trixie"


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
        disk_mb: int | None = None,
        builder_disk_mb: int | None = None,
        is_public: bool = False,
        docker_compat: bool = False,
        context_dir: str | None = None,
        cas: bool = False,
        build_args: dict[str, str] | None = None,
        verbose: bool = False,
        docker_config_json: str | None = None,
    ) -> dict:
        """Build and register this sandbox image.

        Builds a non-CAS image by default. Pass ``cas=True`` to build a CAS
        image through the Image Service.

        Args:
            registered_name: Name to register the image under. Defaults to ``self.name``.
            cpus: CPUs for the build sandbox (default 2.0).
            memory_mb: Memory for the build sandbox in MiB (default 4096).
            disk_mb: Root disk size for the generated sandbox image in MiB.
            builder_disk_mb: Root disk size for the temporary builder sandbox in MiB.
            is_public: Make the registered image publicly accessible.
            docker_compat: Use Docker/BuildKit max compatibility mode (build
                is slower and uses more memory and disk space on builder
                sandbox).
            context_dir: Build context directory, used exactly like
                ``docker build <context_dir>``: it is uploaded as-is and
                ``copy()``/``add()`` sources resolve relative to it. Required
                when this image has ``copy()``/``add()`` ops that read host
                files — building without it then raises. When the image has no
                such ops it may be omitted, and an empty context (just the
                generated Dockerfile) is uploaded so the cwd is not archived.
            cas: Build a CAS image through the Image Service. Defaults to False.
            build_args: Build-time variables for CAS builds. The generated
                Dockerfile declares these arguments for use in ``base_image``
                and build operations. Non-CAS builds reject build arguments.
            docker_config_json: Docker config JSON contents. Omit to discover local
                config at entry; pass "{}" to disable discovery.
            verbose: If True, print build progress to stderr.

        Returns:
            The registered sandbox template response as a dict.
        """
        from .sandbox_builder import build_sandbox_image

        return build_sandbox_image(
            self,
            registered_name=registered_name,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            builder_disk_mb=builder_disk_mb,
            is_public=is_public,
            docker_compat=docker_compat,
            context_dir=context_dir,
            cas=cas,
            build_args=build_args,
            verbose=verbose,
            docker_config_json=docker_config_json,
        )

    def _add_operation(self, op: _ImageBuildOperation) -> "Image":
        self._build_operations.append(op)
        return self
