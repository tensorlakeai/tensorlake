"""Container image utilities that are not part of the SDK interface."""

import hashlib
import importlib
import json
import logging
import os
import pathlib
from dataclasses import dataclass
from typing import Any, Dict, List
from urllib.parse import urlparse

from .interface import Function, Image, InternalError
from .interface.image import _ImageBuildOperation, _ImageBuildOperationType
from .registry import get_functions

_HASH_BUFF_SIZE: int = 1024**2
_SDK_VERSION: str = importlib.metadata.version("tensorlake")


@dataclass
class ImageInformation:
    image: Image
    # Functions that are using the image.
    functions: List[Function]


def image_infos() -> Dict[Image, ImageInformation]:
    # Image objects don't have custom __hash__ and __eq__, so they are compared by object identity.
    image_infos: Dict[Image, ImageInformation] = {}
    for func in get_functions():
        func: Function
        image: Image = func._function_config.image
        if image not in image_infos:
            image_infos[image] = ImageInformation(image=image, functions=[])
        image_infos[image].functions.append(func)

    return image_infos


def create_image_context_file(
    img: Image,
    file_path: str,
    extra_env_vars: List[tuple] | None = None,
) -> None:
    """Create a tar.gz file containing the Dockerfile and all necessary files for building the image"""
    if extra_env_vars:
        import gzip
        import io
        import tarfile

        content = dockerfile_content(img, extra_env_vars=extra_env_vars).encode("utf-8")
        with gzip.open(file_path, "wb") as gz:
            with tarfile.open(fileobj=gz, mode="w") as tar:
                info = tarfile.TarInfo(name="Dockerfile")
                info.size = len(content)
                tar.addfile(info, io.BytesIO(content))
        return

    try:
        from tensorlake_rust_cloud_sdk import (
            create_image_context_file as _rust_create_image_context_file,
        )
    except ImportError:
        try:
            from tensorlake._cloud_sdk import (
                create_image_context_file as _rust_create_image_context_file,
            )
        except ImportError:
            from _cloud_sdk import (
                create_image_context_file as _rust_create_image_context_file,
            )

    _rust_create_image_context_file(
        base_image=img._base_image,
        sdk_version=_SDK_VERSION,
        operations_json=json.dumps(
            [
                {"op": op.type.name, "args": op.args, "options": op.options}
                for op in img._build_operations
            ]
        ),
        file_path=file_path,
    )


def dockerfile_content(img: Image, extra_env_vars: List[tuple] | None = None) -> str:
    """Generate the Dockerfile content based on the build operations."""
    dockerfile_lines: List[str] = [
        f"FROM {img._base_image}",
        "WORKDIR /app",
        # Handle externally-managed environments (PEP 668) on modern Linux distros
        # like Ubuntu 24.04. This env var allows pip to install packages globally.
        "ENV PIP_BREAK_SYSTEM_PACKAGES=1",
    ]

    if extra_env_vars:
        for key, value in extra_env_vars:
            dockerfile_lines.append(f"ENV {key}={value}")

    for op in img._build_operations:
        dockerfile_lines.append(_render_build_op(op))

    # Run tensorlake install after all user commands. There's implicit dependency
    # of tensorlake install success on user commands right now.
    dockerfile_lines.append(f"RUN pip install tensorlake=={_SDK_VERSION}")

    return "\n".join(dockerfile_lines)


def image_hash(img: Image) -> str:
    hasher: Any = hashlib.sha256(img._name.encode())
    hasher.update(img._base_image.encode())
    for op in img._build_operations:
        _add_build_op_to_hasher(op, hasher)

    hasher.update(_SDK_VERSION.encode())

    return hasher.hexdigest()


def _is_url(path: str) -> bool:
    return urlparse(path).scheme in ("http", "https")


def _is_git_repo_url(path: str) -> bool:
    parsed = urlparse(path)
    return parsed.scheme == "git" or (
        parsed.hostname
        and (parsed.hostname == "github.com" or parsed.hostname.endswith(".github.com"))
    )


def _is_inside_git_dir(path: str) -> bool:
    parts = os.path.normpath(path).split(os.sep)
    return ".git" in parts


def _add_build_op_to_hasher(op: _ImageBuildOperation, hasher: Any) -> None:
    hasher.update(op.type.name.encode())

    if op.type in (
        _ImageBuildOperationType.RUN,
        _ImageBuildOperationType.ADD,
        _ImageBuildOperationType.ENV,
    ):
        for arg in op.args:
            hasher.update(arg.encode())

    elif op.type == _ImageBuildOperationType.COPY:
        for root, dirs, files in os.walk(op.args[0]):
            for file in files:
                filename = pathlib.Path(root, file)
                with open(filename, "rb") as fp:
                    data: bytes = fp.read(_HASH_BUFF_SIZE)
                    while data:
                        hasher.update(data)
                        data = fp.read(_HASH_BUFF_SIZE)
    else:
        raise InternalError(
            f"Unknown build operation type {_ImageBuildOperationType.name(op.type)}"
        )


def _render_build_op(op: _ImageBuildOperation) -> str:
    options: str = " " + " ".join([f"--{k}={v}" for k, v in op.options.items()])
    if op.type == _ImageBuildOperationType.ENV:
        body: str = f'{op.args[0]}="{op.args[1]}"'
    else:
        body: str = " ".join(op.args)

    return f"{op.type.name}{options}{body}"
