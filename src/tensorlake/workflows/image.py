"""Container image utilities that are not part of the SDK interface."""

import hashlib
import importlib
import logging
import os
import pathlib
import tarfile
from io import BytesIO
from typing import Any, List
from urllib.parse import urlparse

from .interface.image import Image, _ImageBuildOperation, _ImageBuildOperationType

HASH_BUFF_SIZE: int = 1024**2
SDK_VERSION: str = importlib.metadata.version("tensorlake")


def add_build_op_to_hasher(op: _ImageBuildOperation, hasher: Any) -> None:
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
                    data: bytes = fp.read(HASH_BUFF_SIZE)
                    while data:
                        hasher.update(data)
                        data = fp.read(HASH_BUFF_SIZE)
    else:
        raise ValueError(f"Unsupported build operation type {op.type}")


def render_build_op(op: _ImageBuildOperation) -> str:
    options: str = " " + " ".join([f"--{k}={v}" for k, v in op.options.items()])
    if op.type == _ImageBuildOperationType.ENV:
        body: str = f'{op.args[0]}="{op.args[1]}"'
    else:
        body: str = " ".join(op.args)

    return f"{op.type.name}{options}{body}"


def create_image_context_file(img: Image, file_name: str):
    with tarfile.open(file_name, "w:gz") as tf:
        for op in img._build_operations:
            if op.type == _ImageBuildOperationType.COPY:
                src: str = op.args[0]
                logging.info(f"Adding {src}")
                tf.add(src, src)
            elif op.type == _ImageBuildOperationType.ADD:
                src: str = op.args[0]
                if _is_url(src) or _is_git_repo_url(src):
                    logging.warning(
                        "Skipping ADD: %s is a URL or Git repo reference", src
                    )
                    continue
                if not os.path.exists(src):
                    logging.warning("Skipping ADD: %s does not exist", src)
                    continue
                if _is_inside_git_dir(src):
                    logging.warning("Skipping ADD: %s is inside a .git directory", src)
                    continue
                logging.info("Adding (ADD) %s", src)
                tf.add(src, arcname=src)

        df_content: str = dockerfile_content(img)
        tarinfo = tarfile.TarInfo("Dockerfile")
        tarinfo.size = len(df_content)

        tf.addfile(tarinfo, BytesIO(df_content.encode()))


def dockerfile_content(img: Image) -> str:
    """Generate the Dockerfile content based on the build operations."""
    dockerfile_lines: List[str] = [
        f"FROM {img._base_image}",
        "WORKDIR /app",
    ]

    for op in img._build_operations:
        dockerfile_lines.append(render_build_op(op))

    # Run tensorlake install after all user commands. There's implicit dependency
    # of tensorlake install success on user commands right now.
    dockerfile_lines.append(f"RUN pip install tensorlake=={SDK_VERSION}")

    return "\n".join(dockerfile_lines)


def image_hash(img: Image) -> str:
    hasher: Any = hashlib.sha256(img._name.encode())
    hasher.update(img._base_image.encode())
    for op in img._build_operations:
        add_build_op_to_hasher(op, hasher)

    hasher.update(SDK_VERSION.encode())

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
