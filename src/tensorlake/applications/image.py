"""Application image utilities that are not part of the SDK interface."""

import importlib
import json
from dataclasses import dataclass
from typing import Dict, List

from tensorlake.image import Image
from tensorlake.image.utils import _SDK_VERSION, dockerfile_content

from .interface import Function
from .registry import get_functions


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
