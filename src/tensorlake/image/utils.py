import importlib
from typing import List

from ._dockerfile import image_has_workdir, render_op_line
from .image import Image

_SDK_VERSION: str = importlib.metadata.version("tensorlake")


def dockerfile_content(img: Image, extra_env_vars: List[tuple] | None = None) -> str:
    """Generate the Applications Dockerfile for the given image.

    Wraps the plain image rendering with the extras the Applications image
    builder expects: a default ``WORKDIR /app`` (skipped when the image
    declares its own WORKDIR), ``PIP_BREAK_SYSTEM_PACKAGES=1`` for PEP 668
    Linux distros, and a trailing ``pip install tensorlake`` so the SDK is
    available at runtime.
    """
    dockerfile_lines: List[str] = [f"FROM {img._base_image}"]
    if not image_has_workdir(img):
        # Default workdir for Applications. Skip it when the user declared
        # one so we don't emit two WORKDIR layers.
        dockerfile_lines.append("WORKDIR /app")
    # Handle externally-managed environments (PEP 668) on modern Linux distros
    # like Ubuntu 24.04.
    dockerfile_lines.append("ENV PIP_BREAK_SYSTEM_PACKAGES=1")

    if extra_env_vars:
        for key, value in extra_env_vars:
            dockerfile_lines.append(f"ENV {key}={value}")

    for op in img._build_operations:
        dockerfile_lines.append(render_op_line(op))

    # Run tensorlake install after all user commands. There's implicit dependency
    # of tensorlake install success on user commands right now.
    dockerfile_lines.append(f"RUN pip install tensorlake=={_SDK_VERSION}")

    return "\n".join(dockerfile_lines)
