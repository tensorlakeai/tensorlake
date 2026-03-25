import importlib
from typing import List

from .image import Image, _ImageBuildOperation, _ImageBuildOperationType

_SDK_VERSION: str = importlib.metadata.version("tensorlake")


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


def _render_build_op(op: _ImageBuildOperation) -> str:
    options: str = " " + " ".join([f"--{k}={v}" for k, v in op.options.items()])
    if op.type == _ImageBuildOperationType.ENV:
        body: str = f'{op.args[0]}="{op.args[1]}"'
    else:
        body: str = " ".join(op.args)

    return f"{op.type.name}{options}{body}"
