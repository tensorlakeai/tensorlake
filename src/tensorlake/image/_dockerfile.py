"""Dockerfile rendering helpers shared across the Applications and Sandbox builders.

These functions produce Dockerfile text from an :class:`Image`. Without
build arguments, the output matches the TypeScript SDK's ``dockerfileContent``
so both SDKs generate byte-identical Dockerfiles for the same image definition.
"""

from __future__ import annotations

import json
import re

from .image import Image, _ImageBuildOperation, _ImageBuildOperationType


def render_op_line(op: _ImageBuildOperation) -> str:
    """Format a single build op as a Dockerfile instruction line."""
    options = op.options or {}
    options_str = (
        " " + " ".join(f"--{k}={v}" for k, v in options.items()) if options else ""
    )
    if op.type == _ImageBuildOperationType.ENV:
        return f"ENV{options_str} {op.args[0]}={json.dumps(op.args[1])}"
    return f"{op.type.name}{options_str} {' '.join(op.args)}"


def image_to_dockerfile(image: Image, build_args: dict[str, str] | None = None) -> str:
    """Render a plain Dockerfile string from an :class:`Image`.

    Supplied build arguments are declared globally for ``FROM`` and inside
    the build stage for subsequent operations. Values travel separately to
    the builder; only names are included in the generated Dockerfile.
    Without build arguments, mirrors TypeScript ``dockerfileContent`` exactly.
    """
    names = list(build_args or {})
    for name in names:
        if not isinstance(name, str) or not re.fullmatch(
            r"[A-Za-z_][A-Za-z0-9_]*", name
        ):
            raise ValueError(f"Invalid build argument name: {name!r}")
    declarations = [f"ARG {name}" for name in sorted(names)]
    lines = declarations.copy()
    if image._base_image:
        lines.append(f"FROM {image._base_image}")
        lines.extend(declarations)
    for op in image._build_operations:
        lines.append(render_op_line(op))
    return "\n".join(lines)


def image_has_workdir(image: Image) -> bool:
    """True when the image defines its own WORKDIR op."""
    return any(
        op.type == _ImageBuildOperationType.WORKDIR for op in image._build_operations
    )
