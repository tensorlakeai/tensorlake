"""Dockerfile rendering helpers shared across the Applications and Sandbox builders.

These functions produce Dockerfile text from an :class:`Image`. The output
matches the TypeScript SDK's ``dockerfileContent`` so both SDKs generate
byte-identical Dockerfiles for the same image definition.
"""

from __future__ import annotations

import json

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


def image_to_dockerfile(image: Image) -> str:
    """Render a plain Dockerfile string from an :class:`Image`.

    Mirrors the TypeScript ``dockerfileContent`` exactly.
    """
    lines: list[str] = []
    if image._base_image:
        lines.append(f"FROM {image._base_image}")
    for op in image._build_operations:
        lines.append(render_op_line(op))
    return "\n".join(lines)


def image_has_workdir(image: Image) -> bool:
    """True when the image defines its own WORKDIR op."""
    return any(
        op.type == _ImageBuildOperationType.WORKDIR for op in image._build_operations
    )
