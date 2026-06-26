"""Dockerfile rendering helpers shared across the Applications and Sandbox builders.

These functions produce Dockerfile text from an :class:`Image`. The output
matches the TypeScript SDK's ``dockerfileContent`` so both SDKs generate
byte-identical Dockerfiles for the same image definition.
"""

from __future__ import annotations

import json

from .image import Image, _ImageBuildOperation, _ImageBuildOperationType

# Tensorlake's sandbox runtime runs commands as the ``tl-user`` account. Provision
# it so images built from upstream bases (e.g. ``python:3.11-slim``) that lack the
# account don't fail at runtime with ``user not found: tl-user``.
#
# Best-effort and portable by design:
#   * no-op when ``tl-user`` already exists (Tensorlake base images);
#   * ``useradd`` for glibc bases (Debian/Ubuntu/Fedora/...);
#   * BusyBox ``adduser -D`` fallback for Alpine, which lacks ``useradd``;
#   * trailing ``|| true`` so a base with a non-root default ``USER`` (no
#     permission to edit ``/etc/passwd``) or any other failure never aborts a
#     build that previously succeeded.
# Must stay byte-identical to the TypeScript SDK's ``ENSURE_RUNTIME_USER_COMMAND``.
ENSURE_RUNTIME_USER_COMMAND = (
    "id -u tl-user >/dev/null 2>&1 "
    "|| useradd -m tl-user >/dev/null 2>&1 "
    "|| adduser -D tl-user >/dev/null 2>&1 "
    "|| true"
)


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
        lines.append(f"RUN {ENSURE_RUNTIME_USER_COMMAND}")
    for op in image._build_operations:
        lines.append(render_op_line(op))
    return "\n".join(lines)


def image_has_workdir(image: Image) -> bool:
    """True when the image defines its own WORKDIR op."""
    return any(
        op.type == _ImageBuildOperationType.WORKDIR for op in image._build_operations
    )
