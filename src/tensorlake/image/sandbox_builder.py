"""Sandbox-image build engine.

Takes an :class:`Image` or a Dockerfile path and delegates the build to the
Rust core (``tensorlake._cloud_sdk.build_sandbox_image``), which materializes
the image inside a builder sandbox, snapshots the filesystem, and registers
the snapshot as a named sandbox template
(``POST /platform/v1/.../sandbox-templates``).

This is the programmatic backend for :meth:`Image.build` and the
``tl sbx image create`` CLI command. The Rust core owns parsing and the
Dockerfile-instruction allowlist:

* **Rejected** (the build errors): ``ONBUILD``, ``SHELL``, ``USER``.
* **Accepted but not materialized** (warned; image-config metadata discarded by
  ``docker build --output type=tar``): ``CMD``, ``ENTRYPOINT``, ``EXPOSE``,
  ``HEALTHCHECK``, ``LABEL``, ``STOPSIGNAL``, ``VOLUME``.
* **Supported**: ``FROM``, ``RUN``, ``WORKDIR``, ``ENV``, ``COPY``, ``ADD``,
  multi-stage builds (``FROM ... AS``, ``COPY --from=``), and top-level ``ARG``
  (accepted and forwarded to ``docker build``, though the SDK does not
  substitute ``ARG`` values at parse time).

Private registries: when a Dockerfile pulls ``FROM`` / ``COPY --from=`` an image
in a private container registry, credentials are read from the standard Docker
config file (``$DOCKER_CONFIG/config.json``, default ``~/.docker/config.json``)
and shipped to the builder. See :func:`build_sandbox_image` for details.
"""

from __future__ import annotations

import json
import os
import warnings
from pathlib import Path
from typing import Callable

from tensorlake._tracing import USER_AGENT
from tensorlake.cli._common import Context

from ._dockerfile import image_to_dockerfile
from .image import Image
from .utils import dockerfile_content

EmitFn = Callable[[dict], None]

_DEFAULT_IMAGE_NAME = "default"


# --- Public exceptions ------------------------------------------------------


class SandboxImageError(Exception):
    """Base class for sandbox-image build errors."""


class SandboxImageLoadError(SandboxImageError):
    """The source Dockerfile or Image could not be loaded."""


class SandboxImageBuildError(SandboxImageError):
    """The build failed while provisioning, materializing, or registering."""


# --- Emit helpers -----------------------------------------------------------


def _noop_emit(_obj: dict) -> None:
    pass


def _stderr_emit(obj: dict) -> None:
    import sys

    msg = obj.get("message") or ""
    event_type = obj.get("type", "")
    if event_type == "build_log":
        stream = obj.get("stream", "stdout")
        print(f"[{stream}] {msg}", file=sys.stderr)
    elif msg:
        print(f"[{event_type}] {msg}", file=sys.stderr)


# --- Env / context ---------------------------------------------------------


def _debug_enabled() -> bool:
    return os.environ.get("TENSORLAKE_DEBUG", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _build_context_from_env() -> Context:
    """Resolve auth + project context from Tensorlake env vars."""
    return Context.default(
        api_url=os.environ.get("TENSORLAKE_API_URL"),
        api_key=os.environ.get("TENSORLAKE_API_KEY"),
        personal_access_token=os.environ.get("TENSORLAKE_PAT"),
        namespace=os.environ.get("INDEXIFY_NAMESPACE"),
        organization_id=os.environ.get("TENSORLAKE_ORGANIZATION_ID"),
        project_id=os.environ.get("TENSORLAKE_PROJECT_ID"),
        debug=_debug_enabled(),
    )


# --- Rust delegation -------------------------------------------------------


def _rust_build_sandbox_image(*args, **kwargs) -> str:
    try:
        from tensorlake._cloud_sdk import (
            build_sandbox_image as rust_build_sandbox_image,
        )
    except ImportError:
        from _cloud_sdk import build_sandbox_image as rust_build_sandbox_image

    return rust_build_sandbox_image(*args, **kwargs)


def _run_rust_image_create(
    dockerfile_path: str,
    registered_name: str,
    *,
    dockerfile_text: str | None,
    context_dir: str | None,
    cpus: float,
    memory_mb: int,
    disk_mb: int | None,
    builder_disk_mb: int | None,
    is_public: bool,
    emit: EmitFn,
) -> dict:
    ctx = _build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxImageBuildError(
            "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        )

    emit({"type": "status", "message": f"Building image '{registered_name}'..."})

    def forward_event(event: dict) -> None:
        emit(dict(event))

    result_json = _rust_build_sandbox_image(
        ctx.api_url,
        token,
        dockerfile_path,
        registered_name,
        disk_mb,
        builder_disk_mb,
        cpus,
        memory_mb,
        is_public,
        ctx.organization_id,
        ctx.project_id,
        ctx.namespace,
        ctx.personal_access_token is not None and ctx.api_key is None,
        USER_AGENT,
        dockerfile_text,
        context_dir,
        forward_event,
    )
    try:
        result = json.loads(result_json) if result_json.strip() else {}
    except json.JSONDecodeError as exc:
        raise SandboxImageBuildError(
            f"Rust image builder returned invalid JSON: {result_json.strip()}"
        ) from exc
    emit(
        {
            "type": "image_registered",
            "image_id": result.get("id", ""),
            "name": registered_name,
            "snapshot_id": result.get("snapshot_id", ""),
        }
    )
    return result


# --- Public API -----------------------------------------------------------


def build_sandbox_image(
    source: Image | str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    builder_disk_mb: int | None = None,
    is_public: bool = False,
    context_dir: str | None = None,
    verbose: bool = False,
    emit: EmitFn | None = None,
) -> dict:
    """Build a sandbox image from an :class:`Image` or a Dockerfile path.

    Renders the source to a Dockerfile (for :class:`Image` inputs) and hands
    the Dockerfile path/text plus build context off to the Rust core, which
    parses, validates, materializes, and registers the image. Use the same
    rootfs-builder path and registration semantics as ``tl sbx image create``.

    Args:
        source: An :class:`Image` instance or a path to a Dockerfile.
        registered_name: Name to register the image under. Defaults to the
            image's ``name`` or the Dockerfile stem.
        cpus: CPUs for the build sandbox.
        memory_mb: Memory for the build sandbox in MB.
        disk_mb: Root disk size for the generated sandbox image in MB.
        builder_disk_mb: Root disk size for the temporary builder sandbox in MB.
        is_public: Make the registered image publicly accessible.
        context_dir: Directory used to resolve relative COPY/ADD sources.
            Ignored when ``source`` is a Dockerfile path (the Dockerfile's
            parent directory is used instead).
        verbose: Print progress to stderr. Ignored if ``emit`` is provided.
        emit: Callback invoked for each build event. Takes precedence over
            ``verbose``. Use this to integrate the builder into custom UIs.

    Returns:
        The registered sandbox template JSON response.

    Raises:
        SandboxImageLoadError: The source Dockerfile or Image could not be
            loaded.
        SandboxImageBuildError: The build failed during parsing, provisioning,
            materialization, or registration.
        TypeError: ``source`` is neither an ``Image`` nor a path.

    Private container registries:
        To pull ``FROM`` or ``COPY --from=`` an image in a private registry,
        provide credentials in the standard Docker config file —
        ``$DOCKER_CONFIG/config.json``, defaulting to ``~/.docker/config.json``.
        Populate it with ``docker login`` (or a daemonless tool such as
        ``skopeo``/``crane``/``oras login``, or by writing the JSON directly).
        Set the ``DOCKER_CONFIG`` environment variable to point at a directory
        to scope credentials to a single build in CI without touching global
        docker state. Basic auth (including an API key/token used as the
        password) and identity tokens are supported; credential helpers
        (``credsStore``/``credHelpers``) are resolved on this machine and
        inlined before the build. The target registry must appear under
        ``auths`` or ``credHelpers`` in the config. Credentials are uploaded
        only into the ephemeral builder sandbox (not sent to the
        sandbox-template API, not persisted server-side) and are not baked into
        the resulting image.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    if isinstance(source, Image):
        if not source._base_image:
            raise SandboxImageLoadError("Image must have a base_image to build")
        rust_context_dir = str(Path(context_dir or os.getcwd()).resolve())
        rust_dockerfile_path = str(Path(rust_context_dir) / "Dockerfile")
        dockerfile_text = image_to_dockerfile(source)
        if not dockerfile_text.endswith("\n"):
            dockerfile_text += "\n"
        effective_registered_name = registered_name or source.name
    elif isinstance(source, (str, os.PathLike)):
        path = Path(os.fspath(source)).resolve()
        if not path.is_file():
            raise SandboxImageLoadError(f"Dockerfile not found: {source}")
        rust_dockerfile_path = str(path)
        rust_context_dir = None
        dockerfile_text = None
        effective_registered_name = registered_name or _default_registered_name(
            str(path)
        )
    else:
        # Programmer error — propagate directly rather than wrapping.
        raise TypeError(
            "source must be an Image or a Dockerfile path, got "
            f"{type(source).__name__}"
        )

    if effective_registered_name == _DEFAULT_IMAGE_NAME:
        warnings.warn(
            f"Building sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
            "Pass `registered_name=...` or `Image(name=...)` to avoid collisions "
            "with other default-named images in this project.",
            stacklevel=2,
        )

    try:
        return _run_rust_image_create(
            rust_dockerfile_path,
            effective_registered_name,
            dockerfile_text=dockerfile_text,
            context_dir=rust_context_dir,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            builder_disk_mb=builder_disk_mb,
            is_public=is_public,
            emit=emit,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageBuildError(f"{type(e).__name__}: {e}") from e


def build_sandbox_application_image(
    image: Image,
    *,
    registered_name: str | None = None,
    build_env_vars: list[tuple[str, str]] | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    builder_disk_mb: int | None = None,
    is_public: bool = False,
    context_dir: str | None = None,
    verbose: bool = False,
    emit: EmitFn | None = None,
) -> dict:
    """Build an Applications runtime image as a registered sandbox template.

    Unlike :func:`build_sandbox_image`, this uses the Applications Dockerfile
    wrapper so deployed functions get the SDK runtime, default workdir, and
    deploy-time build environment variables.

    Private container registries are supported the same way as
    :func:`build_sandbox_image` — provide credentials via
    ``$DOCKER_CONFIG/config.json`` (default ``~/.docker/config.json``).
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    if not image._base_image:
        raise SandboxImageLoadError("Image must have a base_image to build")

    effective_registered_name = registered_name or image.name
    if effective_registered_name == _DEFAULT_IMAGE_NAME:
        warnings.warn(
            f"Building sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
            "Pass `registered_name=...` or `Image(name=...)` to avoid collisions "
            "with other default-named images in this project.",
            stacklevel=2,
        )

    rust_context_dir = str(Path(context_dir or os.getcwd()).resolve())
    rust_dockerfile_path = str(Path(rust_context_dir) / "Dockerfile")
    dockerfile_text = dockerfile_content(image, extra_env_vars=build_env_vars)
    if not dockerfile_text.endswith("\n"):
        dockerfile_text += "\n"

    try:
        return _run_rust_image_create(
            rust_dockerfile_path,
            effective_registered_name,
            dockerfile_text=dockerfile_text,
            context_dir=rust_context_dir,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            builder_disk_mb=builder_disk_mb,
            is_public=is_public,
            emit=emit,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageBuildError(f"{type(e).__name__}: {e}") from e


def _default_registered_name(dockerfile_path: str) -> str:
    """Mirror the Rust core's default name derivation for Dockerfile paths.

    Returned only when the user does not pass ``registered_name``. The Rust
    core derives the same name on its own; computing it here lets the
    ``_DEFAULT_IMAGE_NAME`` warning trigger before the Rust call.
    """
    path = Path(dockerfile_path)
    stem = path.stem
    if stem.lower() == "dockerfile":
        parent_name = path.parent.name.strip()
        return parent_name or "sandbox-image"
    return stem or "sandbox-image"
