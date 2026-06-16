"""Sandbox-image build engine.

Takes an :class:`Image` or a Dockerfile path and delegates the build to the
Rust core (``tensorlake._cloud_sdk.build_sandbox_image``), which materializes
the image inside a builder sandbox, snapshots the filesystem, and registers
the snapshot as a named sandbox template
(``POST /platform/v1/.../sandbox-templates``).

This is the programmatic backend for :meth:`Image.build` and the
``tl sbx image create`` CLI command. The Rust core owns parsing and the
warnings for instructions that run during the build but have no effect when a
sandbox runs from the image (``ONBUILD``/``SHELL``/``EXPOSE``/``HEALTHCHECK``/
``LABEL``/``STOPSIGNAL``/``VOLUME``).
"""

from __future__ import annotations

import json
import os
import re
import warnings
from pathlib import Path
from typing import Any, Callable

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


class SandboxImageDeleteError(SandboxImageError):
    """Deleting a registered sandbox image failed."""


class SandboxImageLookupError(SandboxImageError):
    """Looking up a registered sandbox image failed."""


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


def _rust_import_sandbox_image(*args, **kwargs) -> str:
    try:
        from tensorlake._cloud_sdk import (
            import_sandbox_image as rust_import_sandbox_image,
        )
    except ImportError:
        from _cloud_sdk import import_sandbox_image as rust_import_sandbox_image

    return rust_import_sandbox_image(*args, **kwargs)


def _rust_delete_sandbox_image(
    api_url: str,
    token: str,
    image_name: str,
    organization_id: str | None,
    project_id: str | None,
    namespace: str | None,
) -> None:
    try:
        from tensorlake._cloud_sdk import CloudApiClient
    except ImportError:
        from _cloud_sdk import CloudApiClient

    client = CloudApiClient(
        api_url=api_url,
        api_key=token,
        organization_id=organization_id,
        project_id=project_id,
        namespace=namespace,
        user_agent=USER_AGENT,
    )
    try:
        client.delete_sandbox_image(image_name)
    finally:
        client.close()


def _rust_find_sandbox_image_by_name(
    api_url: str,
    token: str,
    image_name: str,
    organization_id: str,
    project_id: str,
    namespace: str | None,
) -> str | None:
    try:
        from tensorlake._cloud_sdk import CloudApiClient
    except ImportError:
        from _cloud_sdk import CloudApiClient

    client = CloudApiClient(
        api_url=api_url,
        api_key=token,
        organization_id=organization_id,
        project_id=project_id,
        namespace=namespace,
        user_agent=USER_AGENT,
    )
    try:
        return client.find_sandbox_image_by_name(
            organization_id, project_id, image_name
        )
    finally:
        client.close()


def _rust_list_sandbox_images(
    api_url: str,
    token: str,
    organization_id: str,
    project_id: str,
    namespace: str | None,
) -> str:
    try:
        from tensorlake._cloud_sdk import CloudApiClient
    except ImportError:
        from _cloud_sdk import CloudApiClient

    client = CloudApiClient(
        api_url=api_url,
        api_key=token,
        organization_id=organization_id,
        project_id=project_id,
        namespace=namespace,
        user_agent=USER_AGENT,
    )
    try:
        return client.list_sandbox_images(organization_id, project_id)
    finally:
        client.close()


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
    docker_compat: bool,
    emit: EmitFn,
) -> dict:
    ctx, token = _resolve_build_credentials()

    emit({"type": "status", "message": f"Building image '{registered_name}'..."})

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
        docker_compat,
        dockerfile_text,
        context_dir,
        _forwarder(emit),
    )
    return _finish_image_registration(result_json, registered_name, emit)


def _run_rust_image_import(
    image_reference: str,
    registered_name: str,
    *,
    cpus: float,
    memory_mb: int,
    disk_mb: int | None,
    builder_disk_mb: int | None,
    is_public: bool,
    docker_compat: bool,
    emit: EmitFn,
) -> dict:
    ctx, token = _resolve_build_credentials()

    emit(
        {
            "type": "status",
            "message": (
                f"Importing image '{image_reference}' as '{registered_name}'..."
            ),
        }
    )

    result_json = _rust_import_sandbox_image(
        ctx.api_url,
        token,
        image_reference,
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
        docker_compat,
        _forwarder(emit),
    )
    return _finish_image_registration(result_json, registered_name, emit)


def _resolve_build_credentials() -> tuple[Context, str]:
    ctx = _build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxImageBuildError(
            "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        )
    return ctx, token


def _forwarder(emit: EmitFn) -> EmitFn:
    def forward_event(event: dict) -> None:
        emit(dict(event))

    return forward_event


def _finish_image_registration(
    result_json: str, registered_name: str, emit: EmitFn
) -> dict:
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


def delete_sandbox_image(image_name: str) -> None:
    """Delete a registered sandbox image by name.

    Uses the same environment-based Tensorlake auth and namespace context as
    :func:`build_sandbox_image`.
    """
    if not isinstance(image_name, str) or not image_name:
        raise TypeError("image_name must be a non-empty string")

    ctx = _build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxImageDeleteError(
            "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        )

    try:
        _rust_delete_sandbox_image(
            ctx.api_url,
            token,
            image_name,
            ctx.organization_id,
            ctx.project_id,
            ctx.namespace,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageDeleteError(f"{type(e).__name__}: {e}") from e


_CAMEL_TO_SNAKE_RE = re.compile(r"(?<!^)(?<![A-Z])([A-Z])")


def _camel_to_snake(name: str) -> str:
    """Convert a single camelCase key to snake_case."""
    return _CAMEL_TO_SNAKE_RE.sub(r"_\1", name).lower()


def _snake_case_keys(value: Any) -> Any:
    """Recursively rewrite dict keys from camelCase to snake_case.

    The platform sandbox-templates API (and therefore the Rust core that
    proxies it) emits camelCase field names such as ``snapshotId`` and
    ``rootfsDiskBytes``. The Python SDK conventionally exposes snake_case keys,
    so normalize the template payloads before handing them back to callers.
    """
    if isinstance(value, dict):
        return {
            _camel_to_snake(key): _snake_case_keys(val) for key, val in value.items()
        }
    if isinstance(value, list):
        return [_snake_case_keys(item) for item in value]
    return value


def find_sandbox_image_by_name(image_name: str) -> dict | None:
    """Look up a registered sandbox image by its registered name.

    Returns the registered sandbox template as a dict, or ``None`` if no image
    with that name exists. Uses the same environment-based Tensorlake auth as
    :func:`build_sandbox_image`, and requires organization/project context
    (``TENSORLAKE_ORGANIZATION_ID`` and ``TENSORLAKE_PROJECT_ID``) since the
    lookup is routed through the platform sandbox-templates API.

    Raises:
        TypeError: ``image_name`` is not a non-empty string.
        SandboxImageLookupError: Credentials or project context are missing, or
            the lookup request failed.
    """
    if not isinstance(image_name, str) or not image_name:
        raise TypeError("image_name must be a non-empty string")

    ctx = _build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxImageLookupError(
            "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        )
    if not ctx.organization_id or not ctx.project_id:
        raise SandboxImageLookupError(
            "Looking up a sandbox image by name requires organization and "
            "project context (TENSORLAKE_ORGANIZATION_ID and "
            "TENSORLAKE_PROJECT_ID)."
        )

    try:
        result_json = _rust_find_sandbox_image_by_name(
            ctx.api_url,
            token,
            image_name,
            ctx.organization_id,
            ctx.project_id,
            ctx.namespace,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageLookupError(f"{type(e).__name__}: {e}") from e

    if not result_json:
        return None
    try:
        return _snake_case_keys(json.loads(result_json))
    except json.JSONDecodeError as exc:
        raise SandboxImageLookupError(
            f"Rust image lookup returned invalid JSON: {result_json.strip()}"
        ) from exc


def list_sandbox_images() -> list[dict]:
    """List all registered sandbox images for the current project.

    Returns the registered sandbox templates as a list of dicts (each with
    ``id``, ``name``, ``snapshot_id``, ``public``, etc.). Uses the same
    environment-based Tensorlake auth as :func:`build_sandbox_image`, and
    requires organization/project context (``TENSORLAKE_ORGANIZATION_ID`` and
    ``TENSORLAKE_PROJECT_ID``) since the listing is routed through the platform
    sandbox-templates API.

    Raises:
        SandboxImageLookupError: Credentials or project context are missing, or
            the list request failed.
    """
    ctx = _build_context_from_env()
    token = ctx.api_key or ctx.personal_access_token
    if not token:
        raise SandboxImageLookupError(
            "Missing TENSORLAKE_API_KEY or TENSORLAKE_PAT credentials."
        )
    if not ctx.organization_id or not ctx.project_id:
        raise SandboxImageLookupError(
            "Listing sandbox images requires organization and project context "
            "(TENSORLAKE_ORGANIZATION_ID and TENSORLAKE_PROJECT_ID)."
        )

    try:
        result_json = _rust_list_sandbox_images(
            ctx.api_url,
            token,
            ctx.organization_id,
            ctx.project_id,
            ctx.namespace,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageLookupError(f"{type(e).__name__}: {e}") from e

    if not result_json:
        return []
    try:
        return _snake_case_keys(json.loads(result_json))
    except json.JSONDecodeError as exc:
        raise SandboxImageLookupError(
            f"Rust image list returned invalid JSON: {result_json.strip()}"
        ) from exc


def build_sandbox_image(
    source: Image | str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    builder_disk_mb: int | None = None,
    is_public: bool = False,
    docker_compat: bool = False,
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
        docker_compat: Use Docker/BuildKit max compatibility mode. Slower and
            may require a larger builder sandbox disk.
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
            docker_compat=docker_compat,
            emit=emit,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageBuildError(f"{type(e).__name__}: {e}") from e


def import_sandbox_image(
    image_reference: str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    builder_disk_mb: int | None = None,
    is_public: bool = False,
    docker_compat: bool = False,
    verbose: bool = False,
    emit: EmitFn | None = None,
) -> dict:
    """Import a registry image directly into a sandbox image — no Docker.

    Unlike :func:`build_sandbox_image`, there is no Dockerfile and no build
    context: the builder pulls the referenced image's layers and applies them
    straight into the rootfs (via ``oci-image-to-ext4``), bypassing the Docker
    daemon entirely. This is the programmatic backend for the
    ``tl sbx image import`` CLI command. The import is always a fresh base from
    the registry — the reference is never resolved against the template
    registry.

    Args:
        image_reference: The registry image reference to import, e.g.
            ``pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime`` or
            ``ghcr.io/org/app@sha256:...``.
        registered_name: Name to register the image under. Defaults to the
            image reference's last path segment with any tag/digest stripped
            (e.g. ``pytorch/pytorch:2.4.1`` -> ``pytorch``).
        cpus: CPUs for the build sandbox.
        memory_mb: Memory for the build sandbox in MB.
        disk_mb: Root disk size for the generated sandbox image in MB.
        builder_disk_mb: Root disk size for the temporary builder sandbox in MB.
        is_public: Make the registered image publicly accessible.
        docker_compat: Use Docker/BuildKit max compatibility mode. Slower and
            may require a larger builder sandbox disk.
        verbose: Print progress to stderr. Ignored if ``emit`` is provided.
        emit: Callback invoked for each build event. Takes precedence over
            ``verbose``.

    Returns:
        The registered sandbox template JSON response.

    Raises:
        SandboxImageBuildError: The image reference is empty, or the import
            failed during provisioning, materialization, or registration.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    if not isinstance(image_reference, str) or not image_reference.strip():
        raise SandboxImageBuildError("image reference to import must not be empty")

    image_reference = image_reference.strip()
    effective_registered_name = registered_name or _default_registered_name_from_image(
        image_reference
    )
    if effective_registered_name == _DEFAULT_IMAGE_NAME:
        warnings.warn(
            f"Importing sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
            "Pass `registered_name=...` to avoid collisions with other "
            "default-named images in this project.",
            stacklevel=2,
        )

    try:
        return _run_rust_image_import(
            image_reference,
            effective_registered_name,
            cpus=cpus,
            memory_mb=memory_mb,
            disk_mb=disk_mb,
            builder_disk_mb=builder_disk_mb,
            is_public=is_public,
            docker_compat=docker_compat,
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
            docker_compat=False,
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


def _default_registered_name_from_image(image_reference: str) -> str:
    """Mirror the Rust core's default name derivation for image imports.

    Returns the last path segment of the reference with any tag/digest
    stripped (e.g. ``pytorch/pytorch:2.4.1`` -> ``pytorch``,
    ``ghcr.io/org/app@sha256:...`` -> ``app``). Computed here so the
    ``_DEFAULT_IMAGE_NAME`` warning can trigger before the Rust call.
    """
    without_digest = image_reference.split("@", 1)[0]
    last_segment = without_digest.rsplit("/", 1)[-1]
    repo, sep, tag = last_segment.rpartition(":")
    name = repo if sep and tag else last_segment
    return name or "imported-image"
