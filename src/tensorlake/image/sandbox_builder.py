"""Sandbox-image build engine.

Takes an :class:`Image` or a Dockerfile path and delegates the build to the
Rust core (``tensorlake._cloud_sdk.build_sandbox_image``), which materializes
the image inside a builder sandbox, snapshots the filesystem, and registers
the snapshot as a named sandbox image: a CAS (content-addressed streaming)
image by default, or a legacy image with ``cas=False``. Lookups, listings
and deletes go through the Rust core's merged catalog of the CAS and legacy
image registries, so both families answer the same template-shaped record.

CAS image builds (the default) run as build operations: the build id is
reported as a ``build_accepted`` event right after admission,
``KeyboardInterrupt`` cancels the remote build through a bounded handshake
(the outcome arrives as an ``interrupted`` event before the interrupt
re-raises), and failures are the typed exceptions of
:mod:`tensorlake.image.builds`, which also exposes start/get/wait/cancel for
leaving a build running. Legacy image builds (``cas=False``) keep their
behavior: their callbacks' errors are swallowed, an interrupt is delivered
only once the native call returns and sends no cancellation, and no CAS
image request is made.

This is the programmatic backend for :meth:`Image.build` and the
``tl sbx image create`` CLI command. The Rust core owns parsing and the
warnings for instructions that run during the build but have no effect when a
sandbox runs from the image (``ONBUILD``/``SHELL``/``EXPOSE``/``HEALTHCHECK``/
``LABEL``/``STOPSIGNAL``/``VOLUME``).
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import tempfile
import warnings
from pathlib import Path
from typing import Any, Callable

from tensorlake._tracing import USER_AGENT
from tensorlake.cli._common import Context

from ._dockerfile import image_to_dockerfile
from .image import Image, _ImageBuildOperation, _ImageBuildOperationType
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


def _load_docker_config_json() -> str:
    """Capture Docker config contents before a CLI starts other work."""
    try:
        from tensorlake._cloud_sdk import load_docker_config_json
    except ImportError:
        from _cloud_sdk import load_docker_config_json

    return load_docker_config_json()


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
) -> tuple[str, str | None, str] | None:
    """Returns ``(legacy_image_name, legacy_image_id, error)`` when the name's
    CAS image was deleted but its same-named legacy image could not be, else
    ``None``. The legacy image name is the deleted CAS image's name, in the CAS
    image registry's lowercase spelling."""
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
        return client.delete_sandbox_image(image_name)
    finally:
        client.close()


def _rust_find_sandbox_image_by_name(
    api_url: str,
    token: str,
    image_name: str,
    organization_id: str | None,
    project_id: str | None,
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
    organization_id: str | None,
    project_id: str | None,
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
    cas: bool,
    build_args: dict[str, str] | None = None,
    allow_pat: bool = False,
    emit: EmitFn,
    docker_config_json: str | None = None,
) -> dict:
    ctx, token = _resolve_build_credentials(allow_pat=allow_pat)
    use_explicit_scope = allow_pat and ctx.api_key is None

    family = "CAS" if cas else "legacy"
    emit(
        {"type": "status", "message": f"Building {family} image '{registered_name}'..."}
    )

    try:
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
            ctx.organization_id if use_explicit_scope else None,
            ctx.project_id if use_explicit_scope else None,
            ctx.namespace,
            use_explicit_scope,
            USER_AGENT,
            docker_compat,
            dockerfile_text,
            context_dir,
            cas=cas,
            build_args=build_args,
            emit=_forwarder(emit),
            docker_config_json=docker_config_json,
        )
    except Exception as exc:
        # CAS only: surface the Image Service's typed outcome (conflict,
        # failed, cancelled, timeout). The legacy path keeps its generic
        # wrapping in the public function.
        if cas:
            _raise_typed_cas_error(exc)
        raise
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
    cas: bool = False,
    allow_pat: bool = False,
    emit: EmitFn,
    docker_config_json: str | None = None,
) -> dict:
    ctx, token = _resolve_build_credentials(allow_pat=allow_pat)
    use_explicit_scope = allow_pat and ctx.api_key is None

    emit(
        {
            "type": "status",
            "message": (
                f"Importing '{image_reference}' as "
                f"{'CAS' if cas else 'legacy'} image '{registered_name}'..."
            ),
        }
    )

    try:
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
            ctx.organization_id if use_explicit_scope else None,
            ctx.project_id if use_explicit_scope else None,
            ctx.namespace,
            use_explicit_scope,
            USER_AGENT,
            docker_compat,
            cas=cas,
            emit=_forwarder(emit),
            docker_config_json=docker_config_json,
        )
    except Exception as exc:
        if cas:
            _raise_typed_cas_error(exc)
        raise
    return _finish_image_registration(result_json, registered_name, emit)


def _raise_typed_cas_error(exc: BaseException) -> None:
    """Re-raise a CAS build failure as its typed :mod:`builds` exception when
    the Rust core reported one; otherwise let the caller's wrapping apply."""
    from .builds import _typed_build_error

    typed = _typed_build_error(exc)
    if typed is not None:
        raise typed from exc


def _resolve_build_credentials(*, allow_pat: bool = False) -> tuple[Context, str]:
    ctx = _build_context_from_env()
    token = ctx.api_key or (ctx.personal_access_token if allow_pat else None)
    if not token:
        if ctx.personal_access_token and not allow_pat:
            raise SandboxImageBuildError(
                "Sandbox image SDKs require TENSORLAKE_API_KEY. "
                "Personal access tokens are CLI-only."
            )
        raise SandboxImageBuildError("Missing TENSORLAKE_API_KEY credentials.")
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
    # Both build paths answer the platform template shape (camelCase keys);
    # the snake_case fallbacks keep older cores readable.
    emit(
        {
            "type": "image_registered",
            "image_id": result.get("id") or result.get("image_id", ""),
            "name": registered_name,
            "snapshot_id": result.get("snapshotId") or result.get("snapshot_id", ""),
        }
    )
    return result


# --- Public API -----------------------------------------------------------


def delete_sandbox_image(image_name: str) -> None:
    """Delete a registered CAS or legacy sandbox image by name or id.

    Both families are covered: an ``image_`` id or a name registered in the
    CAS image registry deletes a CAS image, a ``sandbox_template_`` id or a
    name registered only in the legacy image registry deletes a legacy image. Uses the
    same environment-based Tensorlake auth and namespace context as
    :func:`build_sandbox_image`.

    A name is deleted from both the CAS and the legacy image registries:
    once its CAS image is deleted, a legacy image of the same name can no
    longer start sandboxes, so it is deleted too. CAS image names ignore
    case (``MyProject`` deletes the CAS image ``myproject``), legacy image
    names match exactly. Failing to delete that
    legacy image does not raise, since the CAS image is already gone: a
    warning names the legacy image, the error and the call that deletes
    it. An ``image_`` or ``sandbox_template_`` id deletes only the image it
    names.

    Deleting removes a *published* image. An unfinished CAS image build is
    cancelled with :func:`tensorlake.image.builds.cancel_build` (or
    ``tl sbx image build cancel BUILD_ID``), never by deleting its name: a
    name whose only state is an unfinished first build has no CAS image, so
    the CAS image registry answers 404 and the lookup falls through to the
    legacy image registry as usual. A 202 (deletion still in progress)
    counts as success; a 409 ``image_build_in_progress``, a 5xx or a
    transport failure is raised as is and never falls through to the legacy
    image registry.

    Accepted merged-catalog differences from the released SDKs, for legacy
    images: this removes the legacy image from the legacy image registry
    (the released SDKs only made it unusable and left it listed), and an
    unknown name raises :class:`SandboxImageDeleteError` instead of
    succeeding silently.

    Raises:
        TypeError: ``image_name`` is not a non-empty string.
        SandboxImageDeleteError: Credentials are missing, the image is not
            found in the CAS or legacy image registry, or the delete
            request failed.
    """
    if not isinstance(image_name, str) or not image_name:
        raise TypeError("image_name must be a non-empty string")

    ctx = _build_context_from_env()
    token = ctx.api_key
    if not token:
        if ctx.personal_access_token:
            raise SandboxImageDeleteError(
                "Sandbox image SDKs require TENSORLAKE_API_KEY. "
                "Personal access tokens are CLI-only."
            )
        raise SandboxImageDeleteError("Missing TENSORLAKE_API_KEY credentials.")

    try:
        legacy_cleanup_failure = _rust_delete_sandbox_image(
            ctx.api_url,
            token,
            image_name,
            None,
            None,
            ctx.namespace,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageDeleteError(f"{type(e).__name__}: {e}") from e

    if legacy_cleanup_failure is not None:
        # The legacy image is named exactly like the deleted CAS image, in
        # lowercase; legacy names are case-sensitive, so recovery uses it.
        legacy_image_name, legacy_image_id, error = legacy_cleanup_failure
        label = f" ({legacy_image_id})" if legacy_image_id else ""
        warnings.warn(
            f"Deleted CAS image '{legacy_image_name}', but could not delete the "
            f"legacy image of the same name{label}, which can no longer start "
            f"sandboxes: {error}. Delete it with "
            f"delete_sandbox_image('{legacy_image_id or legacy_image_name}').",
            stacklevel=2,
        )


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
    """Look up a registered CAS or legacy sandbox image by its registered name or id.

    Returns the image as a dict in the sandbox-template shape (CAS images
    carry ``snapshot_format_version == "content_addressed_streaming_v1"``;
    the shape is derived from the CAS image registry's trimmed record, so no
    content digest or build provenance rides on it), or ``None`` if neither
    the CAS nor the legacy image registry knows the reference. Uses the same
    environment-based Tensorlake auth as :func:`build_sandbox_image`. Ingress
    derives project scope from the API key.

    An ``image_`` id names the *current* CAS image under its name; a
    rebuild replaces it. A build's own immutable result is read with
    :func:`tensorlake.image.builds.get_build`.

    A name is looked up in the CAS image registry in lowercase, as the
    registry stores it, and in the legacy image registry exactly as given.

    Accepted merged-catalog difference from the released SDKs, for legacy
    images: a name that fits the CAS image registry's grammar is looked up
    there first and only a clean 404 falls through to the legacy image
    registry, so CAS image registry unavailability (or a deploy 5xx) raises
    :class:`SandboxImageLookupError` for a legacy image name where the
    released SDKs touched only the legacy image registry. A
    ``sandbox_template_`` id never consults the CAS image registry.

    Raises:
        TypeError: ``image_name`` is not a non-empty string.
        SandboxImageLookupError: Credentials or project context are missing, or
            the lookup request failed.
    """
    if not isinstance(image_name, str) or not image_name:
        raise TypeError("image_name must be a non-empty string")

    ctx = _build_context_from_env()
    token = ctx.api_key
    if not token:
        if ctx.personal_access_token:
            raise SandboxImageLookupError(
                "Sandbox image SDKs require TENSORLAKE_API_KEY. "
                "Personal access tokens are CLI-only."
            )
        raise SandboxImageLookupError("Missing TENSORLAKE_API_KEY credentials.")

    try:
        result_json = _rust_find_sandbox_image_by_name(
            ctx.api_url,
            token,
            image_name,
            None,
            None,
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
    """List all registered CAS and legacy sandbox images for the current project.

    Returns every CAS and legacy image as a list of dicts in the
    sandbox-template shape (each with ``id``, ``name``, ``snapshot_id``,
    ``snapshot_format_version``, ``public``, ``created_at``, etc.), newest
    first. Uses the same environment-based Tensorlake API-key auth as
    :func:`build_sandbox_image`. Ingress derives project scope from the API key.

    Raises:
        SandboxImageLookupError: Credentials or project context are missing, or
            the list request failed.
    """
    ctx = _build_context_from_env()
    token = ctx.api_key
    if not token:
        if ctx.personal_access_token:
            raise SandboxImageLookupError(
                "Sandbox image SDKs require TENSORLAKE_API_KEY. "
                "Personal access tokens are CLI-only."
            )
        raise SandboxImageLookupError("Missing TENSORLAKE_API_KEY credentials.")

    try:
        result_json = _rust_list_sandbox_images(
            ctx.api_url,
            token,
            None,
            None,
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


def _run_op_mounts_context(op: _ImageBuildOperation) -> bool:
    """True when a ``RUN`` op bind-mounts the build context (reads host files).

    A BuildKit ``RUN --mount=type=bind`` reads from the build context unless it
    mounts ``from=<stage/image>``. ``bind`` is the default mount type, so a
    mount with no explicit ``type`` counts too. Other mount types
    (``cache``/``tmpfs``/``secret``/``ssh``) don't read the build context.
    """
    if op.type is not _ImageBuildOperationType.RUN:
        return False
    mount = (op.options or {}).get("mount")
    if not mount:
        return False
    fields: dict[str, str] = {}
    for token in mount.split(","):
        key, _, value = token.partition("=")
        fields[key.strip()] = value.strip()
    if fields.get("type", "bind") != "bind":
        return False
    return "from" not in fields


def _op_references_host_files(op: _ImageBuildOperation) -> bool:
    """True when a build op reads files from the host build context.

    ``COPY``/``ADD`` with host sources need a context; a ``RUN`` with a
    ``--mount=type=bind`` (the default type) reads it too. ``COPY --from=<stage>``,
    remote ``ADD <url>``, ``from=`` bind mounts, and non-bind mounts don't.
    """
    if op.type is _ImageBuildOperationType.RUN:
        return _run_op_mounts_context(op)
    if op.type not in (_ImageBuildOperationType.COPY, _ImageBuildOperationType.ADD):
        return False
    if "from" in (op.options or {}):
        return False
    if op.type == _ImageBuildOperationType.ADD and op.args[0].startswith(
        ("http://", "https://")
    ):
        return False
    return True


def _image_requires_context(image: Image) -> bool:
    """True if the image has ops that read files from the host.

    Such builds need a build context to resolve those sources (COPY/ADD host
    sources, or a RUN bind mount), so the caller must pass ``context_dir``
    (mirroring ``docker build <context>``).
    """
    return any(_op_references_host_files(op) for op in image._build_operations)


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
    cas: bool = True,
    build_args: dict[str, str] | None = None,
    verbose: bool = False,
    emit: EmitFn | None = None,
    _allow_pat: bool = False,
    docker_config_json: str | None = None,
) -> dict:
    """Build a CAS or legacy sandbox image from an :class:`Image` or a Dockerfile path.

    Renders the source to a Dockerfile (for :class:`Image` inputs) and hands
    the Dockerfile path/text plus build context off to the Rust core, which
    parses, validates, materializes, and registers the image. Builds a CAS
    image by default and a legacy image with ``cas=False``, with the same
    build and registration semantics as ``tl sbx image create`` and
    ``tl sbx image create --no-cas``.

    Args:
        source: An :class:`Image` instance or a path to a Dockerfile.
        registered_name: Name to register the image under. Defaults to the
            image's ``name`` or the Dockerfile stem (the parent directory's
            name for a file named ``Dockerfile``). A CAS image name
            is lowercased, as the CAS image registry stores it; a default CAS
            image name is also made valid (other characters become ``-``).
            Legacy image names are kept as given.
        cpus: CPUs for the build sandbox.
        memory_mb: Memory for the build sandbox in MiB.
        disk_mb: Root disk size for the generated sandbox image in MiB.
        builder_disk_mb: Root disk size for the temporary builder sandbox in
            MiB. Legacy images only; a CAS image build sizes its builder from
            ``disk_mb``.
        is_public: Make the registered image publicly accessible.
        docker_compat: Use Docker/BuildKit max compatibility mode (build is
            slower and uses more memory and disk space on builder sandbox).
            Legacy images only; a CAS image build always runs BuildKit.
        context_dir: Build context directory for an :class:`Image` source,
            used exactly like ``docker build <context_dir>``: it is uploaded
            as-is and COPY/ADD sources resolve relative to it. Required when
            the image has COPY/ADD ops that read host files — building without
            it then raises. When the image has no such ops it may be omitted,
            and an empty context (just the generated Dockerfile) is uploaded so
            cwd is not archived. Ignored when ``source`` is a Dockerfile path
            (the Dockerfile's parent directory is used instead).
        cas: Build a CAS (content-addressed streaming) image, the default.
            Pass ``False`` to build a legacy image instead.
        build_args: Dockerfile build args (``--build-arg KEY=VALUE``),
            forwarded to BuildKit and expanded in ``FROM`` references. CAS
            images only; a legacy image build with build args is rejected. For an
            :class:`Image` source, the generated Dockerfile declares these
            arguments for ``FROM`` and subsequent build operations.
        docker_config_json: Docker config JSON contents for this call. Omit to
            discover local config at entry; pass "{}" to disable discovery.
        verbose: Print progress to stderr. Ignored if ``emit`` is provided.
        emit: Callback invoked for each build event. Takes precedence over
            ``verbose``. Use this to integrate the builder into custom UIs.

    Returns:
        The registered image in the sandbox-template shape (camelCase keys),
        for both families.

    Raises:
        SandboxImageLoadError: The source Dockerfile or Image could not be
            loaded.
        SandboxImageBuildError: The build failed during parsing, provisioning,
            materialization, or registration.
        TypeError: ``source`` is neither an ``Image`` nor a path.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    with contextlib.ExitStack() as stack:
        if isinstance(source, Image):
            if not source._base_image:
                raise SandboxImageLoadError("Image must have a base_image to build")
            if context_dir is not None:
                # Upload the given directory as-is, like `docker build <dir>`.
                rust_context_dir = str(Path(context_dir).resolve())
            elif _image_requires_context(source):
                raise SandboxImageBuildError(
                    "This image reads files from the host (via COPY/ADD or a "
                    "RUN bind mount), so it needs a build context. Pass "
                    "`context_dir=...` pointing at the directory that contains "
                    "those sources; they resolve relative to it, like "
                    "`docker build <context_dir>`."
                )
            else:
                # No host-file copies: upload an empty context (just the
                # generated Dockerfile) instead of archiving the whole cwd.
                rust_context_dir = stack.enter_context(
                    tempfile.TemporaryDirectory(prefix="tl-build-context-")
                )
            rust_dockerfile_path = str(Path(rust_context_dir) / "Dockerfile")
            dockerfile_text = image_to_dockerfile(source, build_args=build_args)
            if not dockerfile_text.endswith("\n"):
                dockerfile_text += "\n"
            effective_registered_name = _registered_image_name(
                registered_name or source.name, None, cas=cas
            )
        elif isinstance(source, (str, os.PathLike)):
            path = Path(os.fspath(source)).resolve()
            if not path.is_file():
                raise SandboxImageLoadError(f"Dockerfile not found: {source}")
            rust_dockerfile_path = str(path)
            rust_context_dir = None
            dockerfile_text = None
            effective_registered_name = _registered_image_name(
                registered_name, _default_registered_name(str(path)), cas=cas
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
                cas=cas,
                build_args=dict(build_args) if build_args else None,
                docker_config_json=docker_config_json,
                allow_pat=_allow_pat,
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
    cas: bool = True,
    verbose: bool = False,
    emit: EmitFn | None = None,
    _allow_pat: bool = False,
    docker_config_json: str | None = None,
) -> dict:
    """Import a registry image directly into a CAS or legacy sandbox image — no Docker.

    Unlike :func:`build_sandbox_image`, there is no Dockerfile and no build
    context: the builder pulls the referenced image's layers and applies them
    straight into the rootfs (via ``oci-image-to-ext4``), bypassing the Docker
    daemon entirely. This is the programmatic backend for the
    ``tl sbx image import`` CLI command. Imports a CAS image by default and a
    legacy image with ``cas=False``. The import is always a fresh base from
    the registry — the reference is never resolved against the CAS or legacy
    image registries.

    Args:
        image_reference: The registry image reference to import, e.g.
            ``pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime`` or
            ``ghcr.io/org/app@sha256:...``.
        registered_name: Name to register the image under. Defaults to the
            image reference's last path segment with any tag/digest stripped
            (e.g. ``pytorch/pytorch:2.4.1`` -> ``pytorch``). A CAS image name
            is lowercased, as the CAS image registry stores it; a default CAS
            image name is also made valid (other characters become ``-``).
            Legacy image names are kept as given.
        cpus: CPUs for the build sandbox.
        memory_mb: Memory for the build sandbox in MiB.
        disk_mb: Root disk size for the generated sandbox image in MiB.
        builder_disk_mb: Root disk size for the temporary builder sandbox in
            MiB. Legacy images only; a CAS image import sizes its builder from
            ``disk_mb``.
        is_public: Make the registered image publicly accessible.
        docker_compat: Use Docker/BuildKit max compatibility mode (import is
            slower and uses more memory and disk space on builder sandbox).
            Legacy images only.
        cas: Import as a CAS (content-addressed streaming) image, the
            default. Pass ``False`` to import a legacy image instead.
        docker_config_json: Docker config JSON contents for this call. Omit to
            discover local config at entry; pass "{}" to disable discovery.
        verbose: Print progress to stderr. Ignored if ``emit`` is provided.
        emit: Callback invoked for each build event. Takes precedence over
            ``verbose``.

    Returns:
        The registered image in the sandbox-template shape (camelCase keys),
        for both families.

    Raises:
        SandboxImageBuildError: The image reference is empty, or the import
            failed during provisioning, materialization, or registration.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    if not isinstance(image_reference, str) or not image_reference.strip():
        raise SandboxImageBuildError("image reference to import must not be empty")

    image_reference = image_reference.strip()
    effective_registered_name = _registered_image_name(
        registered_name,
        _default_registered_name_from_image(image_reference),
        cas=cas,
        fallback=_DEFAULT_IMPORTED_IMAGE_NAME,
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
            cas=cas,
            docker_config_json=docker_config_json,
            allow_pat=_allow_pat,
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
    _allow_pat: bool = False,
    docker_config_json: str | None = None,
) -> dict:
    """Build an Applications runtime image as a legacy sandbox image.

    Unlike :func:`build_sandbox_image`, this uses the Applications Dockerfile
    wrapper so deployed functions get the SDK runtime, default workdir, and
    deploy-time build environment variables. Applications runtime images stay
    legacy images while CAS is the default for sandbox images.
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
            cas=False,
            docker_config_json=docker_config_json,
            allow_pat=_allow_pat,
            emit=emit,
        )
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageBuildError(f"{type(e).__name__}: {e}") from e


_DEFAULT_SANDBOX_IMAGE_NAME = "sandbox-image"
_DEFAULT_IMPORTED_IMAGE_NAME = "imported-image"
_CAS_NAME_SEPARATORS = "._-"
_RESERVED_IMAGE_NAME_PREFIXES = ("sandbox_template_", "image_", "cas-v1:")
_MAX_CAS_IMAGE_NAME_LENGTH = 255


def _registered_image_name(
    explicit: str | None,
    generated: str | None,
    *,
    cas: bool,
    fallback: str = _DEFAULT_SANDBOX_IMAGE_NAME,
) -> str:
    """Mirror the Rust core's ``registered_image_name``: the name a build or
    import registers, decided before anything is shown or sent.

    A CAS image takes the CAS image registry's spelling: an explicit name is
    lowercased as the registry would, and a generated default is turned into
    a valid CAS image name. Legacy image names are kept as they are.
    """
    if explicit or generated is None:
        name = explicit or ""
        return _canonical_cas_image_name(name) if cas else name
    return _cas_image_name_from(generated, fallback) if cas else generated


def _canonical_cas_image_name(name: str) -> str:
    """Mirror the Rust core's ``canonical_cas_image_name``: the CAS image
    registry ASCII-lowercases every name, so ``MyProject`` and ``myproject``
    name one CAS image."""
    return "".join(chr(ord(c) + 32) if "A" <= c <= "Z" else c for c in name)


def _cas_image_name_from(raw: str, fallback: str) -> str:
    """Mirror the Rust core's ``cas_image_name_from``: a valid CAS image name
    for a generated default.

    The name is ASCII-lowercased and kept if that makes it valid. Otherwise
    every character other than ``a-z``, ``0-9``, ``.``, ``_`` and ``-``
    becomes ``-``, each run of separators collapses to its first character,
    leading and trailing separators are dropped and the result is capped at
    255 characters. ``fallback`` is used when no valid name remains.
    """
    lowered = _canonical_cas_image_name(raw)
    if _is_addressable_cas_image_name(lowered):
        return lowered
    chars: list[str] = []
    for c in lowered:
        if not ("a" <= c <= "z" or "0" <= c <= "9" or c in _CAS_NAME_SEPARATORS):
            c = "-"
        if c in _CAS_NAME_SEPARATORS and chars and chars[-1] in _CAS_NAME_SEPARATORS:
            continue
        chars.append(c)
    name = "".join(chars).strip(_CAS_NAME_SEPARATORS)
    name = name[:_MAX_CAS_IMAGE_NAME_LENGTH].rstrip(_CAS_NAME_SEPARATORS)
    return name if _is_addressable_cas_image_name(name) else fallback


def _is_addressable_cas_image_name(name: str) -> bool:
    """Mirror the Rust core's CAS image name grammar: valid in the CAS image
    registry and not mistaken for a content digest."""
    if (
        not name
        or len(name) > _MAX_CAS_IMAGE_NAME_LENGTH
        or name.startswith(_RESERVED_IMAGE_NAME_PREFIXES)
    ):
        return False
    if len(name) == 64 and all(c in "0123456789abcdef" for c in name):
        return False
    repository, has_tag, tag = name.partition(":")
    if not all(_is_cas_name_component(c) for c in repository.split("/")):
        return False
    if has_tag:
        return (
            0 < len(tag) <= 128
            and all(
                "a" <= c <= "z" or "0" <= c <= "9" or c in _CAS_NAME_SEPARATORS
                for c in tag
            )
            and tag[0] not in ".-"
        )
    return True


def _is_cas_name_component(component: str) -> bool:
    if not component:
        return False
    previous_was_separator = True  # rejects a leading separator
    for c in component:
        is_separator = c in _CAS_NAME_SEPARATORS
        if is_separator:
            if previous_was_separator:
                return False
        elif not ("a" <= c <= "z" or "0" <= c <= "9"):
            return False
        previous_was_separator = is_separator
    return not previous_was_separator  # rejects a trailing separator


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
        return parent_name or _DEFAULT_SANDBOX_IMAGE_NAME
    return stem or _DEFAULT_SANDBOX_IMAGE_NAME


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
    return name or _DEFAULT_IMPORTED_IMAGE_NAME
