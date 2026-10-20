"""Image Service build operations for CAS sandbox images.

A CAS (content-addressed streaming) image is built or imported by an Image
Service *build operation*. This module exposes that operation directly:

- :func:`start_build` / :func:`start_import` admit a build (and upload and
  seal its context) and return an :class:`ImageBuildHandle` without waiting,
  so the operation can be left running.
- :func:`get_build` looks a build up by its service-generated build id.
- :meth:`ImageBuildHandle.refresh`, :meth:`ImageBuildHandle.wait` and
  :meth:`ImageBuildHandle.cancel` read, wait for and cancel it. Module-level
  :func:`wait_build` and :func:`cancel_build` do the same from a build id.

**CAS only.** Every function here addresses the Image Service. Legacy
images (``build_sandbox_image(..., cas=False)``, ``sandbox_template_`` ids,
``.tlsnap`` snapshots) have no build operation: :func:`start_build` and
:func:`start_import` are CAS by construction, and a ``sandbox_template_`` or
``image_`` reference passed as a build id is refused locally, before any
request. The legacy functions in :mod:`tensorlake.image.sandbox_builder`
keep their own requests, results, errors, callback handling and interruption
behavior.

Response enum fields map unfamiliar wire values to ``UNKNOWN``. ``raw`` is
diagnostic only and excluded from equality, hashing, and SDK serialization
(``to_dict()`` for build records). Events retain their original wire strings.

**Build ids are the immutable record.** An image name and its ``image_`` id
name the *current* image; a rebuild replaces what they point at. A build id
and its result (``ImageBuildInfo.result``) never change, so
:meth:`ImageBuildHandle.wait` returns this build's own result even after the
image was rebuilt or deleted.

**Cancellation by build id is sufficient.** Admission returns the build id,
the CLI prints it, and the ``image_build_in_progress`` conflict names it as
``active_build_id``. The client request id (the admission's
``Idempotency-Key``) is kept on the handle only as a recovery aid for an
admission whose response was lost; nothing here requires it.

**Interrupts and deadlines.** ``KeyboardInterrupt`` during
:func:`start_build`, :func:`start_import` or a wait requests remote
cancellation through a bounded handshake (30 s), delivers the outcome as an
``interrupted`` event (``outcome`` is ``cancelled``, ``already_completed``,
``already_cancelled``, ``already_failed``, ``not_admitted`` or
``unconfirmed``), then re-raises ``KeyboardInterrupt``. A second interrupt
forces an immediate exit; the event then says the remote cancellation might
not be confirmed. Disconnecting, losing the process or dropping a handle
never cancels the remote build. A wait ``timeout`` only stops waiting: it
raises :class:`ImageBuildTimeoutError` with the build id and leaves the
build running. Waits otherwise run until the service-reported operation
deadline plus a deployment-recovery allowance, reconnecting across transient
Image Service failures.

A raising ``emit`` callback stops the operation locally and propagates; it
is not a cancel request. (The legacy builder's callbacks keep being
swallowed, as before.)
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
import warnings
from dataclasses import dataclass, field, fields, replace
from pathlib import Path
from typing import Any, Callable

from tensorlake._enums import ResponseEnum
from tensorlake._tracing import USER_AGENT

from ._dockerfile import image_to_dockerfile
from .image import Image
from .sandbox_builder import (
    _DEFAULT_IMAGE_NAME,
    SandboxImageBuildError,
    SandboxImageLoadError,
    _build_context_from_env,
    _default_registered_name,
    _default_registered_name_from_image,
    _image_requires_context,
    _noop_emit,
    _snake_case_keys,
    _stderr_emit,
)

EmitFn = Callable[[dict], None]

__all__ = [
    "CancelBuildOutcome",
    "ImageBuildStatus",
    "ImageBuildPhase",
    "ImageBuildKind",
    "ImageBuildCancellationOutcome",
    "ImageBuildErrorCode",
    "ImageBuildCancelledError",
    "ImageBuildError",
    "ImageBuildFailedError",
    "ImageBuildHandle",
    "ImageBuildInProgressError",
    "ImageBuildInfo",
    "ImageBuildInterruptedError",
    "ImageBuildNotFoundError",
    "ImageBuildTimeoutError",
    "cancel_build",
    "get_build",
    "start_build",
    "start_import",
    "wait_build",
]


class ImageBuildStatus(ResponseEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    UNKNOWN = "unknown"


class ImageBuildPhase(ResponseEnum):
    AWAITING_CONTEXT = "awaiting_context"
    STARTING_BUILDER = "starting_builder"
    EXECUTING = "executing"
    SNAPSHOTTING = "snapshotting"
    PUBLISHING = "publishing"
    FINISHED = "finished"
    UNKNOWN = "unknown"


class ImageBuildKind(ResponseEnum):
    DOCKERFILE = "dockerfile"
    IMPORT = "import"
    DIRECT = "direct"
    UNKNOWN = "unknown"


class ImageBuildCancellationOutcome(ResponseEnum):
    CANCELLED = "cancelled"
    ALREADY_CANCELLED = "already_cancelled"
    ALREADY_COMPLETED = "already_completed"
    ALREADY_FAILED = "already_failed"
    NOT_ADMITTED = "not_admitted"
    UNCONFIRMED = "unconfirmed"
    UNKNOWN = "unknown"

    @property
    def confirmed(self) -> bool:
        return self in (self.CANCELLED, self.ALREADY_CANCELLED, self.ALREADY_FAILED)


class ImageBuildErrorCode(ResponseEnum):
    USAGE = "usage"
    AUTH = "auth"
    NOT_FOUND = "not_found"
    IMAGE_BUILD_IN_PROGRESS = "image_build_in_progress"
    CLIENT_REQUEST_ID_MISMATCH = "client_request_id_mismatch"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"
    SERVICE = "service"
    INTERNAL = "internal"
    UNKNOWN = "unknown"


# --- Exceptions -------------------------------------------------------------


class ImageBuildError(SandboxImageBuildError):
    """An Image Service build operation failed.

    Attributes:
        code: Stable machine-readable code (``usage``, ``auth``,
            ``not_found``, ``image_build_in_progress``,
            ``client_request_id_mismatch``, ``failed``, ``cancelled``,
            ``timeout``, ``interrupted``, ``service``, ``internal``).
        build_id: The build this error is about, when known.
        client_request_id: The admission's request id, when relevant.
        details: Additional structured error fields; ``info`` retains its
            response dictionary while failure/cancellation errors expose a
            parsed record on ``info``.
        raw: The original response from the Rust core, for diagnostics.
    """

    def __init__(
        self,
        message: str,
        *,
        code: ImageBuildErrorCode = ImageBuildErrorCode.INTERNAL,
        **details: Any,
    ):
        super().__init__(message)
        self.code = ImageBuildErrorCode(code)
        self.raw: dict = {}
        self.details = details
        for name, enum in _RESPONSE_ENUMS[ImageBuildError].items():
            if name in details and details[name] is not None:
                details[name] = enum(details[name])
        if "confirmed" in details:
            outcome = details.get("outcome")
            details["confirmed"] = (
                details["confirmed"] is True
                and isinstance(outcome, ImageBuildCancellationOutcome)
                and outcome.confirmed
            )
        self.build_id: str | None = details.get("build_id")
        self.client_request_id: str | None = details.get("client_request_id")


class ImageBuildNotFoundError(ImageBuildError):
    """No such build is visible to the project: unknown, foreign or retired.

    A finished build is kept only while it is its image's current build; a
    later build of the image or deleting the image retires it, after which
    ``get_build``, ``wait_build`` and ``cancel_build`` raise this error.
    """

    def __init__(self, message: str, **details: Any):
        code = ImageBuildErrorCode(details.pop("code", ImageBuildErrorCode.NOT_FOUND))
        if code is not ImageBuildErrorCode.NOT_FOUND:
            raise ValueError("ImageBuildNotFoundError code must be NOT_FOUND")
        super().__init__(message, code=ImageBuildErrorCode.NOT_FOUND, **details)


class ImageBuildInProgressError(ImageBuildError):
    """The image name already has an unfinished build or import.

    Cancel it with its ``active_build_id`` before starting another; the SDK
    never cancels or supersedes it automatically.
    """

    def __init__(self, message: str, **details: Any):
        super().__init__(
            message, code=ImageBuildErrorCode.IMAGE_BUILD_IN_PROGRESS, **details
        )
        self.image_name: str | None = details.get("image_name")
        self.active_build_id: str | None = details.get("active_build_id")
        self.active_kind: ImageBuildKind | None = self.details.get("active_kind")
        self.active_status: ImageBuildStatus | None = self.details.get("active_status")


class ImageBuildFailedError(ImageBuildError):
    """The service reports the build failed. ``error`` is its message."""

    def __init__(self, message: str, **details: Any):
        super().__init__(message, code=ImageBuildErrorCode.FAILED, **details)
        self.error: str = details.get("error") or "no error recorded"
        self.info: ImageBuildInfo | None = _info_or_none(details.get("info"))


class ImageBuildCancelledError(ImageBuildError):
    """The service reports the build was cancelled (a distinct outcome)."""

    def __init__(self, message: str, **details: Any):
        super().__init__(message, code=ImageBuildErrorCode.CANCELLED, **details)
        self.info: ImageBuildInfo | None = _info_or_none(details.get("info"))


class ImageBuildTimeoutError(ImageBuildError):
    """The caller stopped waiting. The build may still be running; it was
    neither failed nor cancelled."""

    def __init__(self, message: str, **details: Any):
        super().__init__(message, code=ImageBuildErrorCode.TIMEOUT, **details)


class ImageBuildInterruptedError(ImageBuildError):
    """An explicit interruption ran the cancellation handshake.

    ``outcome`` is ``cancelled``, ``already_completed``, ``already_cancelled``,
    ``already_failed``, ``not_admitted`` or ``unconfirmed``; ``confirmed`` is
    true only when the remote build is known not to publish.
    """

    def __init__(self, message: str, **details: Any):
        super().__init__(message, code=ImageBuildErrorCode.INTERRUPTED, **details)
        self.outcome: ImageBuildCancellationOutcome = (
            self.details.get("outcome") or ImageBuildCancellationOutcome.UNCONFIRMED
        )
        self.confirmed: bool = self.details.get("confirmed") is True
        self.result: dict | None = details.get("result")


def _info_or_none(raw: Any) -> ImageBuildInfo | None:
    if isinstance(raw, ImageBuildInfo):
        return raw
    if not isinstance(raw, dict):
        return None
    try:
        return ImageBuildInfo.from_dict(raw)
    except ImageBuildError:
        return None


_TYPED_ERRORS: dict[ImageBuildErrorCode, type[ImageBuildError]] = {
    ImageBuildErrorCode.NOT_FOUND: ImageBuildNotFoundError,
    ImageBuildErrorCode.IMAGE_BUILD_IN_PROGRESS: ImageBuildInProgressError,
    ImageBuildErrorCode.FAILED: ImageBuildFailedError,
    ImageBuildErrorCode.CANCELLED: ImageBuildCancelledError,
    ImageBuildErrorCode.TIMEOUT: ImageBuildTimeoutError,
    ImageBuildErrorCode.INTERRUPTED: ImageBuildInterruptedError,
}


def _typed_build_error(exc: BaseException) -> ImageBuildError | None:
    """Decode the Rust core's ``("image_build", None, json)`` payload."""
    args = getattr(exc, "args", ())
    if len(args) != 3 or args[0] != "image_build" or not isinstance(args[2], str):
        return None
    try:
        payload = _decode_response(args[2], ImageBuildError)
    except ImageBuildError:
        return None
    code = payload["code"]
    message = str(payload.get("message") or exc)
    details = {
        key: value
        for key, value in payload.items()
        if key not in ("message", "code", "raw")
    }
    cls = _TYPED_ERRORS.get(code)
    error = (
        cls(message, **details)
        if cls
        else ImageBuildError(message, code=code, **details)
    )
    error.raw = payload["raw"]
    return error


def _raise_typed(exc: BaseException) -> None:
    typed = _typed_build_error(exc)
    if typed is not None:
        if isinstance(typed, ImageBuildInterruptedError) and isinstance(
            typed.result, dict
        ):
            typed.result = _snake_case_keys(typed.result)
            typed.details["result"] = typed.result
        raise typed from exc
    raise ImageBuildError(f"{type(exc).__name__}: {exc}") from exc


# --- Models -----------------------------------------------------------------


@dataclass(frozen=True)
class ImageBuildInfo:
    """One build or import as the Image Service reports it.

    Unknown fields are kept in :attr:`raw`. ``status`` is ``pending``,
    ``running``, ``succeeded``, ``failed`` or ``cancelled``; ``phase`` is
    ``awaiting_context``, ``starting_builder``, ``executing``,
    ``snapshotting``, ``publishing`` or ``finished`` when known. ``result``
    is the published image of a succeeded build in the sandbox-template
    shape with snake_case keys: the same dict
    :func:`tensorlake.image.sandbox_builder.find_sandbox_image_by_name`
    returns for that image. The service exposes no cleanup state, phase
    deadline, content digest or upload capability on a build.
    """

    build_id: str
    status: ImageBuildStatus
    kind: ImageBuildKind | None = None
    name: str | None = None
    phase: ImageBuildPhase | None = None
    attempt_no: int = 0
    client_request_id: str | None = None
    builder_sandbox_id: str | None = None
    error: str | None = None
    result: dict | None = field(default=None, hash=False)
    operation_deadline_ms: int | None = None
    created_at_ms: int | None = None
    updated_at_ms: int | None = None
    raw: dict = field(default_factory=dict, repr=False, compare=False)

    @property
    def is_terminal(self) -> bool:
        return self.status in (
            ImageBuildStatus.SUCCEEDED,
            ImageBuildStatus.FAILED,
            ImageBuildStatus.CANCELLED,
        )

    @classmethod
    def from_dict(cls, raw: dict | str) -> "ImageBuildInfo":
        raw = _decode_response(raw, cls)
        result = raw.get("result")
        # The core hands the result over in the template shape (camelCase);
        # the Python SDK presents snake_case keys everywhere.
        normalized = _snake_case_keys(result) if isinstance(result, dict) else None
        return cls(
            build_id=str(raw.get("build_id") or ""),
            status=raw["status"],
            kind=raw.get("kind"),
            name=raw.get("name"),
            phase=raw.get("phase"),
            attempt_no=raw["attempt_no"],
            client_request_id=raw.get("client_request_id"),
            builder_sandbox_id=raw.get("builder_sandbox_id"),
            error=raw.get("error"),
            result=normalized,
            operation_deadline_ms=raw.get("operation_deadline_ms"),
            created_at_ms=raw.get("created_at_ms"),
            updated_at_ms=raw.get("updated_at_ms"),
            raw=raw["raw"],
        )

    def to_dict(self) -> dict:
        """Serialize model fields; diagnostic raw data is never sent back."""
        return {f.name: getattr(self, f.name) for f in fields(self) if f.name != "raw"}


@dataclass(frozen=True)
class CancelBuildOutcome:
    """The service's answer to a cancellation.

    ``outcome`` is ``cancelled`` (durable), ``already_cancelled``,
    ``already_completed`` (publication won; the result stands),
    ``already_failed`` or ``not_admitted`` (nothing is indexed under the
    client request id: an observation, not a cancellation, and no promise
    about a delayed admission). ``build`` is the observed record when any.
    """

    outcome: ImageBuildCancellationOutcome
    build: ImageBuildInfo | None = None
    raw: dict = field(default_factory=dict, repr=False, compare=False)

    @property
    def confirmed(self) -> bool:
        """Whether the build is known not to publish."""
        return self.outcome.confirmed

    @classmethod
    def from_dict(cls, raw: dict | str) -> "CancelBuildOutcome":
        raw = _decode_response(raw, cls)
        return cls(
            outcome=raw["outcome"],
            build=_info_or_none(raw.get("build")),
            raw=raw["raw"],
        )

    def to_dict(self) -> dict:
        return {
            "outcome": self.outcome,
            "build": self.build.to_dict() if self.build else None,
        }


_RESPONSE_ENUMS = {
    ImageBuildInfo: {
        "status": ImageBuildStatus,
        "kind": ImageBuildKind,
        "phase": ImageBuildPhase,
    },
    CancelBuildOutcome: {"outcome": ImageBuildCancellationOutcome},
    ImageBuildError: {
        "code": ImageBuildErrorCode,
        "active_kind": ImageBuildKind,
        "active_status": ImageBuildStatus,
        "outcome": ImageBuildCancellationOutcome,
    },
}


def _decode_response(response: dict | str, model: type = dict) -> dict:
    """Decode native JSON once. raw is diagnostic and never reconstructs state."""
    try:
        raw = json.loads(response) if isinstance(response, str) else response
        if not isinstance(raw, dict):
            raise ValueError("expected a JSON object")
        family = next((base for base in model.__mro__ if base in _RESPONSE_ENUMS), None)
        if family is None:
            return raw
        if family is ImageBuildInfo and raw.get("status") is None:
            raise ValueError("build response must include status")
        enums = _RESPONSE_ENUMS[family]
        decoded = {**raw, "raw": dict(raw)}
        for name, enum in enums.items():
            optional = name in ("kind", "phase", "active_kind", "active_status") or (
                family is ImageBuildError and name == "outcome"
            )
            if optional and name not in raw:
                continue
            value = raw.get(name)
            if name == "code":
                value = value or "internal"
            decoded[name] = (
                None if optional and not value else enum.from_response(value)
            )
        if family is ImageBuildInfo:
            attempt = raw.get("attempt_no")
            if attempt is None:
                attempt = 0
            if isinstance(attempt, bool) or not isinstance(attempt, (int, float, str)):
                raise ValueError("attempt_no must be a safe integer")
            number = float(attempt)
            if not number.is_integer() or abs(number) > 2**53 - 1:
                raise ValueError("attempt_no must be a safe integer")
            decoded["attempt_no"] = int(number)
        return decoded
    except (ValueError, TypeError, OverflowError) as exc:
        error = ImageBuildError(
            f"Invalid image-build response: {exc}", code=ImageBuildErrorCode.SERVICE
        )
        error.raw = {"response": response}
        raise error from exc


# --- Native delegation -------------------------------------------------------


def _native():
    try:
        from tensorlake import _cloud_sdk
    except ImportError:  # pragma: no cover - source checkout without the wheel
        import _cloud_sdk  # type: ignore[no-redef]

    return _cloud_sdk


def _rust_start_image_build(*args, **kwargs) -> str:
    return _native().start_image_build(*args, **kwargs)


def _rust_start_image_import(*args, **kwargs) -> str:
    return _native().start_image_import(*args, **kwargs)


def _rust_get_image_build(*args, **kwargs) -> str:
    return _native().get_image_build(*args, **kwargs)


def _rust_wait_image_build(*args, **kwargs) -> str:
    return _native().wait_image_build(*args, **kwargs)


def _rust_cancel_image_build(*args, **kwargs) -> str:
    return _native().cancel_image_build(*args, **kwargs)


def _credentials() -> tuple[str, str, str]:
    """``(api_url, token, namespace)`` from the environment; API keys only."""
    ctx = _build_context_from_env()
    token = ctx.api_key
    if not token:
        if ctx.personal_access_token:
            raise ImageBuildError(
                "Sandbox image SDKs require TENSORLAKE_API_KEY. "
                "Personal access tokens are CLI-only.",
                code=ImageBuildErrorCode.AUTH,
            )
        raise ImageBuildError(
            "Missing TENSORLAKE_API_KEY credentials.", code=ImageBuildErrorCode.AUTH
        )
    return ctx.api_url, token, ctx.namespace


def _reject_legacy_reference(build_id: str) -> None:
    """New operations never reinterpret legacy references; fail before any
    request so no Image Service or platform call is made."""
    if not isinstance(build_id, str) or not build_id.strip():
        raise ImageBuildError(
            "build_id must be a non-empty string", code=ImageBuildErrorCode.USAGE
        )
    if build_id.startswith("sandbox_template_"):
        raise ImageBuildError(
            f"'{build_id}' is a legacy sandbox template id; legacy images have no "
            "build operation to get, wait for or cancel",
            code=ImageBuildErrorCode.USAGE,
            build_id=build_id,
        )
    if build_id.startswith("image_"):
        raise ImageBuildError(
            f"'{build_id}' is an image id, not a build id; look the image up with "
            "find_sandbox_image_by_name or use the build id admission returned",
            code=ImageBuildErrorCode.USAGE,
            build_id=build_id,
        )


def _decode_started(result_json: str) -> tuple[ImageBuildInfo, str | None]:
    payload = _decode_response(result_json)
    build = payload.get("build")
    if (
        not isinstance(build, dict)
        or not isinstance(build.get("build_id"), str)
        or not build["build_id"].strip()
    ):
        error = ImageBuildError(
            "Invalid image-build admission: expected a build with a non-empty build_id",
            code=ImageBuildErrorCode.SERVICE,
        )
        error.raw = {"response": payload}
        raise error
    info = ImageBuildInfo.from_dict(build)
    return info, payload.get("client_request_id")


# --- Handle -----------------------------------------------------------------


class ImageBuildHandle:
    """One admitted CAS build. Holds the build identity and the caller's
    credentials; creating or dropping it performs no network I/O."""

    def __init__(
        self,
        info: ImageBuildInfo,
        *,
        client_request_id: str | None = None,
        _credentials: tuple[str, str, str] | None = None,
    ):
        self._info = info
        self._client_request_id = client_request_id or info.client_request_id
        self._creds = _credentials

    def __repr__(self) -> str:
        return (
            f"ImageBuildHandle(build_id={self.build_id!r}, status={self.info.status!r})"
        )

    @property
    def build_id(self) -> str:
        return self._info.build_id

    @property
    def client_request_id(self) -> str | None:
        """The admission's request id, when this handle started the build.
        Never needed to cancel; a recovery aid for a lost admission answer."""
        return self._client_request_id

    @property
    def info(self) -> ImageBuildInfo:
        """The last observed state."""
        return self._info

    def _credentials_or_env(self) -> tuple[str, str, str]:
        if self._creds is None:
            self._creds = _credentials()
        return self._creds

    def refresh(self) -> ImageBuildInfo:
        """Re-read the current state."""
        api_url, token, namespace = self._credentials_or_env()
        try:
            raw = _rust_get_image_build(
                api_url,
                token,
                self.build_id,
                None,
                None,
                namespace,
                False,
                USER_AGENT,
            )
        except Exception as exc:  # noqa: BLE001 - re-raised typed below
            _raise_typed(exc)
        self._info = ImageBuildInfo.from_dict(raw)
        return self._info

    def wait(
        self,
        *,
        timeout: float | None = None,
        follow_logs: bool = False,
        verbose: bool = False,
        emit: EmitFn | None = None,
    ) -> dict:
        """Wait for publication and return the build's immutable result.

        Args:
            timeout: Caller bound in seconds, applied on top of the
                service-informed deadline. Expiry raises
                :class:`ImageBuildTimeoutError` and leaves the build running.
            follow_logs: Stream ``tl-image-builder`` output as ``build_log``
                events while an attempt runs.
            verbose: Print events to stderr when ``emit`` is not given.
            emit: Event callback. A raising callback stops the wait locally
                and propagates; it does not cancel the build.

        Returns:
            The published image of this build in the sandbox-template shape
            with snake_case keys (``ImageBuildInfo.result``): the same dict
            :func:`tensorlake.image.sandbox_builder.find_sandbox_image_by_name`
            returns for it.

        Raises:
            ImageBuildFailedError, ImageBuildCancelledError: Terminal
                service outcomes, distinct from each other.
            ImageBuildTimeoutError: ``timeout`` expired.
            KeyboardInterrupt: After the remote cancellation handshake ran.
        """
        if emit is None:
            emit = _stderr_emit if verbose else _noop_emit
        api_url, token, namespace = self._credentials_or_env()
        try:
            raw = _rust_wait_image_build(
                api_url,
                token,
                self.build_id,
                timeout,
                follow_logs,
                None,
                None,
                namespace,
                False,
                USER_AGENT,
                emit,
            )
        except ImageBuildError:
            raise
        except Exception as exc:  # noqa: BLE001 - re-raised typed below
            _raise_typed(exc)
        result_raw = _decode_response(raw)
        result = _snake_case_keys(result_raw)
        self._info = replace(
            self._info,
            status=ImageBuildStatus.SUCCEEDED,
            result=result,
            raw={**self._info.raw, "status": "succeeded", "result": result_raw},
        )
        return result

    def cancel(self) -> CancelBuildOutcome:
        """Cancel this build by its id (the sufficient selector)."""
        return cancel_build(self.build_id, _creds=self._credentials_or_env())


# --- Module-level operations -------------------------------------------------


def _prepare_source(
    source: Image | str,
    registered_name: str | None,
    context_dir: str | None,
    stack,
    build_args: dict[str, str] | None = None,
) -> tuple[str, str | None, str | None, str]:
    """Resolve a build source the same way :func:`build_sandbox_image` does:
    ``(dockerfile_path, dockerfile_text, context_dir, registered_name)``."""
    if isinstance(source, Image):
        if not source._base_image:
            raise SandboxImageLoadError("Image must have a base_image to build")
        if context_dir is not None:
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
            rust_context_dir = stack.enter_context(
                tempfile.TemporaryDirectory(prefix="tl-build-context-")
            )
        dockerfile_text = image_to_dockerfile(source, build_args=build_args)
        if not dockerfile_text.endswith("\n"):
            dockerfile_text += "\n"
        return (
            str(Path(rust_context_dir) / "Dockerfile"),
            dockerfile_text,
            rust_context_dir,
            registered_name or source.name,
        )
    if isinstance(source, (str, os.PathLike)):
        path = Path(os.fspath(source)).resolve()
        if not path.is_file():
            raise SandboxImageLoadError(f"Dockerfile not found: {source}")
        return (
            str(path),
            None,
            None,
            registered_name or _default_registered_name(str(path)),
        )
    raise TypeError(
        "source must be an Image or a Dockerfile path, got " f"{type(source).__name__}"
    )


def start_build(
    source: Image | str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    is_public: bool = False,
    context_dir: str | None = None,
    build_args: dict[str, str] | None = None,
    verbose: bool = False,
    emit: EmitFn | None = None,
    docker_config_json: str | None = None,
) -> ImageBuildHandle:
    """Admit a CAS image build and return without waiting.

    Parses the source like :func:`tensorlake.image.sandbox_builder.build_sandbox_image`
    with ``cas=True``, admits the build under a fresh client request id,
    uploads and seals the build context, and returns a handle whose
    :meth:`ImageBuildHandle.wait` finishes the operation. The build keeps
    running if this process exits. ``KeyboardInterrupt`` before the handle
    is returned cancels the build (or, while the admission is unresolved,
    resolves it through the request id) and re-raises after the handshake.

    Raises:
        ImageBuildInProgressError: The name already has an unfinished build;
            cancel ``active_build_id`` first.
        SandboxImageLoadError, SandboxImageBuildError: Source problems.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit
    with contextlib.ExitStack() as stack:
        dockerfile_path, dockerfile_text, rust_context_dir, name = _prepare_source(
            source, registered_name, context_dir, stack, build_args=build_args
        )
        if name == _DEFAULT_IMAGE_NAME:
            warnings.warn(
                f"Building sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
                "Pass `registered_name=...` or `Image(name=...)` to avoid collisions "
                "with other default-named images in this project.",
                stacklevel=2,
            )
        api_url, token, namespace = _credentials()
        emit({"type": "status", "message": f"Building image '{name}'..."})
        try:
            raw = _rust_start_image_build(
                api_url,
                token,
                dockerfile_path,
                name,
                disk_mb,
                cpus,
                memory_mb,
                is_public,
                None,
                None,
                namespace,
                False,
                USER_AGENT,
                dockerfile_text,
                rust_context_dir,
                dict(build_args) if build_args else None,
                emit,
                docker_config_json,
            )
        except ImageBuildError:
            raise
        except Exception as exc:  # noqa: BLE001 - re-raised typed below
            _raise_typed(exc)
    info, client_request_id = _decode_started(raw)
    return ImageBuildHandle(
        info,
        client_request_id=client_request_id,
        _credentials=(api_url, token, namespace),
    )


def start_import(
    image_reference: str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    disk_mb: int | None = None,
    is_public: bool = False,
    verbose: bool = False,
    emit: EmitFn | None = None,
    docker_config_json: str | None = None,
) -> ImageBuildHandle:
    """Admit a CAS registry import and return without waiting.

    See :func:`start_build`; there is no Dockerfile and no context.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit
    if not isinstance(image_reference, str) or not image_reference.strip():
        raise SandboxImageBuildError("image reference to import must not be empty")
    reference = image_reference.strip()
    name = registered_name or _default_registered_name_from_image(reference)
    if name == _DEFAULT_IMAGE_NAME:
        warnings.warn(
            f"Importing sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
            "Pass `registered_name=...` to avoid collisions with other "
            "default-named images in this project.",
            stacklevel=2,
        )
    api_url, token, namespace = _credentials()
    emit({"type": "status", "message": f"Importing image '{reference}' as '{name}'..."})
    try:
        raw = _rust_start_image_import(
            api_url,
            token,
            reference,
            name,
            disk_mb,
            cpus,
            memory_mb,
            is_public,
            None,
            None,
            namespace,
            False,
            USER_AGENT,
            emit,
            docker_config_json,
        )
    except ImageBuildError:
        raise
    except Exception as exc:  # noqa: BLE001 - re-raised typed below
        _raise_typed(exc)
    info, client_request_id = _decode_started(raw)
    return ImageBuildHandle(
        info,
        client_request_id=client_request_id,
        _credentials=(api_url, token, namespace),
    )


def get_build(build_id: str) -> ImageBuildHandle:
    """Look a build up by its service-generated id.

    Raises:
        ImageBuildNotFoundError: Unknown or foreign build.
        ImageBuildError: ``build_id`` is a legacy ``sandbox_template_`` or an
            ``image_`` reference (refused before any request).
    """
    _reject_legacy_reference(build_id)
    api_url, token, namespace = _credentials()
    try:
        raw = _rust_get_image_build(
            api_url, token, build_id, None, None, namespace, False, USER_AGENT
        )
    except Exception as exc:  # noqa: BLE001 - re-raised typed below
        _raise_typed(exc)
    return ImageBuildHandle(
        ImageBuildInfo.from_dict(raw),
        _credentials=(api_url, token, namespace),
    )


def wait_build(
    build_id: str,
    *,
    timeout: float | None = None,
    follow_logs: bool = False,
    verbose: bool = False,
    emit: EmitFn | None = None,
) -> dict:
    """:meth:`ImageBuildHandle.wait` for a build id.

    The lookup of ``build_id`` is part of the interruptible wait (not a
    separate call before it), so ``KeyboardInterrupt`` during a slow or
    stalled first request still runs the cancellation handshake by build
    id, and ``timeout`` bounds the lookup too.
    """
    _reject_legacy_reference(build_id)
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit
    api_url, token, namespace = _credentials()
    try:
        raw = _rust_wait_image_build(
            api_url,
            token,
            build_id,
            timeout,
            follow_logs,
            None,
            None,
            namespace,
            False,
            USER_AGENT,
            emit,
        )
    except ImageBuildError:
        raise
    except Exception as exc:  # noqa: BLE001 - re-raised typed below
        _raise_typed(exc)
    return _snake_case_keys(_decode_response(raw))


def cancel_build(
    build_id: str | None = None,
    *,
    client_request_id: str | None = None,
    _creds: tuple[str, str, str] | None = None,
) -> CancelBuildOutcome:
    """Cancel a build.

    ``build_id`` is the sufficient, documented selector. ``client_request_id``
    exists only to recover an admission whose response was lost; its
    ``not_admitted`` outcome is an observation of absence, not a
    cancellation, and does not prevent a delayed original admission from
    creating the build (cancel it again by request id then). Exactly one
    selector is required.
    """
    if (build_id is None) == (client_request_id is None):
        raise ImageBuildError(
            "pass exactly one of build_id or client_request_id",
            code=ImageBuildErrorCode.USAGE,
        )
    if build_id is not None:
        _reject_legacy_reference(build_id)
    api_url, token, namespace = _creds or _credentials()
    try:
        raw = _rust_cancel_image_build(
            api_url,
            token,
            build_id,
            client_request_id,
            None,
            None,
            namespace,
            False,
            USER_AGENT,
        )
    except Exception as exc:  # noqa: BLE001 - re-raised typed below
        _raise_typed(exc)
    return CancelBuildOutcome.from_dict(raw)
