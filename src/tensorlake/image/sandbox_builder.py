"""Sandbox-image build engine.

Takes an :class:`Image` or a Dockerfile path, materializes it inside a build
sandbox, snapshots the filesystem, and registers the snapshot as a named
sandbox template (``POST /platform/v1/.../sandbox-templates``).

This is the programmatic backend for :meth:`Image.build` and the
``tl sbx image create`` CLI command.
"""

from __future__ import annotations

import json
import os
import posixpath
import shlex
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from urllib.parse import urlparse

import httpx

from tensorlake.cli._common import Context
from tensorlake.sandbox import Sandbox, SandboxClient
from tensorlake.sandbox.models import ProcessStatus, SnapshotContentMode

from ._dockerfile import image_to_dockerfile, render_op_line
from .image import Image

EmitFn = Callable[[dict], None]

_BUILD_SANDBOX_PIP_ENV = {"PIP_BREAK_SYSTEM_PACKAGES": "1"}
_IGNORED_DOCKERFILE_INSTRUCTIONS = {
    "CMD",
    "ENTRYPOINT",
    "EXPOSE",
    "HEALTHCHECK",
    "LABEL",
    "STOPSIGNAL",
    "VOLUME",
}
_UNSUPPORTED_DOCKERFILE_INSTRUCTIONS = {
    "ARG",
    "ONBUILD",
    "SHELL",
    "USER",
}

_DEFAULT_IMAGE_NAME = "default"


# --- Public exceptions ------------------------------------------------------


class SandboxImageError(Exception):
    """Base class for sandbox-image build errors."""


class SandboxImageLoadError(SandboxImageError):
    """The source Dockerfile or Image could not be loaded or parsed."""


class SandboxImageBuildError(SandboxImageError):
    """The build failed while provisioning, materializing, or registering."""


# --- Plan dataclasses -------------------------------------------------------


@dataclass(frozen=True)
class DockerfileInstruction:
    keyword: str
    value: str
    line_number: int


@dataclass(frozen=True)
class DockerfileBuildPlan:
    dockerfile_path: str
    context_dir: str
    registered_name: str
    dockerfile_text: str
    base_image: str
    instructions: list[DockerfileInstruction]


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


# --- Dockerfile parsing ----------------------------------------------------


def _default_registered_name(dockerfile_path: str) -> str:
    path = Path(dockerfile_path)
    stem = path.stem
    if stem.lower() == "dockerfile":
        parent_name = path.parent.name.strip()
        return parent_name or "sandbox-image"
    return stem or "sandbox-image"


def _logical_dockerfile_lines(dockerfile_text: str) -> list[tuple[int, str]]:
    logical_lines: list[tuple[int, str]] = []
    parts: list[str] = []
    start_line: int | None = None

    for line_number, raw_line in enumerate(dockerfile_text.splitlines(), start=1):
        stripped = raw_line.strip()
        if not parts and (not stripped or stripped.startswith("#")):
            continue

        if start_line is None:
            start_line = line_number

        line = raw_line.rstrip()
        continued = line.endswith("\\")
        if continued:
            line = line[:-1]

        normalized = line.strip()
        if normalized and not normalized.startswith("#"):
            parts.append(normalized)

        if continued:
            continue

        if parts:
            logical_lines.append((start_line, " ".join(parts)))
        parts = []
        start_line = None

    if parts:
        logical_lines.append((start_line or 1, " ".join(parts)))

    return logical_lines


def _split_instruction(line: str, line_number: int) -> tuple[str, str]:
    parts = line.split(None, 1)
    if not parts:
        raise ValueError(f"line {line_number}: empty Dockerfile instruction")
    keyword = parts[0].upper()
    value = parts[1].strip() if len(parts) > 1 else ""
    return keyword, value


def _strip_leading_flags(value: str) -> tuple[dict[str, str], str]:
    flags: dict[str, str] = {}
    remaining = value.lstrip()

    while remaining.startswith("--"):
        token, sep, rest = remaining.partition(" ")
        if not sep:
            raise ValueError(f"invalid Dockerfile flag syntax: {value}")

        flag_body = token[2:]
        if "=" in flag_body:
            key, flag_value = flag_body.split("=", 1)
            remaining = rest.lstrip()
        else:
            rest = rest.lstrip()
            flag_value, sep, remaining = rest.partition(" ")
            if not sep:
                raise ValueError(f"missing value for Dockerfile flag '{token}'")
            key = flag_body
            remaining = remaining.lstrip()

        flags[key] = flag_value

    return flags, remaining


def _parse_from_value(value: str, line_number: int) -> str:
    flags, remainder = _strip_leading_flags(value)
    if flags.get("platform"):
        # --platform only affects architecture selection; the base image name stays the same.
        pass

    tokens = shlex.split(remainder)
    if not tokens:
        raise ValueError(f"line {line_number}: FROM must include a base image")

    image = tokens[0]
    if len(tokens) > 1 and tokens[1].lower() != "as":
        raise ValueError(f"line {line_number}: unsupported FROM syntax '{value}'")
    return image


def _parse_copy_like_values(
    value: str,
    line_number: int,
    keyword: str,
) -> tuple[dict[str, str], list[str], str]:
    flags, payload = _strip_leading_flags(value)
    if "from" in flags:
        raise ValueError(
            f"line {line_number}: {keyword} --from is not supported for sandbox image creation"
        )

    payload = payload.strip()
    if not payload:
        raise ValueError(
            f"line {line_number}: {keyword} must include source and destination"
        )

    if payload.startswith("["):
        try:
            items = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"line {line_number}: invalid JSON array syntax for {keyword}: {exc}"
            ) from exc
        if not isinstance(items, list) or len(items) < 2:
            raise ValueError(
                f"line {line_number}: {keyword} JSON array form requires at least two string values"
            )
        if not all(isinstance(item, str) for item in items):
            raise ValueError(
                f"line {line_number}: {keyword} JSON array form only supports string values"
            )
        parts = items
    else:
        parts = shlex.split(payload)
        if len(parts) < 2:
            raise ValueError(
                f"line {line_number}: {keyword} must include at least one source and one destination"
            )

    return flags, parts[:-1], parts[-1]


def _parse_env_pairs(value: str, line_number: int) -> list[tuple[str, str]]:
    tokens = shlex.split(value)
    if not tokens:
        raise ValueError(f"line {line_number}: ENV must include a key and value")

    if all("=" in token for token in tokens):
        pairs: list[tuple[str, str]] = []
        for token in tokens:
            key, env_value = token.split("=", 1)
            if not key:
                raise ValueError(f"line {line_number}: invalid ENV token '{token}'")
            pairs.append((key, env_value))
        return pairs

    if len(tokens) < 2:
        raise ValueError(f"line {line_number}: ENV must include a key and value")

    return [(tokens[0], " ".join(tokens[1:]))]


def _resolve_container_path(path: str, working_dir: str) -> str:
    if not path:
        return working_dir
    if path.startswith("/"):
        normalized = posixpath.normpath(path)
    else:
        normalized = posixpath.normpath(posixpath.join(working_dir, path))
    return normalized if normalized.startswith("/") else f"/{normalized}"


def _load_dockerfile_plan(
    dockerfile_path: str,
    registered_name: str | None,
) -> DockerfileBuildPlan:
    path = Path(dockerfile_path).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Dockerfile not found: {dockerfile_path}")

    dockerfile_text = path.read_text(encoding="utf-8")
    base_image: str | None = None
    instructions: list[DockerfileInstruction] = []

    for line_number, line in _logical_dockerfile_lines(dockerfile_text):
        keyword, value = _split_instruction(line, line_number)
        if keyword == "FROM":
            if base_image is not None:
                raise ValueError(
                    f"line {line_number}: multi-stage Dockerfiles are not supported for sandbox image creation"
                )
            base_image = _parse_from_value(value, line_number)
            continue

        if keyword in _UNSUPPORTED_DOCKERFILE_INSTRUCTIONS:
            raise ValueError(
                f"line {line_number}: Dockerfile instruction '{keyword}' is not supported for sandbox image creation"
            )

        instructions.append(
            DockerfileInstruction(
                keyword=keyword,
                value=value,
                line_number=line_number,
            )
        )

    if base_image is None:
        raise ValueError("Dockerfile must contain a FROM instruction")

    return DockerfileBuildPlan(
        dockerfile_path=str(path),
        context_dir=str(path.parent),
        registered_name=(registered_name or _default_registered_name(str(path))),
        dockerfile_text=dockerfile_text,
        base_image=base_image,
        instructions=instructions,
    )


# --- Image → plan ---------------------------------------------------------


def _build_op_to_instruction(op, line_number: int) -> DockerfileInstruction:
    """Convert an in-memory build op into a DockerfileInstruction the executor understands.

    The keyword mirrors ``op.type.name``; the value is the rendered Dockerfile
    line with the leading keyword stripped so the executor's parsers see the
    same text they'd see from a real Dockerfile.
    """
    rendered = render_op_line(op)
    keyword, _, value = rendered.partition(" ")
    return DockerfileInstruction(
        keyword=keyword, value=value.strip(), line_number=line_number
    )


def _load_image_plan(
    image: Image,
    registered_name: str | None,
    context_dir: str | None,
) -> DockerfileBuildPlan:
    """Build a DockerfileBuildPlan from an in-memory Image.

    ``context_dir`` is used to resolve relative COPY/ADD sources; defaults to
    the current working directory.
    """
    if not image._base_image:
        raise ValueError("Image must have a base_image to build")

    resolved_context = str(Path(context_dir or os.getcwd()).resolve())
    instructions = [
        _build_op_to_instruction(op, line_number=idx + 1)
        for idx, op in enumerate(image._build_operations)
    ]

    return DockerfileBuildPlan(
        dockerfile_path=str(Path(resolved_context) / "Dockerfile"),
        context_dir=resolved_context,
        registered_name=registered_name or image.name,
        dockerfile_text=image_to_dockerfile(image),
        base_image=image._base_image,
        instructions=instructions,
    )


# --- Plan execution -------------------------------------------------------


def _run_streaming(
    sandbox: Sandbox,
    command: str,
    args: list[str] | None = None,
    env: dict[str, str] | None = None,
    working_dir: str | None = None,
    emit: EmitFn = _noop_emit,
):
    """Start a process and stream its stdout/stderr in real time via ``emit``."""
    import time

    proc = sandbox.start_process(
        command=command,
        args=args or [],
        env=env,
        working_dir=working_dir,
    )

    stdout_seen = 0
    stderr_seen = 0

    while True:
        stdout_resp = sandbox.get_stdout(proc.pid)
        for line in stdout_resp.lines[stdout_seen:]:
            emit({"type": "build_log", "stream": "stdout", "message": line})
        stdout_seen = len(stdout_resp.lines)

        stderr_resp = sandbox.get_stderr(proc.pid)
        for line in stderr_resp.lines[stderr_seen:]:
            emit({"type": "build_log", "stream": "stderr", "message": line})
        stderr_seen = len(stderr_resp.lines)

        info = sandbox.get_process(proc.pid)
        if info.status != ProcessStatus.RUNNING:
            stdout_resp = sandbox.get_stdout(proc.pid)
            for line in stdout_resp.lines[stdout_seen:]:
                emit({"type": "build_log", "stream": "stdout", "message": line})
            stderr_resp = sandbox.get_stderr(proc.pid)
            for line in stderr_resp.lines[stderr_seen:]:
                emit({"type": "build_log", "stream": "stderr", "message": line})
            break

        time.sleep(0.3)

    for _ in range(10):
        if info.exit_code is not None or info.signal is not None:
            break
        time.sleep(0.2)
        info = sandbox.get_process(proc.pid)

    if info.exit_code is not None:
        exit_code = info.exit_code
    elif info.signal is not None:
        exit_code = -info.signal
    else:
        exit_code = 0

    if exit_code != 0:
        raise RuntimeError(
            f"Command '{command} {' '.join(args or [])}' failed with exit code {exit_code}"
        )
    return exit_code


def _copy_to_sandbox(sandbox: Sandbox, local_path: str, remote_path: str):
    """Copy a local file or directory into the sandbox."""
    if os.path.isfile(local_path):
        with open(local_path, "rb") as f:
            sandbox.write_file(remote_path, f.read())
    elif os.path.isdir(local_path):
        for root, _dirs, files in os.walk(local_path):
            for filename in files:
                full = os.path.join(root, filename)
                rel = os.path.relpath(full, local_path)
                dest = posixpath.join(remote_path, rel)
                with open(full, "rb") as f:
                    sandbox.write_file(dest, f.read())
    else:
        raise FileNotFoundError(f"Local path not found: {local_path}")


def _persist_env_var(
    sandbox: Sandbox,
    process_env: dict[str, str],
    key: str,
    value: str,
    emit: EmitFn = _noop_emit,
):
    escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
    export_line = f'export {key}="{escaped_value}"'
    _run_streaming(
        sandbox,
        "sh",
        ["-c", f"printf '%s\\n' {shlex.quote(export_line)} >> /etc/environment"],
        env=process_env,
        emit=emit,
    )


def _copy_from_context(
    sandbox: Sandbox,
    context_dir: str,
    sources: list[str],
    destination: str,
    working_dir: str,
    keyword: str,
    emit: EmitFn = _noop_emit,
):
    destination_path = _resolve_container_path(destination, working_dir)
    if len(sources) > 1 and not destination_path.endswith("/"):
        raise ValueError(
            f"{keyword} with multiple sources requires a directory destination ending in '/'"
        )

    for source in sources:
        local_source = os.path.join(context_dir, source)
        if len(sources) > 1:
            remote_destination = posixpath.join(
                destination_path.rstrip("/"),
                os.path.basename(source.rstrip("/")),
            )
        else:
            remote_destination = destination_path
            if os.path.isfile(local_source) and destination_path.endswith("/"):
                remote_destination = posixpath.join(
                    destination_path.rstrip("/"),
                    os.path.basename(source),
                )

        emit(
            {
                "type": "status",
                "message": f"{keyword} {source} -> {remote_destination}",
            }
        )
        _copy_to_sandbox(sandbox, local_source, remote_destination)


def _add_url_to_sandbox(
    sandbox: Sandbox,
    url: str,
    destination: str,
    working_dir: str,
    process_env: dict[str, str],
    emit: EmitFn = _noop_emit,
):
    destination_path = _resolve_container_path(destination, working_dir)
    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path.rstrip("/")) or "downloaded"
    if destination_path.endswith("/"):
        destination_path = posixpath.join(destination_path.rstrip("/"), file_name)

    parent_dir = posixpath.dirname(destination_path) or "/"
    emit(
        {
            "type": "status",
            "message": f"ADD {url} -> {destination_path}",
        }
    )
    _run_streaming(sandbox, "mkdir", ["-p", parent_dir], env=process_env, emit=emit)
    _run_streaming(
        sandbox,
        "sh",
        [
            "-c",
            f"curl -fsSL --location {shlex.quote(url)} -o {shlex.quote(destination_path)}",
        ],
        env=process_env,
        working_dir=working_dir,
        emit=emit,
    )


def _execute_dockerfile_plan(
    sandbox: Sandbox,
    plan: DockerfileBuildPlan,
    emit: EmitFn = _noop_emit,
):
    process_env: dict[str, str] = dict(_BUILD_SANDBOX_PIP_ENV)
    working_dir = "/"

    for instruction in plan.instructions:
        keyword = instruction.keyword
        value = instruction.value
        line_number = instruction.line_number

        if keyword == "RUN":
            emit({"type": "status", "message": f"RUN {value}"})
            _run_streaming(
                sandbox,
                "sh",
                ["-c", value],
                env=process_env,
                working_dir=working_dir,
                emit=emit,
            )
            continue

        if keyword == "WORKDIR":
            tokens = shlex.split(value)
            if len(tokens) != 1:
                raise ValueError(
                    f"line {line_number}: WORKDIR must include exactly one path"
                )
            working_dir = _resolve_container_path(tokens[0], working_dir)
            emit({"type": "status", "message": f"WORKDIR {working_dir}"})
            _run_streaming(
                sandbox, "mkdir", ["-p", working_dir], env=process_env, emit=emit
            )
            continue

        if keyword == "ENV":
            for key, env_value in _parse_env_pairs(value, line_number):
                emit({"type": "status", "message": f"ENV {key}={env_value}"})
                process_env[key] = env_value
                _persist_env_var(sandbox, process_env, key, env_value, emit=emit)
            continue

        if keyword == "COPY":
            _flags, sources, destination = _parse_copy_like_values(
                value,
                line_number,
                keyword,
            )
            _copy_from_context(
                sandbox,
                plan.context_dir,
                sources,
                destination,
                working_dir,
                keyword,
                emit=emit,
            )
            continue

        if keyword == "ADD":
            _flags, sources, destination = _parse_copy_like_values(
                value,
                line_number,
                keyword,
            )
            if len(sources) == 1 and urlparse(sources[0]).scheme in {"http", "https"}:
                _add_url_to_sandbox(
                    sandbox,
                    sources[0],
                    destination,
                    working_dir,
                    process_env,
                    emit=emit,
                )
            else:
                _copy_from_context(
                    sandbox,
                    plan.context_dir,
                    sources,
                    destination,
                    working_dir,
                    keyword,
                    emit=emit,
                )
            continue

        if keyword in _IGNORED_DOCKERFILE_INSTRUCTIONS:
            emit(
                {
                    "type": "warning",
                    "message": (
                        f"Skipping Dockerfile instruction '{keyword}' during snapshot materialization. "
                        "It is still preserved in the registered Dockerfile."
                    ),
                }
            )
            continue

        raise ValueError(
            f"line {line_number}: Dockerfile instruction '{keyword}' is not supported for sandbox image creation"
        )


# --- Registration ---------------------------------------------------------


def _register_image(
    ctx: Context,
    name: str,
    dockerfile: str,
    snapshot_id: str,
    snapshot_uri: str,
    is_public: bool = False,
) -> dict:
    """POST to Platform API through the ingress to register the image."""
    org_id = ctx.organization_id
    proj_id = ctx.project_id
    if not org_id or not proj_id:
        raise RuntimeError(
            "Organization ID and Project ID are required. Run 'tl login' and 'tl init'."
        )

    url = f"{ctx.api_url}/platform/v1/organizations/{org_id}/projects/{proj_id}/sandbox-templates"
    bearer_token = ctx.api_key or ctx.personal_access_token
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }
    if ctx.personal_access_token and not ctx.api_key:
        headers["X-Forwarded-Organization-Id"] = org_id
        headers["X-Forwarded-Project-Id"] = proj_id

    body = {
        "name": name,
        "dockerfile": dockerfile,
        "snapshotId": snapshot_id,
        "snapshotUri": snapshot_uri,
        "isPublic": is_public,
    }
    resp = httpx.post(url, json=body, headers=headers, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def _run_plan(
    plan: DockerfileBuildPlan,
    ctx: Context,
    cpus: float,
    memory_mb: int,
    is_public: bool,
    emit: EmitFn,
) -> dict:
    """Materialize a plan in a sandbox, snapshot, and register it."""
    emit(
        {
            "type": "status",
            "message": f"Selected image name: {plan.registered_name}",
        }
    )
    emit({"type": "status", "message": "Creating sandbox..."})
    sandbox_client = SandboxClient(
        api_url=ctx.api_url,
        api_key=ctx.api_key or ctx.personal_access_token,
        organization_id=ctx.organization_id,
        project_id=ctx.project_id,
    )

    sandbox = None
    try:
        sandbox = sandbox_client.create_and_connect(
            image=plan.base_image,
            cpus=cpus,
            memory_mb=memory_mb,
        )
        emit(
            {
                "type": "status",
                "message": f"Sandbox {sandbox.sandbox_id} is running",
            }
        )

        _execute_dockerfile_plan(sandbox, plan, emit=emit)

        emit({"type": "status", "message": "Creating snapshot..."})
        snapshot = sandbox_client.snapshot_and_wait(
            sandbox.sandbox_id,
            content_mode=SnapshotContentMode.FILESYSTEM_ONLY,
        )
        emit(
            {
                "type": "snapshot_created",
                "snapshot_id": snapshot.snapshot_id,
            }
        )

        emit({"type": "status", "message": "Registering image..."})
        if not snapshot.snapshot_uri:
            raise RuntimeError(
                f"Snapshot {snapshot.snapshot_id} completed without a snapshot URI"
            )
        result = _register_image(
            ctx,
            plan.registered_name,
            plan.dockerfile_text,
            snapshot.snapshot_id,
            snapshot.snapshot_uri,
            is_public,
        )
        emit(
            {
                "type": "image_registered",
                "image_id": result.get("id", ""),
                "name": plan.registered_name,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_uri": snapshot.snapshot_uri,
            }
        )
        return result
    finally:
        if sandbox is not None:
            try:
                sandbox.terminate()
            except Exception:
                pass


# --- Public API -----------------------------------------------------------


def build_sandbox_image(
    source: Image | str,
    *,
    registered_name: str | None = None,
    cpus: float = 2.0,
    memory_mb: int = 4096,
    is_public: bool = False,
    context_dir: str | None = None,
    verbose: bool = False,
    emit: EmitFn | None = None,
) -> dict:
    """Build a sandbox image from an :class:`Image` or a Dockerfile path.

    Materializes the image inside a build sandbox, snapshots the filesystem,
    and registers the snapshot as a named sandbox template.

    Args:
        source: An :class:`Image` instance or a path to a Dockerfile.
        registered_name: Name to register the image under. Defaults to the
            image's ``name`` or the Dockerfile stem.
        cpus: CPUs for the build sandbox.
        memory_mb: Memory for the build sandbox in MB.
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
            loaded or parsed.
        SandboxImageBuildError: The build failed during provisioning,
            materialization, or registration.
        TypeError: ``source`` is neither an ``Image`` nor a path.
    """
    if emit is None:
        emit = _stderr_emit if verbose else _noop_emit

    try:
        if isinstance(source, Image):
            plan = _load_image_plan(source, registered_name, context_dir)
        elif isinstance(source, (str, os.PathLike)):
            plan = _load_dockerfile_plan(os.fspath(source), registered_name)
        else:
            # Programmer error — propagate directly rather than wrapping.
            raise TypeError(
                "source must be an Image or a Dockerfile path, got "
                f"{type(source).__name__}"
            )
    except TypeError:
        raise
    except (FileNotFoundError, ValueError, OSError) as e:
        raise SandboxImageLoadError(str(e)) from e

    if plan.registered_name == _DEFAULT_IMAGE_NAME:
        warnings.warn(
            f"Building sandbox image with the default name {_DEFAULT_IMAGE_NAME!r}. "
            "Pass `registered_name=...` or `Image(name=...)` to avoid collisions "
            "with other default-named images in this project.",
            stacklevel=2,
        )

    ctx = _build_context_from_env()
    try:
        return _run_plan(plan, ctx, cpus, memory_mb, is_public, emit=emit)
    except SandboxImageError:
        raise
    except Exception as e:
        raise SandboxImageBuildError(f"{type(e).__name__}: {e}") from e
