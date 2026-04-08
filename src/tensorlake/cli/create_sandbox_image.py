import argparse
import json
import os
import posixpath
import shlex
import sys
import traceback
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

from tensorlake.cli._common import Context
from tensorlake.sandbox import Sandbox, SandboxClient
from tensorlake.sandbox.models import ProcessStatus, SnapshotContentMode

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


def _emit(obj):
    print(json.dumps(obj), flush=True)


def _debug_enabled() -> bool:
    return os.environ.get("TENSORLAKE_DEBUG", "").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _build_context_from_env() -> Context:
    """Build CLI context from environment variables set by the Rust CLI."""
    return Context.default(
        api_url=os.environ.get("TENSORLAKE_API_URL"),
        api_key=os.environ.get("TENSORLAKE_API_KEY"),
        personal_access_token=os.environ.get("TENSORLAKE_PAT"),
        namespace=os.environ.get("INDEXIFY_NAMESPACE"),
        organization_id=os.environ.get("TENSORLAKE_ORGANIZATION_ID"),
        project_id=os.environ.get("TENSORLAKE_PROJECT_ID"),
        debug=_debug_enabled(),
    )


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


def _run_streaming(
    sandbox: Sandbox,
    command: str,
    args: list[str] | None = None,
    env: dict[str, str] | None = None,
    working_dir: str | None = None,
):
    """Start a process and stream its stdout/stderr in real time via NDJSON."""
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
            _emit({"type": "build_log", "stream": "stdout", "message": line})
        stdout_seen = len(stdout_resp.lines)

        stderr_resp = sandbox.get_stderr(proc.pid)
        for line in stderr_resp.lines[stderr_seen:]:
            _emit({"type": "build_log", "stream": "stderr", "message": line})
        stderr_seen = len(stderr_resp.lines)

        info = sandbox.get_process(proc.pid)
        if info.status != ProcessStatus.RUNNING:
            stdout_resp = sandbox.get_stdout(proc.pid)
            for line in stdout_resp.lines[stdout_seen:]:
                _emit({"type": "build_log", "stream": "stdout", "message": line})
            stderr_resp = sandbox.get_stderr(proc.pid)
            for line in stderr_resp.lines[stderr_seen:]:
                _emit({"type": "build_log", "stream": "stderr", "message": line})
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
    sandbox: Sandbox, process_env: dict[str, str], key: str, value: str
):
    escaped_value = value.replace("\\", "\\\\").replace('"', '\\"')
    export_line = f'export {key}="{escaped_value}"'
    _run_streaming(
        sandbox,
        "sh",
        ["-c", f"printf '%s\\n' {shlex.quote(export_line)} >> /etc/environment"],
        env=process_env,
    )


def _copy_from_context(
    sandbox: Sandbox,
    context_dir: str,
    sources: list[str],
    destination: str,
    working_dir: str,
    keyword: str,
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

        _emit(
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
):
    destination_path = _resolve_container_path(destination, working_dir)
    parsed = urlparse(url)
    file_name = os.path.basename(parsed.path.rstrip("/")) or "downloaded"
    if destination_path.endswith("/"):
        destination_path = posixpath.join(destination_path.rstrip("/"), file_name)

    parent_dir = posixpath.dirname(destination_path) or "/"
    _emit(
        {
            "type": "status",
            "message": f"ADD {url} -> {destination_path}",
        }
    )
    _run_streaming(sandbox, "mkdir", ["-p", parent_dir], env=process_env)
    _run_streaming(
        sandbox,
        "sh",
        [
            "-c",
            f"curl -fsSL --location {shlex.quote(url)} -o {shlex.quote(destination_path)}",
        ],
        env=process_env,
        working_dir=working_dir,
    )


def _execute_dockerfile_plan(sandbox: Sandbox, plan: DockerfileBuildPlan):
    process_env: dict[str, str] = dict(_BUILD_SANDBOX_PIP_ENV)
    working_dir = "/"

    for instruction in plan.instructions:
        keyword = instruction.keyword
        value = instruction.value
        line_number = instruction.line_number

        if keyword == "RUN":
            _emit({"type": "status", "message": f"RUN {value}"})
            _run_streaming(
                sandbox,
                "sh",
                ["-c", value],
                env=process_env,
                working_dir=working_dir,
            )
            continue

        if keyword == "WORKDIR":
            tokens = shlex.split(value)
            if len(tokens) != 1:
                raise ValueError(
                    f"line {line_number}: WORKDIR must include exactly one path"
                )
            working_dir = _resolve_container_path(tokens[0], working_dir)
            _emit({"type": "status", "message": f"WORKDIR {working_dir}"})
            _run_streaming(sandbox, "mkdir", ["-p", working_dir], env=process_env)
            continue

        if keyword == "ENV":
            for key, env_value in _parse_env_pairs(value, line_number):
                _emit({"type": "status", "message": f"ENV {key}={env_value}"})
                process_env[key] = env_value
                _persist_env_var(sandbox, process_env, key, env_value)
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
                )
            else:
                _copy_from_context(
                    sandbox,
                    plan.context_dir,
                    sources,
                    destination,
                    working_dir,
                    keyword,
                )
            continue

        if keyword in _IGNORED_DOCKERFILE_INSTRUCTIONS:
            _emit(
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


def create_sandbox_image(
    dockerfile_path: str,
    registered_name: str | None,
    cpus: float,
    memory_mb: int,
    is_public: bool = False,
):
    ctx = _build_context_from_env()

    _emit({"type": "status", "message": f"Loading {dockerfile_path}..."})
    try:
        plan = _load_dockerfile_plan(dockerfile_path, registered_name)
    except Exception as e:
        event = {
            "type": "error",
            "message": f"Failed to load Dockerfile {dockerfile_path}",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)

    _emit(
        {
            "type": "status",
            "message": f"Selected image name: {plan.registered_name}",
        }
    )

    _emit({"type": "status", "message": "Creating sandbox..."})
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
        _emit(
            {
                "type": "status",
                "message": f"Sandbox {sandbox.sandbox_id} is running",
            }
        )

        _execute_dockerfile_plan(sandbox, plan)

        _emit({"type": "status", "message": "Creating snapshot..."})
        snapshot = sandbox_client.snapshot_and_wait(
            sandbox.sandbox_id,
            content_mode=SnapshotContentMode.FILESYSTEM_ONLY,
        )
        _emit(
            {
                "type": "snapshot_created",
                "snapshot_id": snapshot.snapshot_id,
            }
        )

        _emit({"type": "status", "message": "Registering image..."})
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
        _emit(
            {
                "type": "image_registered",
                "image_id": result.get("id", ""),
                "name": plan.registered_name,
                "snapshot_id": snapshot.snapshot_id,
                "snapshot_uri": snapshot.snapshot_uri,
            }
        )

        _emit({"type": "done"})

    except Exception as e:
        event = {
            "type": "error",
            "message": f"Image registration failed: {type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)
    finally:
        if sandbox is not None:
            try:
                sandbox.terminate()
            except Exception:
                pass


def create_sandbox_image_entrypoint():
    parser = argparse.ArgumentParser(
        description="Register a sandbox image from a Dockerfile"
    )
    parser.add_argument(
        "dockerfile_path",
        help="Path to the Dockerfile",
    )
    parser.add_argument(
        "--name",
        "-n",
        default=None,
        help=(
            "Registered sandbox image name. Defaults to the Dockerfile stem, "
            "or the parent directory name when the file is named Dockerfile."
        ),
    )
    parser.add_argument(
        "--cpus",
        type=float,
        default=2.0,
        help="CPUs for the build sandbox that materializes the Dockerfile (default: 2.0)",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=4096,
        help="Memory in MB for the build sandbox that materializes the Dockerfile (default: 4096)",
    )
    parser.add_argument(
        "--public",
        action="store_true",
        default=False,
        help="Make this sandbox image publicly accessible.",
    )
    args = parser.parse_args()

    try:
        create_sandbox_image(
            args.dockerfile_path,
            args.name,
            args.cpus,
            args.memory,
            args.public,
        )
    except SystemExit:
        raise
    except Exception as e:
        event = {
            "type": "error",
            "message": f"create-sandbox-image failed ({type(e).__name__})",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)


if __name__ == "__main__":
    create_sandbox_image_entrypoint()
