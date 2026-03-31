import argparse
import json
import os
import sys
import traceback
from types import ModuleType

import httpx

from tensorlake.applications.remote.code.loader import load_code
from tensorlake.cli._common import Context
from tensorlake.image import Image
from tensorlake.image.image import _ImageBuildOperationType
from tensorlake.image.utils import dockerfile_content
from tensorlake.sandbox import Sandbox, SandboxClient
from tensorlake.sandbox.models import ProcessStatus


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


def _discover_module_images(module: ModuleType | None) -> dict[str, Image]:
    """Discover top-level Image objects from a loaded Python module."""
    if module is None:
        return {}

    images: dict[str, Image] = {}
    for value in vars(module).values():
        if isinstance(value, Image):
            if value.name in images:
                _emit(
                    {
                        "type": "error",
                        "message": f"Duplicate image name '{value.name}'. Each image must have a unique name.",
                    }
                )
                sys.exit(1)
            images[value.name] = value

    return images


def _select_image(images: dict[str, Image], image_name: str | None) -> Image:
    """Select an Image object from discovered images.

    If image_name is given, find by name. Otherwise auto-select if only one exists.
    """
    if not images:
        _emit({"type": "error", "message": "No images found in image file"})
        sys.exit(1)

    if image_name is None:
        if len(images) == 1:
            return next(iter(images.values()))
        names = list(images.keys())
        _emit(
            {
                "type": "error",
                "message": f"Multiple images found: {', '.join(names)}. Use --image-name to select one.",
            }
        )
        sys.exit(1)

    if image_name in images:
        return images[image_name]

    names = list(images.keys())
    _emit(
        {
            "type": "error",
            "message": f"Image '{image_name}' not found. Available: {', '.join(names)}",
        }
    )
    sys.exit(1)


def _run_streaming(
    sandbox: Sandbox,
    command: str,
    args: list[str] | None = None,
    env: dict[str, str] | None = None,
    working_dir: str | None = None,
):
    """Start a process and stream its stdout/stderr in real time via NDJSON.

    Polls get_stdout()/get_stderr() in a loop, emitting new lines as they
    appear so the user sees pip install progress, build output, etc.
    line-by-line as it happens.
    """
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
        # Emit any new stdout lines.
        stdout_resp = sandbox.get_stdout(proc.pid)
        for line in stdout_resp.lines[stdout_seen:]:
            _emit({"type": "build_log", "stream": "stdout", "message": line})
        stdout_seen = len(stdout_resp.lines)

        # Emit any new stderr lines.
        stderr_resp = sandbox.get_stderr(proc.pid)
        for line in stderr_resp.lines[stderr_seen:]:
            _emit({"type": "build_log", "stream": "stderr", "message": line})
        stderr_seen = len(stderr_resp.lines)

        # Check if process has exited.
        info = sandbox.get_process(proc.pid)
        if info.status != ProcessStatus.RUNNING:
            # Drain any remaining output after exit.
            stdout_resp = sandbox.get_stdout(proc.pid)
            for line in stdout_resp.lines[stdout_seen:]:
                _emit({"type": "build_log", "stream": "stdout", "message": line})
            stderr_resp = sandbox.get_stderr(proc.pid)
            for line in stderr_resp.lines[stderr_seen:]:
                _emit({"type": "build_log", "stream": "stderr", "message": line})
            break

        time.sleep(0.3)

    # The daemon may report a non-RUNNING status before the exit code is
    # available.  Re-poll briefly until exit_code or signal is populated.
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
        exit_code = 0  # Process exited, assume success if no code reported

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
                dest = os.path.join(remote_path, rel)
                with open(full, "rb") as f:
                    sandbox.write_file(dest, f.read())
    else:
        raise FileNotFoundError(f"Local path not found: {local_path}")


def _execute_operations(sandbox: Sandbox, image):
    """Translate Image build operations into sandbox commands with streaming output."""
    # Mirror the Dockerfile ENV defaults: WORKDIR /app, PIP_BREAK_SYSTEM_PACKAGES=1.
    # Accumulate user ENV operations so later RUN commands inherit them.
    process_env: dict[str, str] = {"PIP_BREAK_SYSTEM_PACKAGES": "1"}

    # Set working directory.
    _run_streaming(sandbox, "mkdir", ["-p", "/app"], env=process_env)

    for op in image._build_operations:
        if op.type == _ImageBuildOperationType.RUN:
            for cmd in op.args:
                _emit({"type": "status", "message": f"RUN {cmd}"})
                _run_streaming(
                    sandbox, "sh", ["-c", cmd], env=process_env, working_dir="/app"
                )
        elif op.type in (_ImageBuildOperationType.COPY, _ImageBuildOperationType.ADD):
            src, dest = op.args[0], op.args[1]
            _emit({"type": "status", "message": f"COPY {src} -> {dest}"})
            _copy_to_sandbox(sandbox, src, dest)
        elif op.type == _ImageBuildOperationType.ENV:
            key, value = op.args[0], op.args[1]
            _emit({"type": "status", "message": f"ENV {key}={value}"})
            process_env[key] = value
            # Also persist for processes started outside this script.
            _run_streaming(
                sandbox,
                "sh",
                ["-c", f"echo 'export {key}=\"{value}\"' >> /etc/environment"],
                env=process_env,
            )


def _register_image(
    ctx: Context,
    name: str,
    dockerfile: str,
    snapshot_id: str,
    snapshot_uri: str,
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
    # Include scope headers for PAT auth (match Rust CLI behavior).
    if ctx.personal_access_token and not ctx.api_key:
        headers["X-Forwarded-Organization-Id"] = org_id
        headers["X-Forwarded-Project-Id"] = proj_id

    body = {
        "name": name,
        "dockerfile": dockerfile,
        "snapshotId": snapshot_id,
        "snapshotUri": snapshot_uri,
    }
    resp = httpx.post(url, json=body, headers=headers, timeout=30.0)
    resp.raise_for_status()
    return resp.json()


def create_sandbox_image(
    image_file_path: str,
    image_name: str | None,
    cpus: float,
    memory_mb: int,
):
    ctx = _build_context_from_env()

    # 1. Load code & discover images.
    _emit({"type": "status", "message": f"Loading {image_file_path}..."})
    try:
        module = load_code(os.path.abspath(image_file_path))
    except SyntaxError as e:
        _emit(
            {
                "type": "error",
                "message": f"Syntax error in {e.filename}, line {e.lineno}: {e.msg}",
            }
        )
        sys.exit(1)
    except ImportError as e:
        _emit(
            {
                "type": "error",
                "message": "Failed to import image file. Make sure all dependencies are installed.",
                "details": f"{type(e).__name__}: {e}",
            }
        )
        sys.exit(1)
    except Exception as e:
        event = {
            "type": "error",
            "message": f"Failed to load {image_file_path}",
            "details": f"{type(e).__name__}: {e}",
        }
        if _debug_enabled():
            event["traceback"] = traceback.format_exc()
        _emit(event)
        sys.exit(1)

    images = _discover_module_images(module)
    image = _select_image(images, image_name)

    _emit({"type": "status", "message": f"Selected image: {image.name}"})

    # 2. Create sandbox.
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
            image=image._base_image,
            cpus=cpus,
            memory_mb=memory_mb,
        )
        _emit(
            {
                "type": "status",
                "message": f"Sandbox {sandbox.sandbox_id} is running",
            }
        )

        # 3. Execute image operations with streaming output.
        _execute_operations(sandbox, image)

        # 4. Snapshot.
        _emit({"type": "status", "message": "Creating snapshot..."})
        snapshot = sandbox_client.snapshot_and_wait(sandbox.sandbox_id)
        _emit(
            {
                "type": "snapshot_created",
                "snapshot_id": snapshot.snapshot_id,
            }
        )

        # 5. Generate dockerfile text.
        dockerfile = dockerfile_content(image)

        # 6. Register image via Platform API.
        _emit({"type": "status", "message": "Registering image..."})
        if not snapshot.snapshot_uri:
            raise RuntimeError(
                f"Snapshot {snapshot.snapshot_id} completed without a snapshot URI"
            )
        result = _register_image(
            ctx, image.name, dockerfile, snapshot.snapshot_id, snapshot.snapshot_uri
        )
        _emit(
            {
                "type": "image_registered",
                "image_id": result.get("id", ""),
                "name": image.name,
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
        description="Register a sandbox image from a Python file"
    )
    parser.add_argument(
        "image_file_path",
        help="Path to the image Python file",
    )
    parser.add_argument(
        "--image-name",
        "-i",
        default=None,
        help="Name of the image to use (required if multiple images exist in the file)",
    )
    parser.add_argument(
        "--cpus",
        type=float,
        default=2.0,
        help="CPUs for the build sandbox that installs dependencies and builds the image (default: 2.0)",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=4096,
        help="Memory in MB for the build sandbox that installs dependencies and builds the image (default: 4096)",
    )
    args = parser.parse_args()

    try:
        create_sandbox_image(
            args.image_file_path, args.image_name, args.cpus, args.memory
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
