import os
import shutil
import subprocess
import uuid
from pathlib import Path

import pytest

BASE_IMAGES = (
    "tensorlake/ubuntu-minimal",
    "python:3.12-slim",
)


def _require_e2e() -> None:
    if os.environ.get("TENSORLAKE_E2E") != "1":
        pytest.skip("set TENSORLAKE_E2E=1 to run cloud e2e tests")
    if not os.environ.get("TENSORLAKE_API_KEY"):
        pytest.skip("TENSORLAKE_API_KEY is required for cloud e2e tests")
    if _tl_bin() is None:
        pytest.skip("tl binary is required for cloud e2e tests")


def _tl_bin() -> str | None:
    return os.environ.get("TL_BIN") or shutil.which("tl")


def _run_tl(args: list[str], *, cwd: Path, timeout: int) -> subprocess.CompletedProcess:
    tl_bin = _tl_bin()
    assert tl_bin is not None
    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    result = subprocess.run(
        [tl_bin, *args],
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise AssertionError(
            "tl command failed\n"
            f"command: tl {' '.join(args)}\n"
            f"exit code: {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def _write_application(tmp_path: Path, image_names: dict[str, str]) -> Path:
    image_defs = "\n".join(
        f'{name} = Image(name="{image_names[base_image]}", base_image="{base_image}")'
        for name, base_image in (
            ("UBUNTU_IMAGE", "tensorlake/ubuntu-minimal"),
            ("PYTHON_IMAGE", "python:3.12-slim"),
        )
    )
    app_path = tmp_path / "function_executor_path_app.py"
    app_path.write_text(
        f"""from tensorlake.applications import Image, application, function

{image_defs}


@application()
@function(image=UBUNTU_IMAGE)
def function_executor_path_app(value: int) -> int:
    return python_worker(value)


@function(image=PYTHON_IMAGE)
def python_worker(value: int) -> int:
    return value
""",
        encoding="utf-8",
    )
    return app_path


def test_deployed_function_images_boot_function_executor_by_name(
    tmp_path: Path,
) -> None:
    _require_e2e()
    run_id = uuid.uuid4().hex[:12]
    image_names = {
        image: f"e2e-function-executor-{index}-{run_id}"
        for index, image in enumerate(BASE_IMAGES)
    }
    app_path = _write_application(tmp_path, image_names)
    sandbox_ids: list[str] = []

    try:
        _run_tl(["deploy", str(app_path)], cwd=tmp_path, timeout=900)

        for base_image in BASE_IMAGES:
            create = _run_tl(
                [
                    "sbx",
                    "create",
                    "--image",
                    image_names[base_image],
                    "--cpus",
                    "1",
                    "--memory",
                    "1024",
                ],
                cwd=tmp_path,
                timeout=240,
            )
            sandbox_id = create.stdout.strip().splitlines()[-1]
            assert sandbox_id, create.stdout
            sandbox_ids.append(sandbox_id)

            exec_result = _run_tl(
                ["sbx", "exec", sandbox_id, "function-executor", "--help"],
                cwd=tmp_path,
                timeout=120,
            )

            output = exec_result.stdout + exec_result.stderr
            assert "Runs Function Executor" in output
            assert "--executor-id" in output
            assert "--address" in output
    finally:
        tl_bin = _tl_bin()
        for sandbox_id in sandbox_ids:
            subprocess.run(
                [tl_bin, "sbx", "terminate", sandbox_id],
                cwd=tmp_path,
                env=os.environ.copy(),
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=60,
                check=False,
            )
