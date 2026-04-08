import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tensorlake.cli import create_sandbox_image as create_sandbox_image_module
from tensorlake.sandbox.models import SnapshotContentMode

BUILD_CPUS = 2.0
BUILD_MEMORY_MB = 4096


class TestCreateSandboxImage(unittest.TestCase):
    def test_logical_dockerfile_lines_merges_continuations(self):
        dockerfile = """
        # comment
        FROM python:3.12-slim
        RUN apt-get update \\
            && apt-get install -y curl

        ENV A=1 B=two
        """.strip()

        lines = create_sandbox_image_module._logical_dockerfile_lines(dockerfile)

        self.assertEqual(
            lines,
            [
                (2, "FROM python:3.12-slim"),
                (3, "RUN apt-get update && apt-get install -y curl"),
                (6, "ENV A=1 B=two"),
            ],
        )

    def test_load_dockerfile_plan_defaults_name_from_parent_directory(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            app_dir = Path(tmpdir) / "weather-app"
            app_dir.mkdir()
            dockerfile_path = app_dir / "Dockerfile"
            dockerfile_path.write_text(
                "FROM python:3.12-slim\nRUN echo hi\n",
                encoding="utf-8",
            )

            plan = create_sandbox_image_module._load_dockerfile_plan(
                str(dockerfile_path),
                None,
            )

        self.assertEqual(plan.base_image, "python:3.12-slim")
        self.assertEqual(plan.registered_name, "weather-app")
        self.assertEqual(
            plan.instructions,
            [
                create_sandbox_image_module.DockerfileInstruction(
                    keyword="RUN",
                    value="echo hi",
                    line_number=2,
                )
            ],
        )

    def test_load_dockerfile_plan_rejects_multistage(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text(
                "FROM python:3.12-slim AS build\nFROM debian:bookworm-slim\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                ValueError, "multi-stage Dockerfiles are not supported"
            ):
                create_sandbox_image_module._load_dockerfile_plan(
                    str(dockerfile_path),
                    None,
                )

    def test_create_sandbox_image_registers_snapshot_from_dockerfile(self):
        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "sandbox-image.Dockerfile"
            dockerfile_text = "\n".join(
                [
                    "FROM python:3.12-slim",
                    "WORKDIR /app",
                    "COPY . /app",
                    "RUN python -c \"print('hello')\"",
                ]
            )
            dockerfile_path.write_text(dockerfile_text + "\n", encoding="utf-8")

            with (
                patch.object(
                    create_sandbox_image_module,
                    "_build_context_from_env",
                    return_value=ctx,
                ),
                patch.object(
                    create_sandbox_image_module, "_execute_dockerfile_plan"
                ) as execute,
                patch.object(
                    create_sandbox_image_module,
                    "_register_image",
                    return_value={"id": "tpl-1"},
                ) as register_image,
                patch.object(create_sandbox_image_module, "_emit"),
                patch.object(
                    create_sandbox_image_module,
                    "SandboxClient",
                ) as sandbox_client_cls,
            ):
                sandbox_client = sandbox_client_cls.return_value
                sandbox_client.create_and_connect.return_value = sandbox
                sandbox_client.snapshot_and_wait.return_value = snapshot

                create_sandbox_image_module.create_sandbox_image(
                    str(dockerfile_path),
                    registered_name=None,
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                )

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.12-slim",
            cpus=BUILD_CPUS,
            memory_mb=BUILD_MEMORY_MB,
        )
        execute.assert_called_once()
        register_image.assert_called_once_with(
            ctx,
            "sandbox-image",
            dockerfile_text + "\n",
            "snap-1",
            "s3://snapshots/snap-1.tar.zst",
            False,
        )
        sandbox.terminate.assert_called_once_with()
        # Regression: sandbox image builds MUST request a filesystem-only
        # snapshot so the resulting image cold-boots on restore (see PR
        # #583 for the original regression that produced Full snapshots
        # and broke `tl sbx new --image`).
        sandbox_client.snapshot_and_wait.assert_called_once_with(
            "sbx-1",
            content_mode=SnapshotContentMode.FILESYSTEM_ONLY,
        )

    def test_create_sandbox_image_public(self):
        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text(
                "FROM python:3.12-slim\nRUN echo hi\n",
                encoding="utf-8",
            )
            dockerfile_text = dockerfile_path.read_text(encoding="utf-8")

            with (
                patch.object(
                    create_sandbox_image_module,
                    "_build_context_from_env",
                    return_value=ctx,
                ),
                patch.object(create_sandbox_image_module, "_execute_dockerfile_plan"),
                patch.object(
                    create_sandbox_image_module,
                    "_register_image",
                    return_value={"id": "tpl-1"},
                ) as register_image,
                patch.object(create_sandbox_image_module, "_emit"),
                patch.object(
                    create_sandbox_image_module,
                    "SandboxClient",
                ) as sandbox_client_cls,
            ):
                sandbox_client = sandbox_client_cls.return_value
                sandbox_client.create_and_connect.return_value = sandbox
                sandbox_client.snapshot_and_wait.return_value = snapshot

                create_sandbox_image_module.create_sandbox_image(
                    str(dockerfile_path),
                    registered_name="custom-name",
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                    is_public=True,
                )

        register_image.assert_called_once_with(
            ctx,
            "custom-name",
            dockerfile_text,
            "snap-1",
            "s3://snapshots/snap-1.tar.zst",
            True,
        )
        sandbox_client.snapshot_and_wait.assert_called_once_with(
            "sbx-1",
            content_mode=SnapshotContentMode.FILESYSTEM_ONLY,
        )


if __name__ == "__main__":
    unittest.main()
