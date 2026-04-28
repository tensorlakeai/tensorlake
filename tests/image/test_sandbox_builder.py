import tempfile
import unittest
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tensorlake.image import Image
from tensorlake.image import sandbox_builder as sbm
from tensorlake.sandbox.models import SnapshotType

BUILD_CPUS = 2.0
BUILD_MEMORY_MB = 4096


class TestDockerfileParsing(unittest.TestCase):
    def test_logical_dockerfile_lines_merges_continuations(self):
        dockerfile = """
        # comment
        FROM python:3.12-slim
        RUN apt-get update \\
            && apt-get install -y curl

        ENV A=1 B=two
        """.strip()

        lines = sbm._logical_dockerfile_lines(dockerfile)

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

            plan = sbm._load_dockerfile_plan(str(dockerfile_path), None)

        self.assertEqual(plan.base_image, "python:3.12-slim")
        self.assertEqual(plan.registered_name, "weather-app")
        self.assertEqual(
            plan.instructions,
            [
                sbm.DockerfileInstruction(
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
                sbm._load_dockerfile_plan(str(dockerfile_path), None)


def _make_build_patches(ctx, sandbox, snapshot):
    """Common mock bundle for exercising build_sandbox_image without networking."""
    return (
        patch.object(sbm, "_build_context_from_env", return_value=ctx),
        patch.object(sbm, "_execute_dockerfile_plan"),
        patch.object(sbm, "_register_image", return_value={"id": "tpl-1"}),
        patch.object(sbm, "SandboxClient"),
    )


class TestBuildSandboxImageFromDockerfile(unittest.TestCase):
    def test_registers_snapshot_from_dockerfile(self):
        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            sandbox_id="sbx-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
            size_bytes=1234,
            rootfs_disk_bytes=25 * 1024 * 1024 * 1024,
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

            build_ctx, execute, register_image, sandbox_client_cls = (
                _make_build_patches(ctx, sandbox, snapshot)
            )
            with (
                build_ctx,
                execute as execute_mock,
                register_image as register_mock,
                sandbox_client_cls as sandbox_client_cls_mock,
            ):
                sandbox_client = sandbox_client_cls_mock.return_value
                sandbox_client.create_and_connect.return_value = sandbox
                sandbox_client.snapshot_and_wait.return_value = snapshot

                sbm.build_sandbox_image(
                    str(dockerfile_path),
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                )

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.12-slim",
            cpus=BUILD_CPUS,
            memory_mb=BUILD_MEMORY_MB,
        )
        execute_mock.assert_called_once()
        register_mock.assert_called_once_with(
            ctx,
            "sandbox-image",
            dockerfile_text + "\n",
            "snap-1",
            "sbx-1",
            "s3://snapshots/snap-1.tar.zst",
            1234,
            25 * 1024 * 1024 * 1024,
            False,
        )
        sandbox.terminate.assert_called_once_with()
        # Regression: sandbox image builds MUST request a filesystem-only
        # snapshot so the resulting image cold-boots on restore (see PR
        # #583 for the original regression that produced Full snapshots
        # and broke `tl sbx new --image`).
        sandbox_client.snapshot_and_wait.assert_called_once_with(
            "sbx-1",
            snapshot_type=SnapshotType.FILESYSTEM,
        )

    def test_public_flag_and_registered_name(self):
        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            sandbox_id="sbx-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
            size_bytes=1234,
            rootfs_disk_bytes=25 * 1024 * 1024 * 1024,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text(
                "FROM python:3.12-slim\nRUN echo hi\n",
                encoding="utf-8",
            )
            dockerfile_text = dockerfile_path.read_text(encoding="utf-8")

            build_ctx, execute, register_image, sandbox_client_cls = (
                _make_build_patches(ctx, sandbox, snapshot)
            )
            with (
                build_ctx,
                execute,
                register_image as register_mock,
                sandbox_client_cls as sandbox_client_cls_mock,
            ):
                sandbox_client = sandbox_client_cls_mock.return_value
                sandbox_client.create_and_connect.return_value = sandbox
                sandbox_client.snapshot_and_wait.return_value = snapshot

                sbm.build_sandbox_image(
                    str(dockerfile_path),
                    registered_name="custom-name",
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                    is_public=True,
                )

        register_mock.assert_called_once_with(
            ctx,
            "custom-name",
            dockerfile_text,
            "snap-1",
            "sbx-1",
            "s3://snapshots/snap-1.tar.zst",
            1234,
            25 * 1024 * 1024 * 1024,
            True,
        )

    def test_load_errors_raise_SandboxImageLoadError(self):
        with self.assertRaises(sbm.SandboxImageLoadError):
            sbm.build_sandbox_image("/nonexistent/Dockerfile")


class TestBuildSandboxImageFromImage(unittest.TestCase):
    def _run_build(self, image: Image, **kwargs):
        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            sandbox_id="sbx-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
            size_bytes=1234,
            rootfs_disk_bytes=25 * 1024 * 1024 * 1024,
        )

        build_ctx, execute, register_image, sandbox_client_cls = _make_build_patches(
            ctx, sandbox, snapshot
        )
        with (
            build_ctx,
            execute,
            register_image as register_mock,
            sandbox_client_cls as sandbox_client_cls_mock,
        ):
            sandbox_client = sandbox_client_cls_mock.return_value
            sandbox_client.create_and_connect.return_value = sandbox
            sandbox_client.snapshot_and_wait.return_value = snapshot

            result = sbm.build_sandbox_image(image, **kwargs)

        return result, ctx, register_mock, sandbox_client, sandbox

    def test_renders_expected_dockerfile(self):
        image = (
            Image(name="weather-image", base_image="python:3.12-slim")
            .run("apt-get update")
            .workdir("/app")
            .env("APP_ENV", "prod")
            .copy("./src", "/app/src")
        )

        _, _, register_image, sandbox_client, _ = self._run_build(image)

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.12-slim",
            cpus=2.0,
            memory_mb=4096,
        )

        # The registered Dockerfile must match exactly what the TS SDK would
        # generate — no WORKDIR /app or pip install tensorlake injection.
        expected_dockerfile = "\n".join(
            [
                "FROM python:3.12-slim",
                "RUN apt-get update",
                "WORKDIR /app",
                'ENV APP_ENV="prod"',
                "COPY ./src /app/src",
            ]
        )
        register_image.assert_called_once()
        register_args = register_image.call_args.args
        self.assertEqual(register_args[1], "weather-image")
        self.assertEqual(register_args[2], expected_dockerfile)

        sandbox_client.snapshot_and_wait.assert_called_once_with(
            "sbx-1",
            snapshot_type=SnapshotType.FILESYSTEM,
        )

    def test_registered_name_overrides_image_name(self):
        image = Image(name="default-name", base_image="python:3.12-slim")
        _, _, register_image, _, _ = self._run_build(image, registered_name="override")
        self.assertEqual(register_image.call_args.args[1], "override")

    def test_warns_on_default_name(self):
        image = Image(base_image="python:3.12-slim")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self._run_build(image)
        default_name_warnings = [
            w for w in caught if "default" in str(w.message).lower()
        ]
        self.assertTrue(
            default_name_warnings,
            "Expected a warning about building with the default image name",
        )

    def test_rejects_unknown_source_type(self):
        with self.assertRaises(TypeError):
            sbm.build_sandbox_image(12345)  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
