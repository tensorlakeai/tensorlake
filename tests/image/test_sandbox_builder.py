import tempfile
import unittest
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, patch

from tensorlake.image import Image
from tensorlake.image import sandbox_builder as sbm
from tensorlake.image.utils import dockerfile_content

BUILD_CPUS = 2.0
BUILD_MEMORY_MB = 4096


def _make_ctx(**overrides):
    defaults = dict(
        api_url="https://api.tensorlake.test",
        api_key="tl_apiKey_abc",
        personal_access_token=None,
        organization_id="org_1",
        project_id="proj_1",
        namespace="default",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_build_patches(ctx):
    """Common mock bundle for exercising build_sandbox_image without networking."""
    return (
        patch.object(sbm, "_build_context_from_env", return_value=ctx),
        patch.object(
            sbm,
            "_rust_build_sandbox_image",
            return_value='{"id":"tpl-1","snapshot_id":"snap-1"}',
        ),
    )


class TestBuildSandboxImageFromDockerfile(unittest.TestCase):
    def test_registers_snapshot_from_dockerfile(self):
        ctx = _make_ctx()

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

            build_ctx, rust_builder = _make_build_patches(ctx)
            with build_ctx, rust_builder as rust_builder_mock:
                sbm.build_sandbox_image(
                    str(dockerfile_path),
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                )

        rust_builder_mock.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            str(dockerfile_path.resolve()),
            "sandbox-image",
            None,
            None,
            BUILD_CPUS,
            BUILD_MEMORY_MB,
            False,
            "org_1",
            "proj_1",
            "default",
            False,
            sbm.USER_AGENT,
            None,
            None,
            ANY,
        )


class TestBuildSandboxImageFromDockerfileOptions(unittest.TestCase):
    def test_public_flag_and_registered_name(self):
        ctx = _make_ctx()

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text(
                "FROM python:3.12-slim\nRUN echo hi\n",
                encoding="utf-8",
            )

            build_ctx, rust_builder = _make_build_patches(ctx)
            with build_ctx, rust_builder as rust_builder_mock:
                sbm.build_sandbox_image(
                    str(dockerfile_path),
                    registered_name="custom-name",
                    cpus=BUILD_CPUS,
                    memory_mb=BUILD_MEMORY_MB,
                    disk_mb=25 * 1024,
                    builder_disk_mb=32 * 1024,
                    is_public=True,
                )

        rust_builder_mock.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            str(dockerfile_path.resolve()),
            "custom-name",
            25 * 1024,
            32 * 1024,
            BUILD_CPUS,
            BUILD_MEMORY_MB,
            True,
            "org_1",
            "proj_1",
            "default",
            False,
            sbm.USER_AGENT,
            None,
            None,
            ANY,
        )

    def test_missing_dockerfile_raises_load_error(self):
        with self.assertRaises(sbm.SandboxImageLoadError):
            sbm.build_sandbox_image("/nonexistent/Dockerfile")

    def test_rust_builder_events_are_forwarded_to_emit(self):
        ctx = _make_ctx()
        emitted: list[dict] = []

        def fake_rust_builder(*args):
            args[-1]({"type": "status", "message": "builder running"})
            return '{"id":"tpl-1","snapshot_id":"snap-1"}'

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(
                sbm, "_rust_build_sandbox_image", side_effect=fake_rust_builder
            ),
        ):
            sbm._run_rust_image_create(
                "/tmp/Dockerfile",
                "img",
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
                disk_mb=None,
                builder_disk_mb=None,
                dockerfile_text=None,
                context_dir=None,
                is_public=False,
                emit=emitted.append,
            )

        self.assertIn(
            {"type": "status", "message": "Building image 'img'..."},
            emitted,
        )
        self.assertIn({"type": "status", "message": "builder running"}, emitted)


class TestDeleteSandboxImage(unittest.TestCase):
    def test_deletes_sandbox_image_with_env_context(self):
        ctx = _make_ctx()

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(sbm, "_rust_delete_sandbox_image") as rust_delete,
        ):
            sbm.delete_sandbox_image("tensorlake/test:1")

        rust_delete.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "tensorlake/test:1",
            "org_1",
            "proj_1",
            "default",
        )

    def test_delete_requires_credentials(self):
        ctx = _make_ctx(api_key=None, personal_access_token=None)

        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageDeleteError):
                sbm.delete_sandbox_image("image")


class TestBuildSandboxImageFromImage(unittest.TestCase):
    def _run_build(self, image: Image, **kwargs):
        ctx = _make_ctx()
        build_ctx, rust_builder = _make_build_patches(ctx)
        captured: dict[str, object] = {}
        with build_ctx, rust_builder as rust_builder_mock:

            def fake_rust_builder(*args, **_kwargs):
                captured["args"] = args
                captured["dockerfile_path"] = args[2]
                captured["dockerfile_text"] = args[14]
                captured["context_dir"] = args[15]
                return '{"id":"tpl-1","snapshot_id":"snap-1"}'

            rust_builder_mock.side_effect = fake_rust_builder
            result = sbm.build_sandbox_image(image, **kwargs)

        return result, ctx, rust_builder_mock, captured

    def test_renders_expected_dockerfile(self):
        image = (
            Image(name="weather-image", base_image="python:3.12-slim")
            .run("apt-get update")
            .workdir("/app")
            .env("APP_ENV", "prod")
            .copy("./src", "/app/src")
        )

        _, _, rust_builder, captured = self._run_build(image)

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
        rust_builder.assert_called_once()
        self.assertEqual(rust_builder.call_args.args[3], "weather-image")
        self.assertEqual(captured["dockerfile_text"], expected_dockerfile + "\n")

    def test_registered_name_overrides_image_name(self):
        image = Image(name="default-name", base_image="python:3.12-slim")
        _, _, rust_builder, _ = self._run_build(image, registered_name="override")
        self.assertEqual(rust_builder.call_args.args[3], "override")

    def test_image_build_does_not_write_generated_dockerfile_into_context(self):
        image = Image(name="context-image", base_image="python:3.12-slim").copy(
            ".", "/app"
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "input.txt").write_text("hello", encoding="utf-8")
            _, _, _, captured = self._run_build(image, context_dir=tmpdir)
            generated = list(Path(tmpdir).glob(".tensorlake-image-*.Dockerfile"))
            # Path.resolve() may expand symlinks (e.g. /var → /private/var on
            # macOS), so compare the resolved form.
            self.assertEqual(captured["context_dir"], str(Path(tmpdir).resolve()))

        self.assertEqual(generated, [])

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


class TestBuildSandboxApplicationImage(unittest.TestCase):
    def test_function_image_build_checks_function_executor_on_path(self):
        ctx = _make_ctx()
        image = Image(name="function-image", base_image="python:3.12-slim").run(
            "python3 -m pip uninstall -y tensorlake || true"
        )
        build_ctx, rust_builder = _make_build_patches(ctx)
        captured: dict[str, object] = {}

        with build_ctx, rust_builder as rust_builder_mock:

            def fake_rust_builder(*args, **_kwargs):
                captured["dockerfile_text"] = args[14]
                return '{"id":"tpl-1","snapshot_id":"snap-1"}'

            rust_builder_mock.side_effect = fake_rust_builder
            sbm.build_sandbox_application_image(
                image,
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
            )

        dockerfile_text = captured["dockerfile_text"]
        self.assertIsInstance(dockerfile_text, str)
        dockerfile_lines = dockerfile_text.rstrip().splitlines()
        self.assertEqual(dockerfile_lines[-2], "USER root")
        install_line = dockerfile_lines[-1]
        self.assertIn(
            "--force-reinstall --no-cache-dir tensorlake==",
            install_line,
        )
        self.assertNotIn("--prefix=/usr/local", install_line)
        self.assertNotIn("sudo", install_line)
        self.assertNotIn("id -u", install_line)
        self.assertIn("RUN PIP_USER=false python3 -m pip install", install_line)
        self.assertTrue(
            install_line.endswith("&& test -x /usr/local/bin/function-executor"),
            install_line,
        )


class TestApplicationDockerfileContent(unittest.TestCase):
    def test_default_image_uses_ubuntu_minimal_base(self):
        dockerfile = dockerfile_content(Image(name="default-base"))
        self.assertTrue(dockerfile.startswith("FROM tensorlake/ubuntu-minimal\n"))

    def test_sdk_install_uses_python3_module_pip(self):
        dockerfile = dockerfile_content(Image(name="install-command"))
        self.assertIn(
            "python3 -m pip install --break-system-packages "
            "--force-reinstall --no-cache-dir tensorlake==",
            dockerfile,
        )
        self.assertNotIn("--prefix=/usr/local", dockerfile)
        self.assertIn(
            "\nUSER root\nRUN PIP_USER=false python3 -m pip install", dockerfile
        )
        self.assertNotIn("sudo", dockerfile)
        self.assertNotIn("id -u", dockerfile)
        self.assertIn("&& test -x /usr/local/bin/function-executor", dockerfile)


if __name__ == "__main__":
    unittest.main()
