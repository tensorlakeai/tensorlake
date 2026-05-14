import tempfile
import unittest
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, MagicMock, patch

from tensorlake.image import Image
from tensorlake.image import sandbox_builder as sbm

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

    def test_load_errors_raise_SandboxImageLoadError(self):
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

        self.assertEqual(captured["context_dir"], tmpdir)
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


class TestRegisterImage(unittest.TestCase):
    """Coverage for _register_image's branching on api_key vs PAT auth."""

    def _make_ctx(self, **overrides):
        defaults = dict(
            api_url="https://api.tensorlake.test",
            api_key=None,
            personal_access_token=None,
            organization_id=None,
            project_id=None,
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def _mock_response(self):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"id": "tpl-1"}
        return resp

    def test_api_key_uses_scope_less_route_without_forwarded_headers(self):
        ctx = self._make_ctx(api_key="tl_apiKey_abc")
        with patch.object(sbm, "httpx") as httpx_mock:
            httpx_mock.post.return_value = self._mock_response()
            sbm._register_image(
                ctx,
                "img",
                "FROM python",
                "snap-1",
                "sbx-1",
                "s3://x",
                100,
                200,
            )

        call_kwargs = httpx_mock.post.call_args
        url = call_kwargs.args[0]
        headers = call_kwargs.kwargs["headers"]
        self.assertEqual(
            url, "https://api.tensorlake.test/platform/v1/sandbox-templates"
        )
        self.assertEqual(headers["Authorization"], "Bearer tl_apiKey_abc")
        self.assertNotIn("X-Forwarded-Organization-Id", headers)
        self.assertNotIn("X-Forwarded-Project-Id", headers)

    def test_api_key_ignores_env_var_org_project(self):
        # Even if the user has org/project env vars set, the API-key path
        # ignores them — platform-api resolves scope from the bearer token.
        ctx = self._make_ctx(
            api_key="tl_apiKey_abc",
            organization_id="org_env",
            project_id="proj_env",
        )
        with patch.object(sbm, "httpx") as httpx_mock:
            httpx_mock.post.return_value = self._mock_response()
            sbm._register_image(
                ctx,
                "img",
                "FROM python",
                "snap-1",
                "sbx-1",
                "s3://x",
                100,
                200,
            )

        url = httpx_mock.post.call_args.args[0]
        self.assertNotIn("/organizations/", url)
        self.assertNotIn("/projects/", url)

    def test_pat_keeps_scoped_url_and_forwarded_headers(self):
        ctx = self._make_ctx(
            personal_access_token="tl_pat_xyz",
            organization_id="org_1",
            project_id="proj_1",
        )
        with patch.object(sbm, "httpx") as httpx_mock:
            httpx_mock.post.return_value = self._mock_response()
            sbm._register_image(
                ctx,
                "img",
                "FROM python",
                "snap-1",
                "sbx-1",
                "s3://x",
                100,
                200,
            )

        url = httpx_mock.post.call_args.args[0]
        headers = httpx_mock.post.call_args.kwargs["headers"]
        self.assertEqual(
            url,
            "https://api.tensorlake.test/platform/v1/organizations/org_1/projects/proj_1/sandbox-templates",
        )
        self.assertEqual(headers["Authorization"], "Bearer tl_pat_xyz")
        self.assertEqual(headers["X-Forwarded-Organization-Id"], "org_1")
        self.assertEqual(headers["X-Forwarded-Project-Id"], "proj_1")

    def test_pat_without_scope_raises(self):
        ctx = self._make_ctx(personal_access_token="tl_pat_xyz")
        with self.assertRaisesRegex(RuntimeError, "Personal Access Token"):
            sbm._register_image(
                ctx,
                "img",
                "FROM python",
                "snap-1",
                "sbx-1",
                "s3://x",
                100,
                200,
            )

    def test_no_credentials_raises(self):
        ctx = self._make_ctx()
        with self.assertRaisesRegex(RuntimeError, "Missing TENSORLAKE_API_KEY"):
            sbm._register_image(
                ctx,
                "img",
                "FROM python",
                "snap-1",
                "sbx-1",
                "s3://x",
                100,
                200,
            )


if __name__ == "__main__":
    unittest.main()
