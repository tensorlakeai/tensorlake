import os
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


def _make_import_patches(ctx):
    """Common mock bundle for exercising import_sandbox_image without networking."""
    return (
        patch.object(sbm, "_build_context_from_env", return_value=ctx),
        patch.object(
            sbm,
            "_rust_import_sandbox_image",
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
            False,
            None,
            None,
            cas=False,
            emit=ANY,
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
            False,
            None,
            None,
            cas=False,
            emit=ANY,
        )

    def test_missing_dockerfile_raises_load_error(self):
        with self.assertRaises(sbm.SandboxImageLoadError):
            sbm.build_sandbox_image("/nonexistent/Dockerfile")

    def test_docker_compat_is_forwarded(self):
        ctx = _make_ctx()

        with tempfile.TemporaryDirectory() as tmpdir:
            dockerfile_path = Path(tmpdir) / "Dockerfile"
            dockerfile_path.write_text("FROM python:3.12-slim\n", encoding="utf-8")

            build_ctx, rust_builder = _make_build_patches(ctx)
            with build_ctx, rust_builder as rust_builder_mock:
                sbm.build_sandbox_image(str(dockerfile_path), docker_compat=True)

        self.assertIs(rust_builder_mock.call_args.args[14], True)

    def test_rust_builder_events_are_forwarded_to_emit(self):
        ctx = _make_ctx()
        emitted: list[dict] = []

        def fake_rust_builder(*args, **kwargs):
            kwargs["emit"]({"type": "status", "message": "builder running"})
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
                docker_compat=False,
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


class TestFindSandboxImageByName(unittest.TestCase):
    def test_returns_template_dict_with_env_context(self):
        ctx = _make_ctx()

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(
                sbm,
                "_rust_find_sandbox_image_by_name",
                # The Rust core proxies the platform API verbatim, which uses
                # camelCase keys (e.g. snapshotId, rootfsDiskBytes).
                return_value=(
                    '{"id":"tpl-1","name":"tensorlake/test:1",'
                    '"snapshotId":"snap-1","rootfsDiskBytes":1024}'
                ),
            ) as rust_find,
        ):
            result = sbm.find_sandbox_image_by_name("tensorlake/test:1")

        # The public Python API normalizes keys to snake_case.
        self.assertEqual(
            result,
            {
                "id": "tpl-1",
                "name": "tensorlake/test:1",
                "snapshot_id": "snap-1",
                "rootfs_disk_bytes": 1024,
            },
        )
        rust_find.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "tensorlake/test:1",
            "org_1",
            "proj_1",
            "default",
        )

    def test_returns_none_when_not_found(self):
        ctx = _make_ctx()

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(sbm, "_rust_find_sandbox_image_by_name", return_value=None),
        ):
            self.assertIsNone(sbm.find_sandbox_image_by_name("missing"))

    def test_requires_credentials(self):
        ctx = _make_ctx(api_key=None, personal_access_token=None)

        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageLookupError):
                sbm.find_sandbox_image_by_name("image")

    def test_requires_org_and_project_context(self):
        ctx = _make_ctx(organization_id=None, project_id=None)

        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageLookupError):
                sbm.find_sandbox_image_by_name("image")

    def test_empty_name_raises_type_error(self):
        with self.assertRaises(TypeError):
            sbm.find_sandbox_image_by_name("")


class TestListSandboxImages(unittest.TestCase):
    def test_returns_template_list_with_env_context(self):
        ctx = _make_ctx()

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(
                sbm,
                "_rust_list_sandbox_images",
                # The Rust core proxies the platform API verbatim, which uses
                # camelCase keys (e.g. snapshotId).
                return_value='[{"id":"tpl-1","name":"image-a","snapshotId":"snap-a"},'
                '{"id":"tpl-2","name":"image-b","snapshotId":"snap-b"}]',
            ) as rust_list,
        ):
            result = sbm.list_sandbox_images()

        # The public Python API normalizes keys to snake_case.
        self.assertEqual(
            result,
            [
                {"id": "tpl-1", "name": "image-a", "snapshot_id": "snap-a"},
                {"id": "tpl-2", "name": "image-b", "snapshot_id": "snap-b"},
            ],
        )
        rust_list.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "org_1",
            "proj_1",
            "default",
        )

    def test_returns_empty_list_when_no_images(self):
        ctx = _make_ctx()

        with (
            patch.object(sbm, "_build_context_from_env", return_value=ctx),
            patch.object(sbm, "_rust_list_sandbox_images", return_value="[]"),
        ):
            self.assertEqual(sbm.list_sandbox_images(), [])

    def test_requires_credentials(self):
        ctx = _make_ctx(api_key=None, personal_access_token=None)

        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageLookupError):
                sbm.list_sandbox_images()

    def test_requires_org_and_project_context(self):
        ctx = _make_ctx(organization_id=None, project_id=None)

        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageLookupError):
                sbm.list_sandbox_images()


class TestImportSandboxImage(unittest.TestCase):
    def test_imports_registry_image_with_reference_and_no_dockerfile(self):
        ctx = _make_ctx()

        build_ctx, rust_importer = _make_import_patches(ctx)
        with build_ctx, rust_importer as rust_importer_mock:
            sbm.import_sandbox_image(
                "pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
            )

        # The import path delegates to the dedicated Rust entry point with no
        # Dockerfile fields — the image reference is a first-class argument.
        rust_importer_mock.assert_called_once_with(
            "https://api.tensorlake.test",
            "tl_apiKey_abc",
            "pytorch/pytorch:2.4.1-cuda12.1-cudnn9-runtime",  # image_reference
            "pytorch",  # default name derived from the reference
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
            False,
            cas=False,
            emit=ANY,
        )

    def test_default_name_strips_registry_path_and_digest(self):
        self.assertEqual(
            sbm._default_registered_name_from_image("ghcr.io/org/app@sha256:abc"),
            "app",
        )
        self.assertEqual(
            sbm._default_registered_name_from_image("pytorch/pytorch:2.4.1"),
            "pytorch",
        )
        self.assertEqual(
            sbm._default_registered_name_from_image("ubuntu"),
            "ubuntu",
        )

    def test_explicit_registered_name_overrides_default(self):
        ctx = _make_ctx()
        build_ctx, rust_importer = _make_import_patches(ctx)
        with build_ctx, rust_importer as rust_importer_mock:
            sbm.import_sandbox_image(
                "pytorch/pytorch:2.4.1",
                registered_name="override",
            )
        self.assertEqual(rust_importer_mock.call_args.args[3], "override")

    def test_docker_compat_is_forwarded(self):
        ctx = _make_ctx()
        build_ctx, rust_importer = _make_import_patches(ctx)
        with build_ctx, rust_importer as rust_importer_mock:
            sbm.import_sandbox_image("pytorch/pytorch:2.4.1", docker_compat=True)
        self.assertIs(rust_importer_mock.call_args.args[14], True)

    def test_empty_reference_raises_build_error(self):
        with self.assertRaises(sbm.SandboxImageBuildError):
            sbm.import_sandbox_image("   ")

    def test_requires_credentials(self):
        ctx = _make_ctx(api_key=None, personal_access_token=None)
        with patch.object(sbm, "_build_context_from_env", return_value=ctx):
            with self.assertRaises(sbm.SandboxImageBuildError):
                sbm.import_sandbox_image("pytorch/pytorch:2.4.1")


class TestBuildSandboxImageFromImage(unittest.TestCase):
    def _run_build(self, image: Image, **kwargs):
        ctx = _make_ctx()
        build_ctx, rust_builder = _make_build_patches(ctx)
        captured: dict[str, object] = {}
        with build_ctx, rust_builder as rust_builder_mock:

            def fake_rust_builder(*args, **_kwargs):
                captured["args"] = args
                captured["dockerfile_path"] = args[2]
                captured["dockerfile_text"] = args[15]
                context_dir = args[16]
                captured["context_dir"] = context_dir
                # Snapshot the context contents now — a temp dir built for an
                # Image without context_dir is removed once the build returns.
                captured["context_files"] = (
                    sorted(
                        str(p.relative_to(context_dir))
                        for p in Path(context_dir).iterdir()
                    )
                    if context_dir is not None and Path(context_dir).is_dir()
                    else None
                )
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

        # COPY ops require an explicit context_dir.
        _, _, rust_builder, captured = self._run_build(image, context_dir=".")

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

    def test_default_context_is_empty_not_cwd(self):
        # Without an explicit context_dir, an Image build must NOT upload the
        # current working directory (which has no Dockerfile in it). It uses a
        # throwaway empty temp dir instead, so only the generated Dockerfile
        # text is built — nothing from cwd is archived.
        image = Image(name="no-context-image", base_image="python:3.12-slim").run(
            "pip install numpy"
        )
        _, _, _, captured = self._run_build(image)

        context_dir = captured["context_dir"]
        self.assertIsInstance(context_dir, str)
        self.assertNotEqual(
            Path(str(context_dir)).resolve(),
            Path(os.getcwd()).resolve(),
            "Image build must not default its build context to cwd",
        )
        # The temp dir is cleaned up once the build returns.
        self.assertFalse(Path(str(context_dir)).exists())

    def test_default_empty_context_has_no_files(self):
        # An image with no COPY/ADD host-file ops gets an empty context — just
        # the (separately passed) Dockerfile text, nothing archived from disk.
        image = Image(name="no-copy-image", base_image="python:3.12-slim").run(
            "pip install numpy"
        )
        _, _, _, captured = self._run_build(image)
        self.assertEqual(captured["context_files"], [])

    def test_copy_without_context_dir_raises(self):
        # COPY/ADD that reads host files needs a context. Without context_dir
        # the build must fail fast with a clear, actionable message rather than
        # guessing or uploading the whole cwd.
        image = Image(name="copy-image", base_image="python:3.12-slim").copy(
            "requirements.txt", "/tmp/requirements.txt"
        )
        with self.assertRaises(sbm.SandboxImageBuildError) as ctx:
            self._run_build(image)
        self.assertIn("context_dir", str(ctx.exception))

    def test_add_without_context_dir_raises(self):
        image = Image(name="add-image", base_image="python:3.12-slim").add(
            "./data", "/app/data"
        )
        with self.assertRaises(sbm.SandboxImageBuildError) as ctx:
            self._run_build(image)
        self.assertIn("context_dir", str(ctx.exception))

    def test_remote_add_does_not_require_context(self):
        # A remote ADD <url> reads nothing from the host, so it needs no
        # context and builds with an empty one.
        image = Image(name="url-image", base_image="python:3.12-slim").add(
            "https://example.com/data.tar.gz", "/app/data.tar.gz"
        )
        _, _, _, captured = self._run_build(image)
        self.assertEqual(captured["context_files"], [])

    def test_copy_from_stage_does_not_require_context(self):
        # COPY --from=<stage> reads from another build stage, not the host.
        image = Image(name="stage-image", base_image="python:3.12-slim").copy(
            "/build/app", "/app", options={"from": "builder"}
        )
        _, _, _, captured = self._run_build(image)
        self.assertEqual(captured["context_files"], [])

    def test_run_bind_mount_without_context_dir_raises(self):
        # A RUN bind mount reads the build context, so it needs a context.
        # `type=bind` is the default, so omitting it must also be detected.
        for mount in (
            "type=bind,source=.,target=/src",
            "target=/src",
        ):
            image = Image(name="mount-image", base_image="python:3.12-slim").run(
                "make -C /src", options={"mount": mount}
            )
            with self.assertRaises(sbm.SandboxImageBuildError) as ctx:
                self._run_build(image)
            self.assertIn("context_dir", str(ctx.exception))

    def test_run_mount_from_stage_does_not_require_context(self):
        # A `from=` bind mount reads another stage/image, not the host.
        image = Image(name="mount-stage", base_image="python:3.12-slim").run(
            "make", options={"mount": "type=bind,from=builder,target=/src"}
        )
        _, _, _, captured = self._run_build(image)
        self.assertEqual(captured["context_files"], [])

    def test_run_cache_mount_does_not_require_context(self):
        # Non-bind mounts (cache/tmpfs/secret/ssh) don't read the build context.
        image = Image(name="mount-cache", base_image="python:3.12-slim").run(
            "pip install -r req.txt",
            options={"mount": "type=cache,target=/root/.cache"},
        )
        _, _, _, captured = self._run_build(image)
        self.assertEqual(captured["context_files"], [])

    def test_copy_with_context_dir_uploads_dir_as_is(self):
        # With an explicit context_dir, COPY ops are allowed and the directory
        # is uploaded as-is (resolved), like `docker build <dir>`.
        image = Image(name="copy-image", base_image="python:3.12-slim").copy(
            "requirements.txt", "/tmp/requirements.txt"
        )
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "requirements.txt").write_text("flask\n", encoding="utf-8")
            _, _, _, captured = self._run_build(image, context_dir=tmp)
            self.assertEqual(captured["context_dir"], str(Path(tmp).resolve()))

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
                captured["dockerfile_text"] = args[15]
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
