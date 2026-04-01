import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

from tensorlake.cli import create_sandbox_image as create_sandbox_image_module
from tensorlake.image import Image

BUILD_CPUS = 2.0
BUILD_MEMORY_MB = 4096


class TestCreateSandboxImage(unittest.TestCase):
    def test_discover_module_images(self):
        module = ModuleType("test_module")
        image = Image(name="data-tools", base_image="python:3.11-slim")
        module.MY_IMAGE = image

        images = create_sandbox_image_module._discover_module_images(module)

        self.assertEqual(list(images.keys()), ["data-tools"])
        self.assertIs(images["data-tools"], image)

    def test_discover_module_images_ignores_non_images(self):
        module = ModuleType("test_module")
        module.SOME_STRING = "not an image"
        module.SOME_INT = 42

        images = create_sandbox_image_module._discover_module_images(module)

        self.assertEqual(images, {})

    def test_create_sandbox_image_basic(self):
        module = ModuleType("test_module")
        image = Image(name="data-tools", base_image="python:3.11-slim")
        module.MY_IMAGE = image

        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
        )

        with (
            patch.object(
                create_sandbox_image_module,
                "_build_context_from_env",
                return_value=ctx,
            ),
            patch.object(create_sandbox_image_module, "load_code", return_value=module),
            patch.object(create_sandbox_image_module, "_execute_operations"),
            patch.object(
                create_sandbox_image_module,
                "dockerfile_content",
                return_value="FROM python:3.11-slim",
            ),
            patch.object(
                create_sandbox_image_module,
                "_register_image",
                return_value={"id": "tpl-1"},
            ) as register_image,
            patch.object(create_sandbox_image_module, "_emit"),
            patch.object(
                create_sandbox_image_module, "SandboxClient"
            ) as sandbox_client_cls,
        ):
            sandbox_client = sandbox_client_cls.return_value
            sandbox_client.create_and_connect.return_value = sandbox
            sandbox_client.snapshot_and_wait.return_value = snapshot

            create_sandbox_image_module.create_sandbox_image(
                "image.py",
                image_name=None,
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
            )

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.11-slim",
            cpus=BUILD_CPUS,
            memory_mb=BUILD_MEMORY_MB,
        )
        register_image.assert_called_once_with(
            ctx,
            "data-tools",
            "FROM python:3.11-slim",
            "snap-1",
            "s3://snapshots/snap-1.tar.zst",
            False,
        )
        sandbox.terminate.assert_called_once_with()

    def test_create_sandbox_image_public(self):
        module = ModuleType("test_module")
        image = Image(name="data-tools", base_image="python:3.11-slim")
        module.MY_IMAGE = image

        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
        )

        with (
            patch.object(
                create_sandbox_image_module,
                "_build_context_from_env",
                return_value=ctx,
            ),
            patch.object(create_sandbox_image_module, "load_code", return_value=module),
            patch.object(create_sandbox_image_module, "_execute_operations"),
            patch.object(
                create_sandbox_image_module,
                "dockerfile_content",
                return_value="FROM python:3.11-slim",
            ),
            patch.object(
                create_sandbox_image_module,
                "_register_image",
                return_value={"id": "tpl-1"},
            ) as register_image,
            patch.object(create_sandbox_image_module, "_emit"),
            patch.object(
                create_sandbox_image_module, "SandboxClient"
            ) as sandbox_client_cls,
        ):
            sandbox_client = sandbox_client_cls.return_value
            sandbox_client.create_and_connect.return_value = sandbox
            sandbox_client.snapshot_and_wait.return_value = snapshot

            create_sandbox_image_module.create_sandbox_image(
                "image.py",
                image_name=None,
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
                is_public=True,
            )

        register_image.assert_called_once_with(
            ctx,
            "data-tools",
            "FROM python:3.11-slim",
            "snap-1",
            "s3://snapshots/snap-1.tar.zst",
            True,
        )

    def test_discover_module_images_rejects_duplicate_names(self):
        module = ModuleType("test_module")
        module.I1 = Image(name="same-name", base_image="python:3.11-slim")
        module.I2 = Image(name="same-name", base_image="python:3.12-slim")

        with (
            patch.object(create_sandbox_image_module, "_emit") as emit,
            self.assertRaises(SystemExit) as cm,
        ):
            create_sandbox_image_module._discover_module_images(module)

        self.assertEqual(cm.exception.code, 1)
        emit.assert_called_once()
        self.assertIn("Duplicate image name", emit.call_args[0][0]["message"])

    def test_select_image_by_name(self):
        module = ModuleType("test_module")
        module.I1 = Image(name="first", base_image="python:3.11-slim")
        module.I2 = Image(name="second", base_image="python:3.12-slim")

        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(
            snapshot_id="snap-1",
            snapshot_uri="s3://snapshots/snap-1.tar.zst",
        )

        with (
            patch.object(
                create_sandbox_image_module,
                "_build_context_from_env",
                return_value=ctx,
            ),
            patch.object(create_sandbox_image_module, "load_code", return_value=module),
            patch.object(create_sandbox_image_module, "_execute_operations"),
            patch.object(
                create_sandbox_image_module,
                "dockerfile_content",
                return_value="FROM python:3.12-slim",
            ),
            patch.object(
                create_sandbox_image_module,
                "_register_image",
                return_value={"id": "tpl-1"},
            ),
            patch.object(create_sandbox_image_module, "_emit"),
            patch.object(
                create_sandbox_image_module, "SandboxClient"
            ) as sandbox_client_cls,
        ):
            sandbox_client = sandbox_client_cls.return_value
            sandbox_client.create_and_connect.return_value = sandbox
            sandbox_client.snapshot_and_wait.return_value = snapshot

            create_sandbox_image_module.create_sandbox_image(
                "image.py",
                image_name="second",
                cpus=BUILD_CPUS,
                memory_mb=BUILD_MEMORY_MB,
            )

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.12-slim",
            cpus=BUILD_CPUS,
            memory_mb=BUILD_MEMORY_MB,
        )
