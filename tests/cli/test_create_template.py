import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, patch

from tensorlake.applications import Image
from tensorlake.cli import create_template as create_template_module


class TestCreateTemplate(unittest.TestCase):
    def test_collect_image_infos_includes_standalone_module_images(self):
        module = ModuleType("test_module")
        standalone_image = Image(name="data-tools", base_image="python:3.11-slim")
        module.SANDBOX_IMAGE = standalone_image

        with patch.object(create_template_module, "image_infos", return_value={}):
            infos = create_template_module._collect_image_infos(module)

        self.assertEqual(list(infos), [standalone_image])
        self.assertEqual(infos[standalone_image].image, standalone_image)
        self.assertEqual(infos[standalone_image].functions, [])

    def test_create_template_accepts_standalone_image_module(self):
        module = ModuleType("test_module")
        standalone_image = Image(name="data-tools", base_image="python:3.11-slim")
        module.SANDBOX_IMAGE = standalone_image

        ctx = MagicMock()
        sandbox = MagicMock()
        sandbox.sandbox_id = "sbx-1"
        snapshot = SimpleNamespace(snapshot_id="snap-1")

        with (
            patch.object(
                create_template_module, "_build_context_from_env", return_value=ctx
            ),
            patch.object(create_template_module, "load_code", return_value=module),
            patch.object(create_template_module, "image_infos", return_value={}),
            patch.object(create_template_module, "_execute_operations"),
            patch.object(
                create_template_module,
                "dockerfile_content",
                return_value="FROM python:3.11-slim",
            ),
            patch.object(
                create_template_module,
                "_register_template",
                return_value={"id": "tpl-1"},
            ) as register_template,
            patch.object(create_template_module, "_emit"),
            patch.object(create_template_module, "SandboxClient") as sandbox_client_cls,
        ):
            sandbox_client = sandbox_client_cls.return_value
            sandbox_client.create_and_connect.return_value = sandbox
            sandbox_client.snapshot_and_wait.return_value = snapshot

            create_template_module.create_template(
                "template.py",
                image_name=None,
                template_name="data-tools-template",
            )

        sandbox_client.create_and_connect.assert_called_once_with(
            image="python:3.11-slim",
            cpus=2,
            memory_mb=4096,
        )
        register_template.assert_called_once_with(
            ctx,
            "data-tools-template",
            "FROM python:3.11-slim",
            "snap-1",
        )
        sandbox.terminate.assert_called_once_with()
