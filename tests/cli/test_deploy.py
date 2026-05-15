import os
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from tensorlake.applications import Image, application, function
from tensorlake.applications import registry as registry_module
from tensorlake.applications.remote.manifests.function_manifests import ImageRef
from tensorlake.cli import deploy as deploy_module
from tensorlake.image.sandbox_builder import (
    SandboxImageBuildError,
)


@contextmanager
def isolated_registry():
    with (
        patch.object(registry_module, "_function_registry", {}),
        patch.object(registry_module, "_class_registry", {}),
        patch.object(registry_module, "_decorators", []),
    ):
        yield


class TestDeployHelpers(unittest.TestCase):
    def test_format_error_message_does_not_include_exception_payload(self):
        message = deploy_module._format_error_message(
            "build failed", RuntimeError("secret")
        )
        self.assertEqual(message, "build failed (RuntimeError)")
        self.assertNotIn("secret", message)

    def test_format_build_failure_message_includes_inner_error(self):
        message = deploy_module._format_build_failure_message(
            "parser-image",
            RuntimeError("snapshot timed out"),
        )
        self.assertEqual(
            message,
            "image 'parser-image' build failed: snapshot timed out. check your Image() configuration and try again.",
        )

    def test_build_context_from_env_passes_expected_values(self):
        expected_context = object()
        with (
            patch.dict(
                os.environ,
                {
                    "TENSORLAKE_API_URL": "https://api.tensorlake.dev",
                    "TENSORLAKE_API_KEY": "api-key",
                    "TENSORLAKE_PAT": "pat-token",
                    "INDEXIFY_NAMESPACE": "ns",
                    "TENSORLAKE_ORGANIZATION_ID": "org-1",
                    "TENSORLAKE_PROJECT_ID": "proj-1",
                },
                clear=True,
            ),
            patch.object(
                deploy_module.Context,
                "default",
                return_value=expected_context,
            ) as context_default,
        ):
            context = deploy_module._build_context_from_env()

        self.assertIs(context, expected_context)
        context_default.assert_called_once_with(
            api_url="https://api.tensorlake.dev",
            api_key="api-key",
            personal_access_token="pat-token",
            namespace="ns",
            organization_id="org-1",
            project_id="proj-1",
            debug=False,
        )

    def test_warning_missing_secrets_returns_only_missing(self):
        auth = MagicMock()
        auth.list_secret_names.return_value = ["EXISTING"]

        missing = deploy_module._warning_missing_secrets(auth, ["EXISTING", "MISSING"])

        self.assertEqual(missing, ["MISSING"])
        auth.list_secret_names.assert_called_once_with(page_size=100)

    def test_error_event_includes_traceback_when_debug_enabled(self):
        with patch.dict(os.environ, {"TENSORLAKE_DEBUG": "1"}, clear=True):
            try:
                raise RuntimeError("boom")
            except RuntimeError as e:
                event = deploy_module._error_event("deploy failed", e)

        self.assertEqual(event["type"], "error")
        self.assertEqual(event["message"], "deploy failed (RuntimeError)")
        self.assertEqual(event["details"], "RuntimeError: boom")
        self.assertIn("RuntimeError: boom", event["traceback"])


class TestPrepareImages(unittest.TestCase):
    """The sandbox-image path that `tl deploy` takes for every function image."""

    def test_deduplicates_images_by_id_and_returns_image_refs(self):
        image_a = Image(name="image-a")
        image_b = Image(name="image-b")

        with isolated_registry():

            @application()
            @function(image=image_a)
            def app_one() -> str:
                return "a"

            @function(image=image_a)
            def child_one(x: str) -> str:
                return x

            @application()
            @function(image=image_b)
            def app_two() -> str:
                return "b"

            functions = registry_module.get_functions()

            def fake_build(image, *, emit):
                return {"name": f"{image.name}-template"}

            with (
                patch.object(deploy_module, "build_sandbox_image", side_effect=fake_build) as build,
                patch.object(deploy_module, "_emit"),
            ):
                refs = deploy_module._prepare_images(functions)

        self.assertEqual(build.call_count, 2)
        # Both function refs from app_one share image_a, so the map has two
        # distinct entries keyed by Image._id pointing at the right template.
        self.assertEqual(
            {ref.id for ref in refs.values()},
            {"image-a-template", "image-b-template"},
        )
        self.assertTrue(
            all(ref.kind == "sandbox_template" for ref in refs.values())
        )
        self.assertIn(image_a._id, refs)
        self.assertIn(image_b._id, refs)

    def test_falls_back_to_image_name_when_platform_response_lacks_name(self):
        image = Image(name="my-fn-image")

        with isolated_registry():

            @application()
            @function(image=image)
            def app() -> str:
                return ""

            functions = registry_module.get_functions()

            with (
                patch.object(
                    deploy_module,
                    "build_sandbox_image",
                    return_value={},
                ),
                patch.object(deploy_module, "_emit"),
            ):
                refs = deploy_module._prepare_images(functions)

        self.assertEqual(refs[image._id], ImageRef(kind="sandbox_template", id="my-fn-image"))

    def test_sandbox_build_failure_emits_build_failed_and_exits(self):
        image = Image(name="broken-image")

        with isolated_registry():

            @application()
            @function(image=image)
            def app() -> str:
                return ""

            functions = registry_module.get_functions()

            with (
                patch.object(
                    deploy_module,
                    "build_sandbox_image",
                    side_effect=SandboxImageBuildError("snapshot upload timed out"),
                ),
                patch.object(deploy_module, "_emit") as emit,
            ):
                with self.assertRaises(SystemExit) as exc:
                    deploy_module._prepare_images(functions)

        self.assertEqual(exc.exception.code, 1)
        failure = next(
            call.args[0]
            for call in emit.call_args_list
            if call.args[0]["type"] == "build_failed"
        )
        self.assertEqual(failure["image"], "broken-image")
        self.assertIn("snapshot upload timed out", failure["error"])


class TestDeployEntrypoint(unittest.TestCase):
    def _make_auth_context(self):
        return SimpleNamespace(
            api_url="https://api.tensorlake.ai",
            api_key="api-key",
            personal_access_token=None,
            organization_id="org-1",
            project_id="proj-1",
            cloud_client=MagicMock(),
        )

    def test_deploy_emits_user_friendly_import_error(self):
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=self._make_auth_context(),
            ),
            patch.object(deploy_module, "load_code", side_effect=ImportError("boom")),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    upgrade_running_requests=False,
                )

        self.assertEqual(exc.exception.code, 1)
        event = emit.call_args_list[-1].args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn("failed to import application file", event["message"])
        self.assertEqual(event["details"], "ImportError: boom")

    def test_deploy_emits_validation_failed_when_validation_has_errors(self):
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=self._make_auth_context(),
            ),
            patch.object(deploy_module, "load_code"),
            patch.object(
                deploy_module, "validate_loaded_applications", return_value=["x"]
            ),
            patch.object(
                deploy_module,
                "format_validation_messages",
                return_value=[
                    {
                        "severity": "error",
                        "message": "application is invalid",
                        "location": "my_app.py:1",
                    }
                ],
            ),
            patch.object(deploy_module, "has_error_message", return_value=True),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    upgrade_running_requests=False,
                )

        self.assertEqual(exc.exception.code, 1)
        types = [call.args[0]["type"] for call in emit.call_args_list]
        self.assertIn("validation", types)
        self.assertIn("validation_failed", types)

    def test_deploy_runs_build_and_passes_image_refs_to_deploy_step(self):
        auth = self._make_auth_context()
        with isolated_registry():
            image = Image(name="hello-image")

            @application()
            @function(image=image)
            def hello() -> str:
                return "hi"

            functions = registry_module.get_functions()
            image_refs = {
                image._id: ImageRef(kind="sandbox_template", id="hello-image"),
            }

            with (
                patch.object(deploy_module, "_build_context_from_env", return_value=auth),
                patch.object(deploy_module, "load_code"),
                patch.object(deploy_module, "validate_loaded_applications", return_value=[]),
                patch.object(deploy_module, "format_validation_messages", return_value=[]),
                patch.object(deploy_module, "has_error_message", return_value=False),
                patch.object(deploy_module, "list_secret_names", return_value=[]),
                patch.object(deploy_module, "_warning_missing_secrets", return_value=[]),
                patch.object(deploy_module, "get_functions", return_value=functions),
                patch.object(
                    deploy_module, "_prepare_images", return_value=image_refs
                ) as prepare,
                patch.object(deploy_module, "deploy_applications") as deploy_apps,
                patch.object(
                    deploy_module,
                    "example_application_curl_command",
                    return_value="curl",
                ),
                patch.object(deploy_module, "_emit"),
            ):
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    upgrade_running_requests=True,
                )

        prepare.assert_called_once_with(functions)
        deploy_apps.assert_called_once_with(
            applications_file_path=os.path.abspath("my_app.py"),
            upgrade_running_requests=True,
            load_source_dir_modules=False,
            api_client=auth.cloud_client,
            image_refs=image_refs,
        )


if __name__ == "__main__":
    unittest.main()
