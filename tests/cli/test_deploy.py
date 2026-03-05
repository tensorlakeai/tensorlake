import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.cli import deploy as deploy_module


class TestDeployHelpers(unittest.TestCase):
    def test_format_error_message_does_not_include_exception_payload(self):
        message = deploy_module._format_error_message(
            "build failed", RuntimeError("secret")
        )
        self.assertEqual(message, "build failed (RuntimeError)")
        self.assertNotIn("secret", message)

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


class TestDeployEntrypoints(unittest.TestCase):
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
            patch.object(deploy_module, "ImageBuilderV2Client"),
            patch.object(deploy_module, "load_code", side_effect=ImportError("boom")),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    parallel_builds=False,
                    upgrade_running_requests=False,
                )

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(emit.call_args_list[0].args[0]["type"], "status")
        event = emit.call_args_list[-1].args[0]
        self.assertEqual(event["type"], "error")
        self.assertIn(
            "failed to import application file",
            event["message"],
        )
        self.assertEqual(event["details"], "ImportError: boom")

    def test_deploy_emits_validation_failed_when_validation_has_errors(self):
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=self._make_auth_context(),
            ),
            patch.object(deploy_module, "ImageBuilderV2Client"),
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
                    parallel_builds=False,
                    upgrade_running_requests=False,
                )

        self.assertEqual(exc.exception.code, 1)
        event_types = [call.args[0]["type"] for call in emit.call_args_list]
        self.assertIn("validation", event_types)
        self.assertEqual(event_types[-1], "validation_failed")

    def test_deploy_runs_build_and_deploy_flow(self):
        prepare_images = AsyncMock()
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=self._make_auth_context(),
            ),
            patch.object(deploy_module, "ImageBuilderV2Client"),
            patch.object(deploy_module, "load_code"),
            patch.object(
                deploy_module, "validate_loaded_applications", return_value=[]
            ),
            patch.object(deploy_module, "format_validation_messages", return_value=[]),
            patch.object(deploy_module, "has_error_message", return_value=False),
            patch.object(deploy_module, "list_secret_names", return_value=[]),
            patch.object(deploy_module, "_warning_missing_secrets", return_value=[]),
            patch.object(deploy_module, "get_functions", return_value=["fn"]),
            patch.object(deploy_module, "_prepare_images_v2", prepare_images),
            patch.object(deploy_module, "_deploy_applications") as deploy_apps,
            patch.object(deploy_module, "_emit"),
        ):
            deploy_module.deploy(
                application_file_path="my_app.py",
                parallel_builds=True,
                upgrade_running_requests=True,
            )

        prepare_images.assert_awaited_once()
        deploy_apps.assert_called_once()
        call_kwargs = deploy_apps.call_args.kwargs
        self.assertEqual(
            call_kwargs["application_file_path"], os.path.abspath("my_app.py")
        )
        self.assertTrue(call_kwargs["upgrade_running_requests"])
        self.assertEqual(call_kwargs["functions"], ["fn"])

    def test_deploy_entrypoint_emits_error_for_unhandled_exception(self):
        with (
            patch("sys.argv", ["tensorlake-deploy", "my_app.py"]),
            patch.object(deploy_module, "deploy", side_effect=RuntimeError("boom")),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                deploy_module.deploy_entrypoint()

        self.assertEqual(exc.exception.code, 1)
        self.assertEqual(emit.call_count, 1)
        event = emit.call_args.args[0]
        self.assertEqual(event["type"], "error")
        self.assertEqual(event["message"], "deploy failed (RuntimeError)")
        self.assertEqual(event["details"], "RuntimeError: boom")


if __name__ == "__main__":
    unittest.main()
