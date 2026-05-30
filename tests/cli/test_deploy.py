import asyncio
import os
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake.applications import Image, application, function
from tensorlake.applications import registry as registry_module
from tensorlake.cli import deploy as deploy_module


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
            RuntimeError("rootfs builder failed"),
        )
        self.assertEqual(
            message,
            "image 'parser-image' build failed: rootfs builder failed. check your Image() configuration and try again.",
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

    @contextmanager
    def _successful_deploy_patches(
        self,
        auth,
        functions,
        *,
        declared_secret_names=None,
        missing_secret_names=None,
    ):
        with (
            patch.object(deploy_module, "_build_context_from_env", return_value=auth),
            patch.object(deploy_module, "load_code"),
            patch.object(
                deploy_module, "validate_loaded_applications", return_value=[]
            ),
            patch.object(deploy_module, "format_validation_messages", return_value=[]),
            patch.object(deploy_module, "has_error_message", return_value=False),
            patch.object(
                deploy_module,
                "list_secret_names",
                return_value=declared_secret_names or [],
            ),
            patch.object(
                deploy_module,
                "_warning_missing_secrets",
                return_value=missing_secret_names or [],
            ),
            patch.object(deploy_module, "get_functions", return_value=functions),
        ):
            yield

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
        event_types = [call.args[0]["type"] for call in emit.call_args_list]
        self.assertIn("validation", event_types)
        self.assertEqual(event_types[-1], "validation_failed")

    def test_deploy_runs_build_and_deploy_flow(self):
        prepare_images = AsyncMock()
        auth = self._make_auth_context()
        application = SimpleNamespace(_name="app-one")
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=auth,
            ),
            patch.object(deploy_module, "load_code"),
            patch.object(
                deploy_module, "validate_loaded_applications", return_value=[]
            ),
            patch.object(deploy_module, "format_validation_messages", return_value=[]),
            patch.object(deploy_module, "has_error_message", return_value=False),
            patch.object(deploy_module, "list_secret_names", return_value=[]),
            patch.object(deploy_module, "_warning_missing_secrets", return_value=[]),
            patch.object(deploy_module, "get_functions", return_value=["fn"]),
            patch.object(deploy_module, "_prepare_images", prepare_images),
            patch.object(deploy_module, "deploy_applications") as deploy_apps,
            patch.object(
                deploy_module,
                "filter_applications",
                return_value=iter([application]),
            ),
            patch.object(
                deploy_module,
                "example_application_curl_command",
                return_value="curl https://example.test",
            ),
            patch.object(deploy_module, "_emit") as emit,
        ):
            deploy_module.deploy(
                application_file_path="my_app.py",
                upgrade_running_requests=True,
            )

        prepare_images.assert_awaited_once_with(
            ["fn"],
            context_dir=os.path.dirname(os.path.abspath("my_app.py")),
            build_envs=None,
        )
        deploy_apps.assert_called_once_with(
            applications_file_path=os.path.abspath("my_app.py"),
            upgrade_running_requests=True,
            load_source_dir_modules=False,
            api_client=auth.cloud_client,
        )
        deployed_event = next(
            call.args[0]
            for call in emit.call_args_list
            if call.args[0]["type"] == "deployed"
        )
        self.assertEqual(deployed_event["type"], "deployed")
        self.assertEqual(deployed_event["application"], "app-one")
        self.assertEqual(deployed_event["curl_command"], "curl https://example.test")

    def test_deploy_emits_missing_secret_names(self):
        prepare_images = AsyncMock()
        auth = self._make_auth_context()
        with (
            self._successful_deploy_patches(
                auth,
                ["fn"],
                declared_secret_names=["EXISTING", "MISSING_ONE", "MISSING_TWO"],
                missing_secret_names=["MISSING_ONE", "MISSING_TWO"],
            ),
            patch.object(deploy_module, "_prepare_images", prepare_images),
            patch.object(deploy_module, "deploy_applications"),
            patch.object(
                deploy_module,
                "filter_applications",
                return_value=iter([]),
            ),
            patch.object(deploy_module, "_emit") as emit,
        ):
            deploy_module.deploy(
                application_file_path="my_app.py",
                upgrade_running_requests=False,
            )

        missing_event = next(
            call.args[0]
            for call in emit.call_args_list
            if call.args[0]["type"] == "missing_secrets"
        )
        self.assertEqual(missing_event["count"], 2)
        self.assertEqual(missing_event["names"], ["MISSING_ONE", "MISSING_TWO"])

    def test_deploy_builds_each_explicit_application_image_as_sandbox_template(self):
        auth = self._make_auth_context()
        with isolated_registry():
            parser_image = Image(name="parser-image")
            agent_image = Image(name="agent-image")
            code_exec_image = Image(name="code-exec-image")

            @function()
            def get_extraction_schema(payload):
                return payload

            @function()
            def get_document_content(payload):
                return payload

            @function(image=parser_image)
            def upload_and_parse_document(file):
                return file

            @function(image=agent_image)
            def run_finance_agent(parsed_document):
                return parsed_document

            @function(image=agent_image)
            def execute_sql_query(query):
                return query

            @function(image=code_exec_image)
            def execute_code(code):
                return code

            @function(image=agent_image)
            def run_query_agent(question):
                return question

            @application()
            @function()
            def finance_analyzer(file):
                parsed_document = upload_and_parse_document(file)
                return run_finance_agent(parsed_document)

            @application()
            @function(image=agent_image)
            def finance_query(question):
                execute_sql_query(question)
                return run_query_agent(question)

            functions = [
                get_extraction_schema,
                get_document_content,
                upload_and_parse_document,
                run_finance_agent,
                execute_sql_query,
                execute_code,
                run_query_agent,
                finance_analyzer,
                finance_query,
            ]
            with (
                self._successful_deploy_patches(auth, functions),
                patch.object(
                    deploy_module,
                    "build_sandbox_application_image",
                    return_value={},
                ) as build_image,
                patch.object(deploy_module, "_deploy_applications"),
                patch.object(deploy_module, "_emit") as emit,
            ):
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    upgrade_running_requests=False,
                )

        self.assertEqual(
            [call.kwargs["registered_name"] for call in build_image.call_args_list],
            ["parser-image", "agent-image", "code-exec-image"],
        )
        self.assertEqual(
            [call.args[0] for call in build_image.call_args_list],
            [parser_image, agent_image, code_exec_image],
        )
        emit.assert_any_call({"type": "build_done"})

    def test_deploy_builds_shared_explicit_image_once(self):
        auth = self._make_auth_context()
        shared_image = Image(name="shared-image")

        application = SimpleNamespace(
            _name="app-one",
            _function_config=SimpleNamespace(
                function_name="app-one",
                image=shared_image,
            ),
            _application_config=SimpleNamespace(version="v1"),
        )
        helper = SimpleNamespace(
            _function_config=SimpleNamespace(
                function_name="helper-one",
                image=shared_image,
            ),
            _application_config=None,
        )
        functions = [application, helper]

        with (
            patch.object(deploy_module, "_build_context_from_env", return_value=auth),
            patch.object(deploy_module, "load_code"),
            patch.object(
                deploy_module, "validate_loaded_applications", return_value=[]
            ),
            patch.object(deploy_module, "format_validation_messages", return_value=[]),
            patch.object(deploy_module, "has_error_message", return_value=False),
            patch.object(deploy_module, "list_secret_names", return_value=[]),
            patch.object(deploy_module, "_warning_missing_secrets", return_value=[]),
            patch.object(deploy_module, "get_functions", return_value=functions),
            patch.object(
                deploy_module,
                "build_sandbox_application_image",
                return_value={},
            ) as build_image,
            patch.object(deploy_module, "_deploy_applications") as deploy_apps,
            patch.object(deploy_module, "_emit") as emit,
        ):
            deploy_module.deploy(
                application_file_path="my_app.py",
                upgrade_running_requests=False,
            )

        build_image.assert_called_once()
        self.assertEqual(build_image.call_args.args[0], shared_image)
        self.assertEqual(
            build_image.call_args.kwargs["registered_name"], "shared-image"
        )
        build_start_events = [
            call.args[0]
            for call in emit.call_args_list
            if call.args and call.args[0]["type"] == "build_start"
        ]
        self.assertEqual(
            build_start_events,
            [{"type": "build_start", "image": "shared-image"}],
        )
        deploy_apps.assert_called_once()

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

    def test_deploy_entrypoint_passes_build_envs(self):
        with (
            patch(
                "sys.argv",
                [
                    "tensorlake-deploy",
                    "my_app.py",
                    "--build-env",
                    "PIP_INDEX_URL=https://test.pypi.org/simple/",
                    "--build-env",
                    "PIP_EXTRA_INDEX_URL=https://pypi.org/simple/",
                ],
            ),
            patch.object(deploy_module, "deploy") as deploy,
        ):
            deploy_module.deploy_entrypoint()

        self.assertEqual(
            deploy.call_args.kwargs["build_envs"],
            [
                ("PIP_INDEX_URL", "https://test.pypi.org/simple/"),
                ("PIP_EXTRA_INDEX_URL", "https://pypi.org/simple/"),
            ],
        )

    def test_prepare_images_emits_inner_build_error_message(self):
        image = Image(name="parser-image")
        application = SimpleNamespace(
            _function_config=SimpleNamespace(image=image),
        )

        with (
            patch.object(
                deploy_module, "filter_applications", return_value=[application]
            ),
            patch.object(
                deploy_module, "functions_for_application", return_value=[application]
            ),
            patch.object(
                deploy_module,
                "build_sandbox_application_image",
                side_effect=deploy_module.SandboxImageBuildError(
                    "rootfs builder failed"
                ),
            ),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                asyncio.run(
                    deploy_module._prepare_images(
                        [application],
                        context_dir=os.getcwd(),
                    )
                )

        self.assertEqual(exc.exception.code, 1)
        emit.assert_any_call(
            {
                "type": "build_failed",
                "image": "parser-image",
                "error": (
                    "image 'parser-image' build failed: rootfs builder failed. "
                    "check your Image() configuration and try again."
                ),
            }
        )


if __name__ == "__main__":
    unittest.main()
