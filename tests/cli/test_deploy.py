import asyncio
import hashlib
import json
import os
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from tensorlake import builder as builder_module
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


def fake_create_image_context_file(image, file_path):
    with open(file_path, "wb") as handle:
        handle.write(f"context:{image._id}".encode())


def expected_context_sha(image):
    return hashlib.sha256(f"context:{image._id}".encode()).hexdigest()


class FakeV3CloudClient:
    def __init__(self):
        self.calls = []
        self.application_build_info_calls = []
        self.stream_calls = []
        self.create_responses = {}
        self._responses = {}

    def create_application_build(
        self,
        build_service_path: str,
        request_json: str,
        image_contexts: list[tuple[str, bytes]],
    ) -> str:
        request = json.loads(request_json)
        self.calls.append((build_service_path, request_json, image_contexts))
        app_build_id = f"app-build-{len(self.calls)}"
        image_builds = [
            {
                "id": f"img-build-{app_build_id}-{image['key']}",
                "app_version_id": app_build_id,
                "key": image["key"],
                "name": image.get("name"),
                "context_sha256": image["context_sha256"],
                "status": "pending",
                "created_at": "2026-03-10T10:00:00Z",
                "updated_at": "2026-03-10T10:00:00Z",
                "function_names": image["function_names"],
            }
            for image in request["images"]
        ]
        create_response = {
            "id": app_build_id,
            "organization_id": "org-1",
            "project_id": "proj-1",
            "name": request["name"],
            "version": request["version"],
            "status": "building",
            "image_builds": image_builds,
        }
        self.create_responses[app_build_id] = create_response
        self._responses[app_build_id] = {
            "id": app_build_id,
            "organization_id": "org-1",
            "project_id": "proj-1",
            "name": request["name"],
            "version": request["version"],
            "status": "succeeded",
            "created_at": "2026-03-10T10:00:00Z",
            "updated_at": "2026-03-10T10:01:00Z",
            "finished_at": "2026-03-10T10:01:00Z",
            "image_builds": [
                {
                    **image_build,
                    "status": "succeeded",
                    "image_uri": f"registry.example.com/{request['name']}/{image_build['key']}:latest",
                    "image_digest": f"sha256:{image_build['key']}",
                    "finished_at": "2026-03-10T10:01:00Z",
                }
                for image_build in image_builds
            ],
        }
        return json.dumps(create_response)

    def application_build_info_json(
        self,
        build_service_path: str,
        application_build_id: str,
    ) -> str:
        self.application_build_info_calls.append(
            (build_service_path, application_build_id)
        )
        return json.dumps(self._responses[application_build_id])

    def stream_build_logs_to_stderr_prefixed(
        self,
        build_service_path: str,
        build_id: str,
        prefix: str,
        color: str | None = None,
    ) -> None:
        self.stream_calls.append((build_service_path, build_id, prefix, color))
        return None


class TestDeployHelpers(unittest.TestCase):
    def test_collect_application_build_request_groups_functions_using_default_image(
        self,
    ):
        with isolated_registry():
            custom_image = Image(name="custom-image")

            @function()
            def default_helper(payload):
                return payload

            @function(image=custom_image)
            def custom_helper(payload):
                return payload

            @application()
            @function()
            def default_image_app(payload):
                payload = default_helper(payload)
                return custom_helper(payload)

            all_functions = [default_helper, custom_helper, default_image_app]

            def fake_build_image_context(image):
                return f"context:{image._id}".encode()

            with patch.object(
                builder_module,
                "build_image_context",
                side_effect=fake_build_image_context,
            ):
                request = builder_module.collect_application_build_request(
                    default_image_app, all_functions
                )

        images = {image.key: image for image in request.images}
        self.assertEqual(
            set(images), {default_helper._function_config.image._id, custom_image._id}
        )

        default_image_request = images[default_helper._function_config.image._id]
        self.assertEqual(default_image_request.name, "default")
        self.assertEqual(
            set(default_image_request.function_names),
            {
                default_helper._function_config.function_name,
                default_image_app._function_config.function_name,
            },
        )

        custom_image_request = images[custom_image._id]
        self.assertEqual(custom_image_request.name, "custom-image")
        self.assertEqual(
            custom_image_request.function_names,
            [custom_helper._function_config.function_name],
        )

    def test_format_error_message_does_not_include_exception_payload(self):
        message = deploy_module._format_error_message(
            "build failed", RuntimeError("secret")
        )
        self.assertEqual(message, "build failed (RuntimeError)")
        self.assertNotIn("secret", message)

    def test_format_build_failure_message_includes_inner_error(self):
        message = deploy_module._format_build_failure_message(
            "parser-image",
            RuntimeError("404 Not Found: POST /images/v3/applications"),
        )
        self.assertEqual(
            message,
            "image 'parser-image' build failed: 404 Not Found: POST /images/v3/applications. check your Image() configuration and try again.",
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
    def _successful_deploy_patches(self, auth, functions):
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
        ):
            yield

    def test_deploy_emits_user_friendly_import_error(self):
        with (
            patch.object(
                deploy_module,
                "_build_context_from_env",
                return_value=self._make_auth_context(),
            ),
            patch.object(deploy_module, "mk_builder", return_value=MagicMock()),
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
            patch.object(deploy_module, "mk_builder", return_value=MagicMock()),
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
            patch.object(
                deploy_module, "mk_builder", return_value=MagicMock()
            ) as mk_builder,
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

        prepare_images.assert_awaited_once()
        mk_builder.assert_called_once()
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

    def test_deploy_v3_path_builds_all_functions_for_each_application(self):
        cloud_client = FakeV3CloudClient()
        auth = SimpleNamespace(
            api_url="https://api.tensorlake.ai",
            api_key="api-key",
            personal_access_token=None,
            organization_id="org-1",
            project_id="proj-1",
            cloud_client=cloud_client,
        )
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

            default_image = get_extraction_schema._function_config.image

            with (
                self._successful_deploy_patches(auth, functions),
                patch.object(
                    builder_module,
                    "create_image_context_file",
                    side_effect=fake_create_image_context_file,
                ),
                patch.object(deploy_module, "_deploy_applications"),
                patch.object(deploy_module, "_emit"),
            ):
                deploy_module.deploy(
                    application_file_path="my_app.py",
                    upgrade_running_requests=False,
                    image_builder_version="v3",
                )

        self.assertEqual(len(cloud_client.calls), 2)

        analyzer_build_service_path, analyzer_request_json, analyzer_image_contexts = (
            cloud_client.calls[0]
        )
        self.assertEqual(analyzer_build_service_path, "/images/v3/applications")

        analyzer_request = json.loads(analyzer_request_json)
        self.assertEqual(
            analyzer_request["name"], finance_analyzer._function_config.function_name
        )
        analyzer_images = {
            image["context_tar_part_name"]: image
            for image in analyzer_request["images"]
        }
        self.assertEqual(
            set(analyzer_image_contexts),
            {
                (default_image._id, f"context:{default_image._id}".encode()),
                (parser_image._id, f"context:{parser_image._id}".encode()),
                (agent_image._id, f"context:{agent_image._id}".encode()),
                (code_exec_image._id, f"context:{code_exec_image._id}".encode()),
            },
        )
        self.assertEqual(
            set(analyzer_images),
            {
                default_image._id,
                parser_image._id,
                agent_image._id,
                code_exec_image._id,
            },
        )
        self.assertEqual(
            {
                image_key: image["context_sha256"]
                for image_key, image in analyzer_images.items()
            },
            {
                default_image._id: expected_context_sha(default_image),
                parser_image._id: expected_context_sha(parser_image),
                agent_image._id: expected_context_sha(agent_image),
                code_exec_image._id: expected_context_sha(code_exec_image),
            },
        )
        self.assertEqual(
            set(analyzer_images[default_image._id]["function_names"]),
            {
                get_extraction_schema._function_config.function_name,
                get_document_content._function_config.function_name,
                finance_analyzer._function_config.function_name,
            },
        )
        self.assertEqual(
            analyzer_images[parser_image._id]["function_names"],
            [upload_and_parse_document._function_config.function_name],
        )
        self.assertEqual(
            set(analyzer_images[agent_image._id]["function_names"]),
            {
                run_finance_agent._function_config.function_name,
                execute_sql_query._function_config.function_name,
                run_query_agent._function_config.function_name,
                finance_query._function_config.function_name,
            },
        )
        self.assertEqual(
            analyzer_images[code_exec_image._id]["function_names"],
            [execute_code._function_config.function_name],
        )

        query_request = json.loads(cloud_client.calls[1][1])
        query_images = {
            image["context_tar_part_name"]: image for image in query_request["images"]
        }
        self.assertEqual(
            set(query_images),
            {
                default_image._id,
                parser_image._id,
                agent_image._id,
                code_exec_image._id,
            },
        )
        self.assertEqual(
            {
                image_key: image["context_sha256"]
                for image_key, image in query_images.items()
            },
            {
                default_image._id: expected_context_sha(default_image),
                parser_image._id: expected_context_sha(parser_image),
                agent_image._id: expected_context_sha(agent_image),
                code_exec_image._id: expected_context_sha(code_exec_image),
            },
        )
        self.assertEqual(
            set(query_images[default_image._id]["function_names"]),
            {
                get_extraction_schema._function_config.function_name,
                get_document_content._function_config.function_name,
                finance_analyzer._function_config.function_name,
            },
        )
        self.assertEqual(
            query_images[parser_image._id]["function_names"],
            [upload_and_parse_document._function_config.function_name],
        )
        self.assertEqual(
            set(query_images[agent_image._id]["function_names"]),
            {
                run_finance_agent._function_config.function_name,
                execute_sql_query._function_config.function_name,
                run_query_agent._function_config.function_name,
                finance_query._function_config.function_name,
            },
        )
        self.assertEqual(
            query_images[code_exec_image._id]["function_names"],
            [execute_code._function_config.function_name],
        )
        self.assertEqual(
            cloud_client.create_responses["app-build-1"]["status"], "building"
        )
        self.assertEqual(
            cloud_client.create_responses["app-build-2"]["status"], "building"
        )
        self.assertEqual(
            len(cloud_client.create_responses["app-build-1"]["image_builds"]), 4
        )
        self.assertEqual(
            len(cloud_client.create_responses["app-build-2"]["image_builds"]), 4
        )
        self.assertTrue(
            all(
                image["status"] == "pending"
                for image in cloud_client.create_responses["app-build-1"][
                    "image_builds"
                ]
            )
        )
        self.assertTrue(
            all(
                image["status"] == "pending"
                for image in cloud_client.create_responses["app-build-2"][
                    "image_builds"
                ]
            )
        )
        self.assertEqual(
            cloud_client.application_build_info_calls,
            [
                ("/images/v3/applications", "app-build-1"),
                ("/images/v3/applications", "app-build-2"),
            ],
        )
        self.assertEqual(
            {
                (
                    build_service_path,
                    build_id,
                    prefix,
                )
                for build_service_path, build_id, prefix, _color in cloud_client.stream_calls
            },
            {
                (
                    "/images/v3",
                    f"img-build-app-build-1-{default_image._id}",
                    f"{finance_analyzer._function_config.function_name}/default",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-1-{parser_image._id}",
                    f"{finance_analyzer._function_config.function_name}/parser-image",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-1-{agent_image._id}",
                    f"{finance_analyzer._function_config.function_name}/agent-image",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-1-{code_exec_image._id}",
                    f"{finance_analyzer._function_config.function_name}/code-exec-image",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-2-{default_image._id}",
                    f"{finance_query._function_config.function_name}/default",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-2-{parser_image._id}",
                    f"{finance_query._function_config.function_name}/parser-image",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-2-{agent_image._id}",
                    f"{finance_query._function_config.function_name}/agent-image",
                ),
                (
                    "/images/v3",
                    f"img-build-app-build-2-{code_exec_image._id}",
                    f"{finance_query._function_config.function_name}/code-exec-image",
                ),
            },
        )

    def test_deploy_v2_path_builds_images_and_then_deploys(self):
        class FakeCloudClient:
            def __init__(self):
                self.start_calls = []
                self.stream_calls = []
                self.info_calls = []

            def start_image_build(
                self,
                build_service_path: str,
                application_name: str,
                application_version: str,
                function_name: str,
                image_name: str,
                image_key: str,
                context_tar_gz: bytes,
            ) -> str:
                self.start_calls.append(
                    {
                        "build_service_path": build_service_path,
                        "application_name": application_name,
                        "application_version": application_version,
                        "function_name": function_name,
                        "image_name": image_name,
                        "image_key": image_key,
                        "context_tar_gz": context_tar_gz,
                    }
                )
                return json.dumps(
                    {
                        "id": f"build-{function_name}",
                        "status": "building",
                        "created_at": "2026-03-10T10:00:00Z",
                        "updated_at": "2026-03-10T10:00:00Z",
                        "finished_at": None,
                    }
                )

            def stream_build_logs_to_stderr(
                self,
                build_service_path: str,
                build_id: str,
            ) -> None:
                self.stream_calls.append((build_service_path, build_id))

            def build_info_json(
                self,
                build_service_path: str,
                build_id: str,
            ) -> str:
                self.info_calls.append((build_service_path, build_id))
                return json.dumps(
                    {
                        "id": build_id,
                        "status": "succeeded",
                        "created_at": "2026-03-10T10:00:00Z",
                        "updated_at": "2026-03-10T10:01:00Z",
                        "finished_at": "2026-03-10T10:01:00Z",
                    }
                )

        cloud_client = FakeCloudClient()
        auth = SimpleNamespace(
            api_url="https://api.tensorlake.ai",
            api_key="api-key",
            personal_access_token=None,
            organization_id="org-1",
            project_id="proj-1",
            cloud_client=cloud_client,
        )

        shared_image = MagicMock()
        shared_image._id = "img-shared"
        shared_image.name = "shared-image"

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

        def fake_build_image_context(image):
            return f"context:{image._id}".encode()

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
                builder_module,
                "build_image_context",
                side_effect=fake_build_image_context,
            ),
            patch.object(deploy_module, "_deploy_applications") as deploy_apps,
            patch.object(deploy_module, "_emit") as emit,
        ):
            deploy_module.deploy(
                application_file_path="my_app.py",
                upgrade_running_requests=False,
                image_builder_version="v2",
            )

        self.assertEqual(
            [call["build_service_path"] for call in cloud_client.start_calls],
            ["/images/v2", "/images/v2"],
        )
        self.assertEqual(
            [call["function_name"] for call in cloud_client.start_calls],
            ["app-one", "helper-one"],
        )
        self.assertEqual(
            [call["context_tar_gz"] for call in cloud_client.start_calls],
            [b"context:img-shared", b"context:img-shared"],
        )
        self.assertEqual(
            cloud_client.stream_calls,
            [("/images/v2", "build-app-one"), ("/images/v2", "build-helper-one")],
        )
        self.assertEqual(
            cloud_client.info_calls,
            [("/images/v2", "build-app-one"), ("/images/v2", "build-helper-one")],
        )
        build_start_events = [
            call.args[0]
            for call in emit.call_args_list
            if call.args and call.args[0]["type"] == "build_start"
        ]
        self.assertEqual(
            build_start_events,
            [
                {"type": "build_start", "image": "shared-image"},
                {"type": "build_start", "image": "shared-image"},
            ],
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

    def test_deploy_entrypoint_passes_image_builder_version(self):
        with (
            patch(
                "sys.argv",
                [
                    "tensorlake-deploy",
                    "my_app.py",
                    "--image-builder-version",
                    "v3",
                ],
            ),
            patch.object(deploy_module, "deploy") as deploy,
        ):
            deploy_module.deploy_entrypoint()

        self.assertEqual(deploy.call_args.kwargs["image_builder_version"], "v3")

    def test_prepare_images_emits_inner_build_error_message(self):
        builder = MagicMock()
        builder.build = AsyncMock(
            side_effect=deploy_module.ApplicationImageBuildError(
                image_name="parser-image",
                error=RuntimeError("404 Not Found: POST /images/v3/applications"),
            )
        )

        with (
            patch.object(deploy_module, "filter_applications", return_value=[object()]),
            patch.object(
                deploy_module,
                "collect_application_build_request",
                return_value=MagicMock(),
            ),
            patch.object(deploy_module, "_emit") as emit,
        ):
            with self.assertRaises(SystemExit) as exc:
                asyncio.run(deploy_module._prepare_images(builder, ["fn"]))

        self.assertEqual(exc.exception.code, 1)
        emit.assert_called_once_with(
            {
                "type": "build_failed",
                "image": "parser-image",
                "error": "image 'parser-image' build failed: 404 Not Found: POST /images/v3/applications. check your Image() configuration and try again.",
            }
        )


if __name__ == "__main__":
    unittest.main()
