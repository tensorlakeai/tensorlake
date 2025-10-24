import unittest
from typing import List

from pydantic import BaseModel

from tensorlake.applications import Retries, application, cls, function
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.manifests.application import (
    ApplicationManifest,
    create_application_manifest,
)
from tensorlake.applications.remote.manifests.function import (
    ParameterManifest,
    RetryPolicyManifest,
)
from tensorlake.applications.remote.manifests.function_resources import (
    FunctionResourcesManifest,
)

# The tests in this file verify application manifest generation for various functions.


@application()
@function()
def default_application_function(x: int) -> str:
    return "success"


@function()
def function_with_default_timeout(x: int) -> str:
    return "success"


@function(timeout=10)
def function_with_custom_timeout(x: int) -> str:
    return "success"


@cls(init_timeout=20)
class FunctionWithInitializationTimeout:
    @function()
    def run(self, x: int) -> str:
        return "success"


class TestFunctionManifestTimeouts(unittest.TestCase):
    def test_expected_timeouts(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        self.assertEqual(
            app_manifest.functions["function_with_default_timeout"].timeout_sec,
            300,
        )
        self.assertEqual(
            app_manifest.functions[
                "function_with_default_timeout"
            ].initialization_timeout_sec,
            300,
        )
        self.assertEqual(
            app_manifest.functions["function_with_custom_timeout"].timeout_sec,
            10,
        )
        self.assertEqual(
            app_manifest.functions[
                "function_with_custom_timeout"
            ].initialization_timeout_sec,
            10,
        )
        self.assertEqual(
            app_manifest.functions["FunctionWithInitializationTimeout.run"].timeout_sec,
            300,
        )
        self.assertEqual(
            app_manifest.functions[
                "FunctionWithInitializationTimeout.run"
            ].initialization_timeout_sec,
            20,
        )


@function()
def function_without_retries(x: int) -> str:
    return "success"


@function(
    retries=Retries(
        max_retries=2,
    )
)
def function_with_custom_retries(x: int) -> str:
    return "success"


@application(
    retries=Retries(
        max_retries=3,
    )
)
@function()
def application_function_with_custom_retries(x: int) -> str:
    return "success"


class TestFunctionManifestRetries(unittest.TestCase):
    def test_default_graph_retries(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        retry_policy_metadata: RetryPolicyManifest = app_manifest.functions[
            "function_without_retries"
        ].retry_policy
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 0)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 1.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 60.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 2.0)

    def test_custom_application_retries(self):
        # Tests that custom application level retries get applied to functions that don't have their own retry policy.
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=application_function_with_custom_retries,
            all_functions=get_functions(),
        )

        retry_policy_metadata: RetryPolicyManifest = app_manifest.functions[
            "function_without_retries"
        ].retry_policy
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 3)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 1.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 60.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 2.0)

    def test_custom_function_retries(self):
        # Tests that custom app level retries don't get applied to functions that have their own retry policy.
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=application_function_with_custom_retries,
            all_functions=get_functions(),
        )

        retry_policy_metadata: RetryPolicyManifest = app_manifest.functions[
            "function_with_custom_retries"
        ].retry_policy
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 2)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 1.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 60.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 2.0)


@function()
def function_with_default_resources(x: int) -> str:
    return "success"


@function(cpu=2.25, memory=2, ephemeral_disk=10, gpu="H100")
def function_with_custom_resources(x: int) -> str:
    return "success"


@function(gpu="A100-40GB:4")
def function_with_many_gpus_per_model(x: int) -> str:
    return "success"


@function(gpu=["A100-40GB:4", "H100:2", "A100-80GB:1"])
def function_with_many_gpus_and_models(x: int) -> str:
    return "success"


class TestFunctionManifestResources(unittest.TestCase):
    def test_default_function_resources(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        resource_metadata: FunctionResourcesManifest = app_manifest.functions[
            "function_with_default_resources"
        ].resources
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 1.0)
        self.assertEqual(resource_metadata.memory_mb, 1024)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 2 * 1024)
        self.assertEqual(resource_metadata.gpus, [])

    def test_custom_function_resources(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        resource_metadata: FunctionResourcesManifest = app_manifest.functions[
            "function_with_custom_resources"
        ].resources
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 2.25)
        self.assertEqual(resource_metadata.memory_mb, 2048)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 10 * 1024)
        self.assertEqual(len(resource_metadata.gpus), 1)
        self.assertEqual(resource_metadata.gpus[0].count, 1)
        self.assertEqual(resource_metadata.gpus[0].model, "H100")

    def test_custom_function_resources_many_gpus_per_model(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        resource_metadata: FunctionResourcesManifest = app_manifest.functions[
            "function_with_many_gpus_per_model"
        ].resources
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 1.0)
        self.assertEqual(resource_metadata.memory_mb, 1024)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 2 * 1024)
        self.assertEqual(len(resource_metadata.gpus), 1)
        self.assertEqual(resource_metadata.gpus[0].count, 4)
        self.assertEqual(resource_metadata.gpus[0].model, "A100-40GB")

    def test_custom_function_resources_many_gpu_models(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        resource_metadata: FunctionResourcesManifest = app_manifest.functions[
            "function_with_many_gpus_and_models"
        ].resources
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 1.0)
        self.assertEqual(resource_metadata.memory_mb, 1024)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 2 * 1024)
        self.assertEqual(len(resource_metadata.gpus), 3)
        self.assertEqual(resource_metadata.gpus[0].count, 4)
        self.assertEqual(resource_metadata.gpus[0].model, "A100-40GB")
        self.assertEqual(resource_metadata.gpus[1].count, 2)
        self.assertEqual(resource_metadata.gpus[1].model, "H100")
        self.assertEqual(resource_metadata.gpus[2].count, 1)
        self.assertEqual(resource_metadata.gpus[2].model, "A100-80GB")


@function()
def function_with_basic_types(text: str, count: int, factor: float = 1.5) -> str:
    return f"Processed {count} items"


@function()
def function_with_list_return_type(count: int) -> List[int]:
    return list(range(count))


@function()
def function_with_complex_types(items: List[str], mapping: dict = None) -> str | int:
    return len(items)


class RequestPayload(BaseModel):
    name: str
    age: int
    email: str
    is_active: bool = True


@function()
def function_with_pydantic_model(payload: RequestPayload) -> str:
    return f"Processing {payload.name}"


class TestGraphMetadataParameterExtraction(unittest.TestCase):
    def test_parameter_extraction_basic_types(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )
        parameters: List[ParameterManifest] | None = app_manifest.functions[
            "function_with_basic_types"
        ].parameters

        self.assertIsNotNone(parameters)
        self.assertEqual(len(parameters), 3)

        # Check text parameter
        text_param = next(p for p in parameters if p.name == "text")
        self.assertEqual(text_param.data_type, {"type": "string"})
        self.assertTrue(text_param.required)

        # Check count parameter
        count_param = next(p for p in parameters if p.name == "count")
        self.assertEqual(count_param.data_type, {"type": "integer"})
        self.assertTrue(count_param.required)

        # Check factor parameter with default
        factor_param = next(p for p in parameters if p.name == "factor")
        self.assertEqual(factor_param.data_type, {"type": "number", "default": 1.5})
        self.assertFalse(factor_param.required)

    def test_parameter_extraction_return_type(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )
        self.assertEqual(
            app_manifest.functions["function_with_list_return_type"].return_type,
            {"type": "array", "items": {"type": "integer"}},
        )

    def test_parameter_extraction_complex_types(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )
        parameters: List[ParameterManifest] | None = app_manifest.functions[
            "function_with_complex_types"
        ].parameters

        self.assertIsNotNone(parameters)
        self.assertEqual(len(parameters), 2)

        # Check List[str] parameter
        items_param = next(p for p in parameters if p.name == "items")
        self.assertEqual(
            items_param.data_type, {"type": "array", "items": {"type": "string"}}
        )
        self.assertTrue(items_param.required)

        # Check dict parameter with default None
        mapping_param = next(p for p in parameters if p.name == "mapping")
        self.assertEqual(
            mapping_param.data_type,
            {"type": "object", "description": "dict object", "default": None},
        )
        self.assertFalse(mapping_param.required)

    def test_parameter_extraction_pydantic_model(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )
        parameters: List[ParameterManifest] | None = app_manifest.functions[
            "function_with_pydantic_model"
        ].parameters

        self.assertIsNotNone(parameters)
        self.assertEqual(len(parameters), 1)

        # Check Pydantic model parameter
        payload_param = next(p for p in parameters if p.name == "payload")

        # The parameter should contain the full Pydantic schema
        expected_schema = {
            "type": "object",
            "properties": {
                "name": {"type": "string", "title": "Name"},
                "age": {"type": "integer", "title": "Age"},
                "email": {"title": "Email", "type": "string"},
                "is_active": {"default": True, "title": "Is Active", "type": "boolean"},
            },
            "required": ["name", "age", "email"],
            "title": "RequestPayload",
        }

        self.assertEqual(payload_param.data_type, expected_schema)
        self.assertTrue(payload_param.required)


# This test is commented out because right now we don't provide max_concurrency feature in SDK interface.
# @function()
# def function_with_default_concurrency(x: int) -> str:
#     return "success"


# @function(max_concurrency=5)
# def function_with_custom_concurrency(x: int) -> str:
#     return "success"


# class TestGraphMetadataFunctionConcurrency(unittest.TestCase):
#     def test_function_with_default_concurrency(self):
#         app_manifest: ApplicationManifest = create_application_manifest(
#             application_function=default_application_function,
#             all_functions=get_functions(),
#         )
#         self.assertEqual(
#             app_manifest.functions["function_with_default_concurrency"].max_concurrency,
#             1,
#         )

#     def test_function_with_custom_concurrency(self):
#         app_manifest: ApplicationManifest = create_application_manifest(
#             application_function=default_application_function,
#             all_functions=get_functions(),
#         )
#         self.assertEqual(
#             app_manifest.functions["function_with_custom_concurrency"].max_concurrency,
#             5,
#         )


if __name__ == "__main__":
    unittest.main()
