import unittest

from tensorlake.applications import Retries, application, cls, function
from tensorlake.applications.registry import get_functions
from tensorlake.applications.remote.manifests.application import (
    ApplicationManifest,
    create_application_manifest,
)
from tensorlake.applications.remote.manifests.function import (
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
    def test_default_application_level_retries(self):
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

    def test_custom_application_level_retries(self):
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


@function()
def function_with_default_container_limits(x: int) -> str:
    return "success"


@function(min_containers=2, max_containers=10)
def function_with_custom_container_limits(x: int) -> str:
    return "success"


@function(min_containers=1)
def function_with_only_min_containers(x: int) -> str:
    return "success"


@function(max_containers=5)
def function_with_only_max_containers(x: int) -> str:
    return "success"


class TestFunctionManifestContainerLimits(unittest.TestCase):
    def test_default_container_limits(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        self.assertIsNone(
            app_manifest.functions[
                "function_with_default_container_limits"
            ].min_containers,
        )
        self.assertIsNone(
            app_manifest.functions[
                "function_with_default_container_limits"
            ].max_containers,
        )

    def test_custom_container_limits(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        self.assertEqual(
            app_manifest.functions[
                "function_with_custom_container_limits"
            ].min_containers,
            2,
        )
        self.assertEqual(
            app_manifest.functions[
                "function_with_custom_container_limits"
            ].max_containers,
            10,
        )

    def test_only_min_containers(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        self.assertEqual(
            app_manifest.functions["function_with_only_min_containers"].min_containers,
            1,
        )
        self.assertIsNone(
            app_manifest.functions["function_with_only_min_containers"].max_containers,
        )

    def test_only_max_containers(self):
        app_manifest: ApplicationManifest = create_application_manifest(
            application_function=default_application_function,
            all_functions=get_functions(),
        )

        self.assertIsNone(
            app_manifest.functions["function_with_only_max_containers"].min_containers,
        )
        self.assertEqual(
            app_manifest.functions["function_with_only_max_containers"].max_containers,
            5,
        )


if __name__ == "__main__":
    unittest.main()
