import unittest
from typing import List, Union

from testing import test_graph_name

from tensorlake import (
    Graph,
    RouteTo,
    tensorlake_function,
)
from tensorlake.functions_sdk.graph_definition import (
    ComputeGraphMetadata,
    ResourceMetadata,
    RetryPolicyMetadata,
)
from tensorlake.functions_sdk.retries import Retries


class TestGraphMetadataFunctionTimeouts(unittest.TestCase):
    def test_function_timeouts(self):
        @tensorlake_function()
        def function_with_default_timeout(x: int) -> str:
            return "success"

        @tensorlake_function(timeout=10)
        def function_with_custom_timeout(x: int) -> str:
            return "success"

        @tensorlake_function(
            timeout=99,
            next=[function_with_custom_timeout, function_with_default_timeout],
        )
        def router_with_custom_timeout(
            x: int,
        ) -> RouteTo[
            int, Union[function_with_default_timeout, function_with_custom_timeout]
        ]:
            return RouteTo(x, function_with_default_timeout)

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=router_with_custom_timeout,
        )

        graph_metadata: ComputeGraphMetadata = graph.definition()
        self.assertEqual(
            graph_metadata.nodes["router_with_custom_timeout"].timeout_sec,
            99,
        )
        self.assertEqual(
            graph_metadata.nodes[
                "function_with_default_timeout"
            ].timeout_sec,
            300,
        )
        self.assertEqual(
            graph_metadata.nodes["function_with_custom_timeout"].timeout_sec,
            10,
        )


class TestGraphMetadataFunctionRetries(unittest.TestCase):
    def test_default_graph_retries(self):
        @tensorlake_function()
        def function_without_retries(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_without_retries,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        retry_policy_metadata: RetryPolicyMetadata = (
            graph_metadata.start_node.retry_policy
        )
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 0)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 1.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 60.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 2.0)

    def test_custom_graph_retries(self):
        # Tests that custom graph level retries get applied to functions that don't have their own retry policy.
        @tensorlake_function()
        def function_without_retries(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_without_retries,
            retries=Retries(
                max_retries=3,
                initial_delay=10.0,
                max_delay=120.0,
                delay_multiplier=10.0,
            ),
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        retry_policy_metadata: RetryPolicyMetadata = (
            graph_metadata.start_node.retry_policy
        )
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 3)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 10.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 120.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 10.0)

    def test_custom_function_retries(self):
        # Tests that custom graph level retries don't get applied to functions that have their own retry policy.
        @tensorlake_function(
            retries=Retries(
                max_retries=2,
                initial_delay=2.0,
                max_delay=10.0,
                delay_multiplier=5.0,
            )
        )
        def function_with_custom_retries(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_custom_retries,
            retries=Retries(
                max_retries=3,
                initial_delay=10.0,
                max_delay=120.0,
                delay_multiplier=10.0,
            ),
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        retry_policy_metadata: RetryPolicyMetadata = (
            graph_metadata.start_node.retry_policy
        )
        self.assertIsNotNone(retry_policy_metadata)
        self.assertEqual(retry_policy_metadata.max_retries, 2)
        self.assertEqual(retry_policy_metadata.initial_delay_sec, 2.0)
        self.assertEqual(retry_policy_metadata.max_delay_sec, 10.0)
        self.assertEqual(retry_policy_metadata.delay_multiplier, 5.0)


class TestGraphMetadataFunctionResources(unittest.TestCase):
    def test_default_function_resources(self):
        @tensorlake_function()
        def function_without_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_without_resources,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        resource_metadata: ResourceMetadata = (
            graph_metadata.start_node.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 1.0)
        self.assertEqual(resource_metadata.memory_mb, 1024)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 2 * 1024)
        self.assertEqual(resource_metadata.gpus, [])

    def test_custom_function_resources(self):
        @tensorlake_function(cpu=2.25, memory=2, ephemeral_disk=10, gpu="H100")
        def function_with_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_resources,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        resource_metadata: ResourceMetadata = (
            graph_metadata.start_node.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 2.25)
        self.assertEqual(resource_metadata.memory_mb, 2048)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 10 * 1024)
        self.assertEqual(len(resource_metadata.gpus), 1)
        self.assertEqual(resource_metadata.gpus[0].count, 1)
        self.assertEqual(resource_metadata.gpus[0].model, "H100")

    def test_custom_function_resources_many_gpus_per_model(self):
        @tensorlake_function(gpu="A100-40GB:4")
        def function_with_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_resources,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        resource_metadata: ResourceMetadata = (
            graph_metadata.start_node.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 1.0)
        self.assertEqual(resource_metadata.memory_mb, 1024)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 2 * 1024)
        self.assertEqual(len(resource_metadata.gpus), 1)
        self.assertEqual(resource_metadata.gpus[0].count, 4)
        self.assertEqual(resource_metadata.gpus[0].model, "A100-40GB")

    def test_custom_function_resources_many_gpu_models(self):
        @tensorlake_function(gpu=["A100-40GB:4", "H100:2", "A100-80GB:1"])
        def function_with_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_resources,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        resource_metadata: ResourceMetadata = (
            graph_metadata.start_node.resources
        )
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


if __name__ == "__main__":
    unittest.main()
