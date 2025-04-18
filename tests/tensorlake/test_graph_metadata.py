import unittest
from typing import List, Union

from testing import test_graph_name

from tensorlake import (
    Graph,
    tensorlake_function,
    tensorlake_router,
)
from tensorlake.functions_sdk.graph_definition import (
    ComputeGraphMetadata,
    ResourceMetadata,
    RetryPolicyMetadata,
)
from tensorlake.functions_sdk.resources import GPU_MODEL, GPUResourceMetadata
from tensorlake.functions_sdk.retries import Retries


class TestGraphMetadataFunctionTimeouts(unittest.TestCase):
    def test_function_timeouts(self):
        @tensorlake_function()
        def function_with_default_timeout(x: int) -> str:
            return "success"

        @tensorlake_function(timeout=10)
        def function_with_custom_timeout(x: int) -> str:
            return "success"

        @tensorlake_router(timeout=99)
        def router_with_custom_timeout(
            x: int,
        ) -> List[Union[function_with_default_timeout, function_with_custom_timeout]]:
            return function_with_default_timeout

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=router_with_custom_timeout,
        )
        graph.route(
            router_with_custom_timeout,
            [function_with_default_timeout, function_with_custom_timeout],
        )

        graph_metadata: ComputeGraphMetadata = graph.definition()
        self.assertEqual(
            graph_metadata.nodes[
                "router_with_custom_timeout"
            ].dynamic_router.timeout_sec,
            99,
        )
        self.assertEqual(
            graph_metadata.nodes[
                "function_with_default_timeout"
            ].compute_fn.timeout_sec,
            300,
        )
        self.assertEqual(
            graph_metadata.nodes["function_with_custom_timeout"].compute_fn.timeout_sec,
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
            graph_metadata.start_node.compute_fn.retry_policy
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
            graph_metadata.start_node.compute_fn.retry_policy
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
            graph_metadata.start_node.compute_fn.retry_policy
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
            graph_metadata.start_node.compute_fn.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 0.125)
        self.assertEqual(resource_metadata.memory_mb, 128)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 100 * 1024)
        self.assertIsNone(resource_metadata.gpu)

    def test_custom_function_resources(self):
        @tensorlake_function(cpu=2.25, memory=2, ephemeral_disk=10, gpu=GPU_MODEL.H100)
        def function_with_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_resources,
        )
        graph_metadata: ComputeGraphMetadata = graph.definition()
        resource_metadata: ResourceMetadata = (
            graph_metadata.start_node.compute_fn.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 2.25)
        self.assertEqual(resource_metadata.memory_mb, 2048)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 10 * 1024)
        self.assertIsNotNone(resource_metadata.gpu)
        gpu_metadata: GPUResourceMetadata = resource_metadata.gpu
        self.assertEqual(gpu_metadata.count, 1)
        self.assertEqual(gpu_metadata.model, GPU_MODEL.H100)

    def test_custom_function_resources_many_gpus(self):
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
            graph_metadata.start_node.compute_fn.resources
        )
        self.assertIsNotNone(resource_metadata)
        self.assertEqual(resource_metadata.cpus, 0.125)
        self.assertEqual(resource_metadata.memory_mb, 128)
        self.assertEqual(resource_metadata.ephemeral_disk_mb, 100 * 1024)
        self.assertIsNotNone(resource_metadata.gpu)
        gpu_metadata: GPUResourceMetadata = resource_metadata.gpu
        self.assertEqual(gpu_metadata.count, 4)
        self.assertEqual(gpu_metadata.model, GPU_MODEL.A100_40GB)

    def test_custom_function_resources_not_supported_gpu_model(self):
        @tensorlake_function(gpu="NOT_SUPPORTED:4")
        def function_with_resources(x: int) -> str:
            return "success"

        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_with_resources,
        )
        try:
            graph.definition()
            self.fail("Expected ValueError not raised")
        except ValueError as e:
            self.assertIn("Unsupported GPU model", str(e))


if __name__ == "__main__":
    unittest.main()
