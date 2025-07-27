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
    ParameterMetadata,
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
            graph_metadata.functions["router_with_custom_timeout"].timeout_sec,
            99,
        )
        self.assertEqual(
            graph_metadata.functions["function_with_default_timeout"].timeout_sec,
            300,
        )
        self.assertEqual(
            graph_metadata.functions["function_with_custom_timeout"].timeout_sec,
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
            graph_metadata.entrypoint.retry_policy
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
            graph_metadata.entrypoint.retry_policy
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
            graph_metadata.entrypoint.retry_policy
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
        resource_metadata: ResourceMetadata = graph_metadata.entrypoint.resources
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
        resource_metadata: ResourceMetadata = graph_metadata.entrypoint.resources
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
        resource_metadata: ResourceMetadata = graph_metadata.entrypoint.resources
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
        resource_metadata: ResourceMetadata = graph_metadata.entrypoint.resources
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


class TestGraphMetadataParameterExtraction(unittest.TestCase):
    def test_parameter_extraction_basic_types(self):
        @tensorlake_function()
        def process_data(text: str, count: int, factor: float = 1.5) -> str:
            return f"Processed {count} items"

        graph = Graph(
            name=test_graph_name(self),
            description="test parameter extraction",
            start_node=process_data,
        )

        graph_metadata: ComputeGraphMetadata = graph.definition()
        parameters = graph_metadata.entrypoint.parameters

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
        @tensorlake_function()
        def get_numbers(count: int) -> List[int]:
            return list(range(count))

        graph = Graph(
            name=test_graph_name(self),
            description="test return type extraction",
            start_node=get_numbers,
        )

        graph_metadata: ComputeGraphMetadata = graph.definition()
        self.assertEqual(
            graph_metadata.entrypoint.return_type,
            {"type": "array", "items": {"type": "integer"}},
        )

    def test_parameter_extraction_complex_types(self):
        @tensorlake_function()
        def process_items(items: List[str], mapping: dict = None) -> Union[str, int]:
            return len(items)

        graph = Graph(
            name=test_graph_name(self),
            description="test complex types",
            start_node=process_items,
        )

        graph_metadata: ComputeGraphMetadata = graph.definition()
        parameters = graph_metadata.entrypoint.parameters

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


if __name__ == "__main__":
    unittest.main()
