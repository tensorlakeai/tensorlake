import unittest
from typing import List

# Import from local package using absolute import path.
from hello_world.hello_world import hello_world
from hello_world.subpackage.subpackage import (
    TensorlakeComputeSubpackageHelloWorld,
    subpackage_hello_world,
    tensorlale_function_subpackage_hello_world,
)

# Import local module using absolute import path, works because the the code dir zip is added to PYTHONPATH.
from repeat import repeat_hello_world

# Check that symlinks inside the code dir works.
from testing_symlink import test_graph_name

from tensorlake import Graph, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path
from tensorlake.remote_graph import RemoteGraph

# Warning: this test file is not a part of Python package so relative import from it are not possible.
# Some subdirs in this code directory are Python packages (they have __init__.py file) and some are not.
# The Python modules (files) inside Python packages can do relative imports and we test it there.

# Absolute imports work in this file because the code directory is added to sys.path both when running
# the test file as a script using poetry and when running graph code zip in Function Executor.


@tensorlake_function()
def call_hello_world() -> str:
    return hello_world()


@tensorlake_function()
def call_repeat_hello_world(times: int) -> List[str]:
    return repeat_hello_world(times)


@tensorlake_function()
def call_hello_world_from_subpackage() -> str:
    return subpackage_hello_world()


@tensorlake_function()
def function_from_symlink_is_available() -> bool:
    return True if test_graph_name else False


@tensorlake_function()
def import_from_subdir_fails() -> bool:
    try:
        from hello_world.subdir.subdir import foo

        return False
    except ImportError:
        return True


class TestGraphCodeSerialization(unittest.TestCase):
    def test_call_hello_world(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=call_hello_world,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "call_hello_world")
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], hello_world())

    def test_call_repeat_hello_world(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=call_repeat_hello_world,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True, times=3)
        output = graph.output(invocation_id, "call_repeat_hello_world")
        self.assertEqual(output, repeat_hello_world(3))

    def test_call_hello_world_from_subpackage(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=call_hello_world_from_subpackage,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "call_hello_world_from_subpackage")
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], subpackage_hello_world())

    def test_function_from_symlink_is_available(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=function_from_symlink_is_available,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "function_from_symlink_is_available")
        self.assertEqual(len(output), 1)
        self.assertTrue(output[0])

    def test_import_from_subdir_fails(self):
        # Direct import from subdir works when this test file is directly executed by Python.
        # This is some known peculiar behavior of Python import system.
        # There's no clear explanation/reference on why it works this way.
        from hello_world.subdir.subdir import foo

        self.assertIsNotNone(foo)

        # However when this file is imported as a module, the direct import fails.
        # Test that this happens indeed.
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=import_from_subdir_fails,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, "import_from_subdir_fails")
        self.assertEqual(len(output), 1)
        self.assertTrue(output[0])

    def test_imported_tensorlake_function(self):
        # Check that the function imported from the module works.
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=tensorlale_function_subpackage_hello_world,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(
            invocation_id, tensorlale_function_subpackage_hello_world.name
        )
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], subpackage_hello_world())

    def test_imported_tensorlake_compute(self):
        # Check that the function imported from the module works.
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=TensorlakeComputeSubpackageHelloWorld,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        invocation_id = graph.run(block_until_done=True)
        output = graph.output(invocation_id, TensorlakeComputeSubpackageHelloWorld.name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], subpackage_hello_world())


# TODO: Add test case that validates that multiprocessing works.


if __name__ == "__main__":
    unittest.main()
