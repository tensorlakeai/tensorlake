import os
import unittest
from typing import Optional

from testing import test_graph_name

from tensorlake import Graph, RemoteGraph, TensorlakeCompute, tensorlake_function
from tensorlake.functions_sdk.graph_serialization import graph_code_dir_path

# The tests in this file verify publicly stated performance critical behaviors of Tensorlake RemoteGraphs.

cached_pipe_in_fd: Optional[int] = None
cached_pipe_out_fd: Optional[int] = None


@tensorlake_function()
def fd_caching_function(action: str) -> str:
    global cached_pipe_in_fd
    global cached_pipe_out_fd

    if action == "create_fd":
        cached_pipe_in_fd, cached_pipe_out_fd = os.pipe()
    elif action == "write_fd":
        if cached_pipe_out_fd is None:
            raise ValueError("cached_pipe_out_fd is None")
        os.write(cached_pipe_out_fd, "write_to_cacheable_fd\n".encode())
    elif action == "read_fd":
        if cached_pipe_in_fd is None:
            raise ValueError("cached_pipe_in_fd is None")
        return os.read(cached_pipe_in_fd, 1024).decode()
    else:
        raise ValueError("Invalid action")

    return "success"


class TestFileDescriptorCaching(unittest.TestCase):
    def test_second_write_goes_to_cached_file_descriptor_if_same_func(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=fd_caching_function,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        create_fd_invocation_id = graph.run(block_until_done=True, request="create_fd")
        output = graph.output(create_fd_invocation_id, "fd_caching_function")
        self.assertEqual(output, ["success"])

        # Fails if the file descriptor is not cached between different invocations of
        # the same function version. File descriptor caching is required to e.g. not
        # load a model into GPU on each invocation.
        write_fd_invocation_id = graph.run(block_until_done=True, request="write_fd")
        output = graph.output(write_fd_invocation_id, "fd_caching_function")
        self.assertEqual(output, ["success"])

        # Fail if the write to the cached file descriptor didn's happen for any reason.
        # This verifies that the file descriptor state is not altered between invocations
        # of the same function version.
        read_file_invocation_id = graph.run(block_until_done=True, request="read_fd")
        output = graph.output(read_file_invocation_id, "fd_caching_function")
        self.assertEqual(output, ["write_to_cacheable_fd\n"])


class TensorlakeComputeTestObject(TensorlakeCompute):
    name = "TensorlakeComputeTestObject"

    def __init__(self):
        super().__init__()
        global tensorlake_compute_object_constructor_calls
        tensorlake_compute_object_constructor_calls += 1

        global cached_tensorlake_compute_object
        cached_tensorlake_compute_object = self

    def run(self, action: str) -> str:
        global tensorlake_compute_object_constructor_calls
        global cached_tensorlake_compute_object

        if action == "check_constructor_called_once":
            if tensorlake_compute_object_constructor_calls != 1:
                raise ValueError(
                    f"TensorlakeComputeTestObject constructor called {tensorlake_compute_object_constructor_calls} times"
                )
        elif action == "check_cached_object":
            if cached_tensorlake_compute_object is None:
                raise ValueError("cached_tensorlake_compute_object is None")
            if cached_tensorlake_compute_object is not self:
                raise ValueError(
                    "cached_tensorlake_compute_object is not the currently running object"
                )
        else:
            raise ValueError("Invalid action")

        return "success"


tensorlake_compute_object_constructor_calls: int = 0
cached_tensorlake_compute_object: Optional[TensorlakeComputeTestObject] = None


class TestTensorlakeComputeObjectCaching(unittest.TestCase):
    def test_compute_object_caching(self):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=TensorlakeComputeTestObject,
        )
        graph = RemoteGraph.deploy(
            graph=graph, code_dir_path=graph_code_dir_path(__file__)
        )

        # Run many times to ensure that the behavior is repeatable over many invocations.
        for i in range(5):
            # The object is created once and cached in memory.
            invocation_id = graph.run(
                block_until_done=True, request="check_constructor_called_once"
            )
            output = graph.output(invocation_id, TensorlakeComputeTestObject.name)
            self.assertEqual(output, ["success"])

            # Every new invocation will reuse the cached compute object.
            invocation_id = graph.run(
                block_until_done=True, request="check_cached_object"
            )
            output = graph.output(invocation_id, TensorlakeComputeTestObject.name)
            self.assertEqual(output, ["success"])


if __name__ == "__main__":
    unittest.main()
