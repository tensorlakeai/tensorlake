import os
import unittest
from typing import Optional

from testing import test_graph_name

from tensorlake import Graph, tensorlake_function
from tensorlake.remote_graph import RemoteGraph

cached_pipe_in_fd: Optional[int] = None
cached_pipe_out_fd: Optional[int] = None


@tensorlake_function()
def caching_function(action: str) -> str:
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
            start_node=caching_function,
        )
        graph = RemoteGraph.deploy(graph)

        create_fd_invocation_id = graph.run(block_until_done=True, action="create_fd")
        output = graph.output(create_fd_invocation_id, "caching_function")
        self.assertEqual(output, ["success"])

        # Fails if the file descriptor is not cached.
        write_fd_invocation_id = graph.run(block_until_done=True, action="write_fd")
        output = graph.output(write_fd_invocation_id, "caching_function")
        self.assertEqual(output, ["success"])

        # Fail if the write to the cached file descriptor didn's happen for any reason.
        read_file_invocation_id = graph.run(block_until_done=True, action="read_fd")
        output = graph.output(read_file_invocation_id, "caching_function")
        self.assertEqual(output, ["write_to_cacheable_fd\n"])


if __name__ == "__main__":
    unittest.main()
