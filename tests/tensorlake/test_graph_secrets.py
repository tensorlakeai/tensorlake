import os
import unittest
from typing import List, Union

import parameterized
from testing import remote_or_local_graph, test_graph_name

from tensorlake import (
    Graph,
    tensorlake_function,
)
from tensorlake.functions_sdk.functions import tensorlake_router


class TestGraphSecrets(unittest.TestCase):
    @parameterized.parameterized.expand([(False), (True)])
    @unittest.skipIf(
        os.environ.get("PLATFORM_EXECUTOR_TESTS") == "1",
        "Test skipped for platform executor",
    )
    def test_secrets_settable(self, is_remote):
        @tensorlake_function(secrets=["SECRET_NAME"])
        def node_with_secret(x: int) -> int:
            return x + 1

        graph = Graph(
            name=test_graph_name(self), description="test", start_node=node_with_secret
        )
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "node_with_secret")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], 2)

    @parameterized.parameterized.expand([(False), (True)])
    @unittest.skipIf(
        os.environ.get("PLATFORM_EXECUTOR_TESTS") == "1",
        "Test skipped for platform executor",
    )
    def test_graph_router_secrets_settable(self, is_remote):
        @tensorlake_function()
        def add_two(x: int) -> int:
            return x + 2

        @tensorlake_function()
        def add_three(x: int) -> int:
            return x + 3

        @tensorlake_router(secrets=["SECRET_NAME_ROUTER"])
        def route_if_even(x: int) -> List[Union[add_two, add_three]]:
            if x % 2 == 0:
                return add_three
            else:
                return add_two

        graph = Graph(
            name=test_graph_name(self), description="test", start_node=route_if_even
        )
        graph.route(route_if_even, [add_two, add_three])
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, x=2)
        output = graph.output(invocation_id, "add_three")
        self.assertEqual(output, [5])


if __name__ == "__main__":
    unittest.main()
