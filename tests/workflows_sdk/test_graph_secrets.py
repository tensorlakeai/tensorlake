import os
import unittest
from typing import List, Union

from testing import test_graph_name

from tensorlake import (
    Graph,
    RouteTo,
    tensorlake_function,
)


@tensorlake_function(secrets=["SECRET_NAME"])
def node_with_secret(x: int) -> int:
    return x + 1


@tensorlake_function()
def add_two(x: int) -> int:
    return x + 2


@tensorlake_function()
def add_three(x: int) -> int:
    return x + 3


@tensorlake_function(secrets=["SECRET_NAME_ROUTER"], next=[add_two, add_three])
def route_if_even(x: int) -> RouteTo[int, Union[add_two, add_three]]:
    if x % 2 == 0:
        return RouteTo(x, add_three)
    else:
        return RouteTo(x, add_two)


class TestGraphSecrets(unittest.TestCase):
    def test_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=node_with_secret
        )
        invocation_id = graph.run(block_until_done=True, request=1)
        output = graph.output(invocation_id, "node_with_secret")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], 2)

    def test_graph_router_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=route_if_even
        )
        invocation_id = graph.run(block_until_done=True, request=2)
        output = graph.output(invocation_id, "add_three")
        self.assertEqual(output, [5])


if __name__ == "__main__":
    unittest.main()
