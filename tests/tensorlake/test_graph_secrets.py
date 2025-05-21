import os
import unittest
from typing import List, Union

from testing import test_graph_name

from tensorlake import (
    Graph,
    tensorlake_function,
)
from tensorlake.functions_sdk.functions import tensorlake_router


@tensorlake_function(secrets=["SECRET_NAME"])
def node_with_secret(x: int) -> int:
    return x + 1


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


class TestGraphSecrets(unittest.TestCase):
    def test_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=node_with_secret
        )
        invocation_id = graph.run(block_until_done=True, x=1)
        output = graph.output(invocation_id, "node_with_secret")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], 2)

    def test_graph_router_secrets_settable(self):
        # Only test local graph mode here because behavior of secrets in remote graph depends
        # on Executor flavor.
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=route_if_even
        )
        graph.route(route_if_even, [add_two, add_three])
        invocation_id = graph.run(block_until_done=True, x=2)
        output = graph.output(invocation_id, "add_three")
        self.assertEqual(output, [5])


if __name__ == "__main__":
    unittest.main()
