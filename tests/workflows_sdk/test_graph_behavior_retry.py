import unittest

import parameterized
import testing
from pydantic import BaseModel
from testing import remote_or_local_graph, test_graph_name

from tensorlake import tensorlake_function
from tensorlake.functions_sdk.graph import Graph
from tensorlake.functions_sdk.retries import Retries


class MyObject(BaseModel):
    x: str


@tensorlake_function(retries=Retries(max_retries=4))
def simple_function(x: MyObject) -> MyObject:
    if x.x == "a":
        raise Exception("test exception")
    print("simple_function", x.x)
    return MyObject(x=x.x + "b")


class TestGraphBehaviorRetry(unittest.TestCase):
    def test_simple_function(self):
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=simple_function
        )
        graph = remote_or_local_graph(graph, remote=True)
        invocation_id = graph.run(block_until_done=True, x=MyObject(x="a"))
        output = graph.output(invocation_id, "simple_function")
        self.assertTrue(len(output) == 0)


if __name__ == "__main__":
    unittest.main()
