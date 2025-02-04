import unittest
from typing import List

import parameterized
import testing
from pydantic import BaseModel
from testing import remote_or_local_graph, test_graph_name

from tensorlake import Graph
from tensorlake.functions_sdk.functions import tensorlake_function


class AccumulatedState(BaseModel):
    sum: int = 0


@tensorlake_function()
def generate_seq(x: int) -> List[int]:
    return [i for i in range(x)]


@tensorlake_function()
def fail_generate_seq(x: int) -> List[int]:
    raise ValueError("test: fail_generate_seq function failed")


@tensorlake_function(accumulate=AccumulatedState)
def accumulate_reduce(acc: AccumulatedState, y: int) -> AccumulatedState:
    acc.sum += y
    return acc


@tensorlake_function()
def store_result(acc: AccumulatedState) -> int:
    return acc.sum


@tensorlake_function()
def add_one(x: int) -> int:
    return x + 1


@tensorlake_function()
def add_one_or_fail_at_0(x: int) -> int:
    if x == 0:
        raise ValueError("test: add_one_or_fail_at_0 function failed")
    return x + 1


class TestGraphReduce(unittest.TestCase):
    @parameterized.parameterized.expand([(True), (False)])
    def test_simple(self, is_remote: bool):
        graph = Graph(name=test_graph_name(self), start_node=generate_seq)
        graph.add_edge(generate_seq, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)

        graph = remote_or_local_graph(graph, remote=is_remote)
        invocation_id = graph.run(block_until_done=True, x=6)
        result = graph.output(invocation_id, store_result.name)
        self.assertEqual(result[0], 15)  # 0 + 1 + 2 + 3 + 4 + 5

    @parameterized.parameterized.expand([True])
    def test_failure_in_parent(self, is_remote: bool):
        # Not running this with local graph because local execution currently
        # raises an exception on function error and fails the test case.
        graph = Graph(
            name=test_graph_name(self),
            start_node=generate_seq,
        )
        graph.add_edge(generate_seq, add_one_or_fail_at_0)
        graph.add_edge(add_one_or_fail_at_0, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)
        graph = remote_or_local_graph(graph, remote=is_remote)

        invocation_id = graph.run(block_until_done=True, x=3)
        outputs = graph.output(invocation_id, store_result.name)
        self.assertEqual(len(outputs), 0, "Expected zero results")

    @parameterized.parameterized.expand([True])
    def test_failure_start_node(self, is_remote: bool):
        # Not running this with local graph because local execution currently
        # raises an exception on function error and fails the test case.

        graph = Graph(
            name=test_graph_name(self),
            start_node=fail_generate_seq,
        )
        graph.add_edge(fail_generate_seq, add_one)
        graph.add_edge(add_one, accumulate_reduce)
        graph.add_edge(accumulate_reduce, store_result)
        graph = remote_or_local_graph(
            graph, remote=is_remote, additional_modules=[testing, parameterized]
        )

        invocation_id = graph.run(block_until_done=True, x=3)
        outputs = graph.output(invocation_id, store_result.name)
        self.assertEqual(len(outputs), 0, "Expected zero results")


if __name__ == "__main__":
    unittest.main()
