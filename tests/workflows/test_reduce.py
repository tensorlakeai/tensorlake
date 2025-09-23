import unittest
from typing import List

import parameterized
from pydantic import BaseModel

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


class AccumulatedState(BaseModel):
    sum: int = 0


@tensorlake.api()
@tensorlake.function()
def success_api_function(x: int) -> AccumulatedState:
    seq = tensorlake.map(transform_int_to_accumulated_state, generate_seq(x))
    return tensorlake.reduce(accumulate_reduce, seq, AccumulatedState(sum=0))


# TODO: We need to allow a future as reducer input so tensorlake functions can generate sequences.
def generate_seq(x: int) -> List[int]:
    return [i for i in range(x)]


@tensorlake.function()
def transform_int_to_accumulated_state(x: int) -> AccumulatedState:
    return AccumulatedState(sum=x)


@tensorlake.function()
def accumulate_reduce(acc: AccumulatedState, y: AccumulatedState) -> AccumulatedState:
    acc.sum += y.sum
    return acc


@tensorlake.function()
def fail_generate_seq(x: int) -> List[int]:
    raise ValueError("test: fail_generate_seq function failed")


@tensorlake.function()
def store_result(acc: AccumulatedState) -> int:
    return acc.sum


@tensorlake.function()
def add_one(x: int) -> int:
    return x + 1


@tensorlake.function()
def add_one_or_fail_at_0(x: int) -> int:
    if x == 0:
        raise ValueError("test: add_one_or_fail_at_0 function failed")
    return x + 1


class TestGraphReduce(unittest.TestCase):
    @parameterized.parameterized.expand([(True), (False)])
    def test_simple(self, is_remote: bool):
        if is_remote:
            deploy(__file__)

        request: tensorlake.Request = tensorlake.call_api(
            success_api_function, 6, remote=is_remote
        )
        result: AccumulatedState = request.output()
        self.assertEqual(result, AccumulatedState(sum=15))  # 0 + 1 + 2 + 3 + 4 + 5

    def test_single_item_reduce(self):
        deploy(__file__)

        # TODO:

    # @parameterized.parameterized.expand([True])
    # def test_failure_in_parent(self, is_remote: bool):
    #     # Not running this with local graph because local execution currently
    #     # raises an exception on function error and fails the test case.
    #     graph = Graph(
    #         name=test_graph_name(self),
    #         start_node=generate_seq,
    #     )
    #     graph.add_edge(generate_seq, add_one_or_fail_at_0)
    #     graph.add_edge(add_one_or_fail_at_0, accumulate_reduce)
    #     graph.add_edge(accumulate_reduce, store_result)
    #     graph = remote_or_local_graph(graph, remote=is_remote)

    #     invocation_id = graph.run(block_until_done=True, x=3)
    #     outputs = graph.output(invocation_id, store_result.name)
    #     self.assertEqual(len(outputs), 0, "Expected zero results")

    # @parameterized.parameterized.expand([True])
    # def test_failure_start_node(self, is_remote: bool):
    #     # Not running this with local graph because local execution currently
    #     # raises an exception on function error and fails the test case.

    #     graph = Graph(
    #         name=test_graph_name(self),
    #         start_node=fail_generate_seq,
    #     )
    #     graph.add_edge(fail_generate_seq, add_one)
    #     graph.add_edge(add_one, accumulate_reduce)
    #     graph.add_edge(accumulate_reduce, store_result)
    #     graph = remote_or_local_graph(graph, remote=is_remote)

    #     invocation_id = graph.run(block_until_done=True, x=3)
    #     outputs = graph.output(invocation_id, store_result.name)
    #     self.assertEqual(len(outputs), 0, "Expected zero results")


if __name__ == "__main__":
    unittest.main()
