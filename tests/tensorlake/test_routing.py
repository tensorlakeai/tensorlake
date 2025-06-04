import unittest
from typing import List, Union

import parameterized
from testing import remote_or_local_graph, test_graph_name

from tensorlake import Graph, RouteTo, tensorlake_function


@tensorlake_function()
def a(value: int) -> int:
    return value + 3


@tensorlake_function()
def b(value: int) -> int:
    return value + 4


@tensorlake_function()
def c(value: int) -> int:
    return value + 5


@tensorlake_function(next=[a, b])
def fan_out(value: int) -> int:
    return value


@tensorlake_function(next=[a, b, c])
def route_out(value: int) -> RouteTo[int, Union[a, b, c]]:
    if value % 2 == 0:
        return RouteTo(value + 1, [a])

    return RouteTo(value + 2, [b, c])


@tensorlake_function(accumulate=int)
def sum_of_squares(current: int, value: int) -> int:
    return current + value


@tensorlake_function(next=sum_of_squares)
def square_values(value: int) -> int:
    return value * value


@tensorlake_function(next=square_values)
def parallel_map(count: int) -> List[int]:
    return list(range(count))


_GRAPH_PARAM_SETS = [(False), (True)]


class TestRouting(unittest.TestCase):
    @parameterized.parameterized.expand(_GRAPH_PARAM_SETS)
    def test_fan_out(self, is_remote):
        graph = Graph(name=test_graph_name(self), start_node=fan_out)
        graph = remote_or_local_graph(graph, is_remote)
        inv = graph.run(block_until_done=True, value=3)
        a_out = graph.output(inv, "a")
        b_out = graph.output(inv, "b")

        self.assertEqual(6, a_out[0])  # 6 == 3 + 3
        self.assertEqual(7, b_out[0])  # 7 == 3 + 4

    @parameterized.parameterized.expand(_GRAPH_PARAM_SETS)
    def test_route_out(self, is_remote):
        graph = Graph(name=test_graph_name(self), start_node=route_out)
        graph = remote_or_local_graph(graph, is_remote)
        inv = graph.run(block_until_done=True, value=3)
        a_out = graph.output(inv, "a")
        b_out = graph.output(inv, "b")
        c_out = graph.output(inv, "c")

        # Verify graph outputs.
        self.assertEqual([], a_out)
        self.assertEqual(9, b_out[0])  # 7 == 3 + 2 + 4
        self.assertEqual(10, c_out[0])  # 8 == 3 + 2 + 5

    @parameterized.parameterized.expand(_GRAPH_PARAM_SETS)
    def test_parallel_map(self, is_remote):
        graph = Graph(name=test_graph_name(self), start_node=parallel_map)
        graph = remote_or_local_graph(graph, is_remote)
        inv = graph.run(block_until_done=True, count=3)
        sum_out = graph.output(inv, "sum_of_squares")

        # Verify graph outputs.
        self.assertEqual(5, sum_out[0])  # 0^2 + 1^2 + 2^2


if __name__ == "__main__":
    unittest.main()
