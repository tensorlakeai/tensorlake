import unittest
from typing import List, Union

from pydantic import BaseModel

from tensorlake import RouteTo
from tensorlake.functions_sdk.functions import tensorlake_function
from tensorlake.functions_sdk.graph import Graph


class TestValidations(unittest.TestCase):
    def test_function_signature_types(self):
        class ComplexType(BaseModel):
            pass

        @tensorlake_function()
        def node1(a: int, b: ComplexType) -> int:
            pass

        @tensorlake_function()
        def node2(b):
            pass

        g = Graph(
            "test-graph",
            start_node=node1,
        )

        msg = "Input param b in node2 has empty type annotation"

        with self.assertRaises(Exception) as cm:
            g.add_edge(node1, node2)

        self.assertEqual(msg, str(cm.exception))

    def test_function_return_type_annotation(self):
        class ComplexType(BaseModel):
            pass

        @tensorlake_function()
        def node1(a: int, b: ComplexType) -> int:
            pass

        @tensorlake_function()
        def node2(b: float):
            pass

        g = Graph(
            "test-graph",
            start_node=node1,
        )

        msg = "Function node2 has empty return type annotation"

        with self.assertRaises(Exception) as cm:
            g.add_edge(node1, node2)

        self.assertEqual(msg, str(cm.exception))

    def test_callables_are_in_added_nodes(self):
        class ComplexType(BaseModel):
            pass

        def node1(a: int, b: ComplexType) -> int:
            pass

        @tensorlake_function()
        def node2(b: int) -> ComplexType:
            pass

        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node1,
            )

            g.add_edge(node1, node2)

        msg = "Unable to add node of type `<class 'function'>`. Required, `TensorlakeCompute`"
        self.assertEqual(msg, str(cm.exception))

    def test_router_callables_are_in_added_nodes_union(self):
        @tensorlake_function()
        def node0(a: int) -> int:
            pass

        @tensorlake_function()
        def node1(a: int) -> int:
            pass

        @tensorlake_function()
        def node2(a: int) -> int:
            pass

        @tensorlake_function()
        def node3(a: int) -> int:
            pass

        @tensorlake_function(next=[node1, node2])
        def router(a: int) -> RouteTo[int, Union[node1, node3]]:
            pass

        @tensorlake_function(next=[node2])
        def router2(a: int) -> RouteTo[int, node1]:
            pass

        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node0,
            )

            g.add_edge(node0, router)
        msg = "Unable to find 'node3' in available next nodes: ['node1', 'node2']"
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(Exception) as cm:
            g = Graph(
                "test-graph",
                start_node=node0,
            )

            g.add_edge(node0, router2)
        msg = "Unable to find 'node1' in available next nodes: ['node2']"
        self.assertEqual(msg, str(cm.exception))

    def test_route_validation_with_valid_return_type_signature(self):
        @tensorlake_function()
        def start() -> int:
            return 1

        @tensorlake_function()
        def end() -> int:
            return 1

        @tensorlake_function(next=[start, end])
        def route1(**kwargs: dict) -> Union[start, end]:
            return 10

        g = Graph(name="test", start_node=start)
        g.add_edge(start, route1)

    def test_unreachable_graph_nodes(self):
        @tensorlake_function()
        def start() -> int:
            return 1

        @tensorlake_function()
        def middle() -> int:
            return 1

        @tensorlake_function()
        def end() -> int:
            return 1

        graph = Graph(
            name="test_unreachable_graph_nodes",
            start_node=start,
        )
        graph.add_edge(middle, end)
        with self.assertRaises(Exception) as cm:
            graph.validate_graph()
        self.assertEqual(
            "Some nodes in the graph are not reachable from start node",
            str(cm.exception),
        )

    def test_validation_does_not_change_graph(self):
        """
        Ensure that the graph is not changed when calling validate_graph.

        This is to catch potential regressions like doing a defaultdict key creation.
        """

        @tensorlake_function()
        def start() -> int:
            return 1

        @tensorlake_function()
        def middle() -> int:
            return 1

        @tensorlake_function()
        def end() -> int:
            return 1

        graph = Graph(
            name="test_unreachable_graph_nodes",
            start_node=start,
        )
        graph.add_edge(start, middle)
        graph.add_edge(middle, end)

        graph_def = graph.definition()

        graph.validate_graph()

        graph_def2 = graph.definition()

        self.assertEqual(
            graph_def,
            graph_def2,
            "ensure graph is not changed for example by triggering a defaultdict key creation",
        )


if __name__ == "__main__":
    unittest.main()
