import unittest
from pathlib import Path
from typing import List, Tuple, Union

import parameterized
from pydantic import BaseModel
from testing import remote_or_local_graph, test_graph_name
from typing_extensions import TypedDict

from tensorlake import (
    Graph,
    GraphRequestContext,
    Map,
    RemoteGraph,
    RouteTo,
    TensorlakeCompute,
    tensorlake_function,
)
from tensorlake.functions_sdk.data_objects import File


class SimpleModelObjectStr(BaseModel):
    x: str


class SimpleModelObjectInt(BaseModel):
    x: int


@tensorlake_function()
def simple_function(x: SimpleModelObjectStr) -> SimpleModelObjectStr:
    return SimpleModelObjectStr(x=x.x + "b")


@tensorlake_function(input_encoder="json", output_encoder="json")
def simple_function_with_json_encoder(x: str) -> str:
    return x + "b"


@tensorlake_function(input_encoder="json")
def simple_function_with_str_as_input(x: str) -> str:
    return x + "cc"


@tensorlake_function(input_encoder="invalid")
def simple_function_with_invalid_encoder(
    x: SimpleModelObjectStr,
) -> SimpleModelObjectStr:
    return SimpleModelObjectStr(x=x.x + "b")


class ComplexObject(BaseModel):
    invocation_id: str
    graph_name: str
    graph_version: str


@tensorlake_function(inject_ctx=True)
def simple_function_ctx(
    ctx: GraphRequestContext, x: SimpleModelObjectStr
) -> ComplexObject:
    ctx.request_state.set("my_key", 10)
    ctx.request_state.timer("test_timer", 1.8)
    ctx.request_state.counter("test_counter", 8)
    return ComplexObject(
        invocation_id=ctx.request_id,
        graph_name=ctx.graph_name,
        graph_version=ctx.graph_version,
    )


@tensorlake_function(inject_ctx=True)
def simple_function_ctx_b(ctx: GraphRequestContext, x: ComplexObject) -> int:
    val = ctx.request_state.get("my_key")
    return val + 1


class SimpleFunctionCtxC(TensorlakeCompute):
    name = "SimpleFunctionCtxC"
    inject_ctx = True

    def __init__(self):
        super().__init__()

    def run(self, ctx: GraphRequestContext, x: ComplexObject) -> int:
        print(f"ctx: {ctx}")
        val = ctx.request_state.get("my_key")
        assert val == 10
        not_present = ctx.request_state.get("not_present")
        assert not_present is None
        return val + 1


@tensorlake_function()
def generate_seq(x: int) -> Map[int]:
    return Map(range(x))


@tensorlake_function()
def square(x: int) -> int:
    return x * x


@tensorlake_function(input_encoder="json", output_encoder="json")
def square_with_json_encoder(x: int) -> int:
    return x * x


class Sum(BaseModel):
    val: int = 0


@tensorlake_function(accumulate=Sum)
def sum_of_squares(init_value: Sum, x: int) -> Sum:
    init_value.val += x
    return init_value


class JsonSum(TypedDict):
    val: int


@tensorlake_function(accumulate=JsonSum, input_encoder="json")
def sum_of_squares_with_json_encoding(init_value: JsonSum, x: int) -> JsonSum:
    val = init_value.get("val", 0)
    init_value["val"] = val + x
    return init_value


@tensorlake_function()
def make_it_string(x: Sum) -> str:
    return str(x.val)


@tensorlake_function()
def add_two(x: Sum) -> int:
    return x.val + 2


@tensorlake_function()
def add_three(x: Sum) -> int:
    return x.val + 3


@tensorlake_function(next=[add_two, add_three])
def route_if_even(x: Sum) -> RouteTo[Sum, Union[add_two, add_three]]:
    print(f"routing input {x}")
    if x.val % 2 == 0:
        return RouteTo(x, add_three)
    else:
        return RouteTo(x, add_two)


@tensorlake_function()
def make_it_string_from_int(x: int) -> str:
    return str(x)


@tensorlake_function()
def handle_file(f: File) -> int:
    return len(f.data)


def create_pipeline_graph_with_map(test_case: unittest.TestCase) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case), description="test", start_node=generate_seq
    )
    graph.add_edge(generate_seq, square)
    return graph


def create_pipeline_graph_with_map_reduce(test_case: unittest.TestCase) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case), description="test", start_node=generate_seq
    )
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, make_it_string)
    return graph


def create_pipeline_graph_with_map_reduce_with_json_encoder(
    test_case: unittest.TestCase,
) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case),
        description="test",
        start_node=square_with_json_encoder,
    )
    graph.add_edge(square_with_json_encoder, sum_of_squares_with_json_encoding)
    return graph


def create_pipeline_graph_with_different_encoders(
    test_case: unittest.TestCase,
) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case),
        description="test",
        start_node=simple_function_with_json_encoder,
    )
    graph.add_edge(simple_function_with_json_encoder, simple_function_with_str_as_input)
    return graph


def create_router_graph(test_case: unittest.TestCase) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case), description="test", start_node=generate_seq
    )
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, route_if_even)
    graph.add_edge(add_two, make_it_string_from_int)
    graph.add_edge(add_three, make_it_string_from_int)
    return graph


def create_simple_pipeline_graph(test_case: unittest.TestCase) -> Graph:
    graph = Graph(
        name=test_graph_name(test_case),
        description="A simple pipeline",
        start_node=generate_seq,
    )
    graph.add_edge(generate_seq, square)
    graph.add_edge(square, sum_of_squares)
    graph.add_edge(sum_of_squares, make_it_string)
    return graph


class SimpleFunctionCtxClsObject(BaseModel):
    x: int

    def __eq__(self, other):
        if isinstance(other, SimpleFunctionCtxClsObject):
            return self.x == other.x
        return False


class SimpleFunctionCtxCls(TensorlakeCompute):
    name = "SimpleFunctionCtxCls"

    def __init__(self):
        super().__init__()

    def run(self, obj: SimpleFunctionCtxClsObject) -> SimpleFunctionCtxClsObject:
        return SimpleFunctionCtxClsObject(x=obj.x + 1)


class SimpleRouterCtxClsObject(BaseModel):
    x: int


class SimpleFunctionCtxCls1(TensorlakeCompute):
    name = "SimpleFunctionCtxCls1"

    def __init__(self):
        super().__init__()

    def run(self, obj: SimpleRouterCtxClsObject) -> SimpleRouterCtxClsObject:
        return SimpleRouterCtxClsObject(x=obj.x + 1)


class SimpleFunctionCtxCls2(TensorlakeCompute):
    name = "SimpleFunctionCtxCls2"

    def __init__(self):
        super().__init__()

    def run(self, obj: SimpleRouterCtxClsObject) -> SimpleRouterCtxClsObject:
        return SimpleRouterCtxClsObject(x=obj.x + 2)


class SimpleRouterCtxCls(TensorlakeCompute):
    name = "SimpleRouterCtxCls"
    next = [SimpleFunctionCtxCls1, SimpleFunctionCtxCls2]

    def __init__(self):
        super().__init__()

    def run(
        self, obj: SimpleRouterCtxClsObject
    ) -> RouteTo[
        SimpleRouterCtxClsObject, Union[SimpleFunctionCtxCls1, SimpleFunctionCtxCls2]
    ]:
        if obj.x % 2 == 0:
            return RouteTo(obj, SimpleFunctionCtxCls1)
        else:
            return RouteTo(obj, SimpleFunctionCtxCls2)


@tensorlake_function()
def return_dict(x: int) -> dict:
    return {"input": dict(x=1, y=2, z=3)}


@tensorlake_function()
def sum_dict(d: dict) -> int:
    return d["input"]["x"] + d["input"]["y"] + d["input"]["z"]


@tensorlake_function(input_encoder="json", output_encoder="json")
def return_dict_json(x: int) -> dict:
    return {"input": dict(x=1, y=2, z=3)}


@tensorlake_function(input_encoder="json", output_encoder="json")
def sum_dict_json(d: dict) -> int:
    return d["input"]["x"] + d["input"]["y"] + d["input"]["z"]


@tensorlake_function()
def return_multiple_dicts(x: int) -> dict:
    return {"input1": dict(x=1, y=2, z=3), "input2": dict(x=1, y=2, z=3)}


@tensorlake_function()
def sum_multiple_dicts(d: dict) -> int:
    return (
        d["input1"]["x"]
        + d["input1"]["y"]
        + d["input1"]["z"]
        + d["input2"]["x"]
        + d["input2"]["y"]
        + d["input2"]["z"]
    )


@tensorlake_function()
def map_to_dicts_with_index_of_each_character_wrapped_into_data_dict(
    text: str,
) -> Map[dict]:
    return Map(
        [{"data": {"index": index, "char": char}} for index, char in enumerate(text)]
    )


@tensorlake_function()
def format_index_and_char_wrapped_into_data_dict(d: dict) -> str:
    return f"{d['data']['char']}={d['data']['index']}"


@tensorlake_function()
def return_none_if_arg_odd(x: int) -> int:
    if x % 2 == 0:
        return x
    return None


@tensorlake_function()
def add_two_int_arg(x: int) -> int:
    return x + 2


@tensorlake_function(input_encoder="json", output_encoder="json")
def return_pydantic_base_model_json(x: int) -> dict:
    return {"input": SimpleModelObjectInt(x=x).model_dump()}


@tensorlake_function(input_encoder="json", output_encoder="json")
def return_field_from_pydantic_base_model_json(d: dict) -> int:
    p = SimpleModelObjectInt.model_validate(d["input"])
    return p.x


@tensorlake_function()
def should_not_run(x: str) -> str:
    return x + "c"


@tensorlake_function(next=should_not_run)
def simple_success(x: str) -> RouteTo[str, should_not_run]:
    return RouteTo(x + "b", [])


class TestGraphBehaviors(unittest.TestCase):
    @parameterized.parameterized.expand([(False), (True)])
    def test_simple_function(self, is_remote):
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=simple_function
        )
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(
            request=SimpleModelObjectStr(x="a"), block_until_done=True
        )
        output = graph.output(invocation_id, "simple_function")
        # TODO: Do self.assertEqual(output, [MyObject(x="ab")]) here and in other tests
        # once we know why Pydantic objects == is False when all their field values are
        # the same. This only happens when graph code doesn't getp updated on second and
        # later test runs because graph version didn't change and graph exists already.
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0].x, "ab")

    @parameterized.parameterized.expand([(False), (True)])
    def test_simple_function_cls(self, is_remote):
        graph = Graph(name=test_graph_name(self), start_node=SimpleFunctionCtxCls)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(
            block_until_done=True, request=SimpleFunctionCtxClsObject(x=1)
        )
        output = graph.output(invocation_id, "SimpleFunctionCtxCls")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0].x, 2)

    @parameterized.parameterized.expand([(False), (True)])
    def test_simple_function_with_json_encoding(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=simple_function_with_json_encoder,
        )
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request="a")
        output = graph.output(invocation_id, "simple_function_with_json_encoder")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0], "ab")

    @parameterized.parameterized.expand([True])
    def test_remote_graph_by_name(self, is_remote):
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=simple_function
        )
        remote_or_local_graph(graph, is_remote)
        # Gets the graph by name
        graph = RemoteGraph.by_name(test_graph_name(self))
        invocation_id = graph.run(
            block_until_done=True, request=SimpleModelObjectStr(x="a")
        )
        output = graph.output(invocation_id, "simple_function")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0].x, "ab")

    @parameterized.parameterized.expand([(False), (True)])
    def test_simple_function_with_invalid_encoding(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=simple_function_with_invalid_encoder,
        )
        graph = remote_or_local_graph(graph, is_remote)
        self.assertRaises(
            ValueError,
            graph.run,
            block_until_done=True,
            request=SimpleModelObjectStr(x="a"),
        )

    @parameterized.parameterized.expand([(False), (True)])
    def test_return_dict_as_args(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=return_dict,
        )
        graph.add_edge(return_dict, sum_dict)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=1)
        output = graph.output(invocation_id, sum_dict.name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 6)

    @parameterized.parameterized.expand([(False), (True)])
    def test_return_multiple_dict_as_args(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=return_multiple_dicts,
        )
        graph.add_edge(return_multiple_dicts, sum_multiple_dicts)
        graph = remote_or_local_graph(
            graph,
            is_remote,
        )
        invocation_id = graph.run(block_until_done=True, request=1)
        output = graph.output(invocation_id, sum_multiple_dicts.name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 12)

    @parameterized.parameterized.expand([(False), (True)])
    def test_return_dict_args_json(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=return_dict_json,
        )
        graph.add_edge(return_dict_json, sum_dict_json)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=1)
        output = graph.output(invocation_id, sum_dict_json.name)
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 6)

        output1 = graph.output(invocation_id, return_dict_json.name)
        self.assertEqual(len(output1), 1)
        self.assertEqual(output1[0], {"input": {"x": 1, "y": 2, "z": 3}})

    @parameterized.parameterized.expand([(False), (True)])
    def test_return_dict_args_as_dict_in_list(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=map_to_dicts_with_index_of_each_character_wrapped_into_data_dict,
        )

        graph.add_edge(
            map_to_dicts_with_index_of_each_character_wrapped_into_data_dict,
            format_index_and_char_wrapped_into_data_dict,
        )
        graph = remote_or_local_graph(
            graph,
            is_remote,
        )
        invocation_id = graph.run(block_until_done=True, request="hi")
        output = graph.output(
            invocation_id,
            map_to_dicts_with_index_of_each_character_wrapped_into_data_dict.name,
        )
        self.assertEqual(len(output), 2)
        self.assertEqual(output[0], {"data": {"index": 0, "char": "h"}})
        self.assertEqual(output[1], {"data": {"index": 1, "char": "i"}})

    @parameterized.parameterized.expand([(False), (True)])
    def test_map_operation(self, is_remote):
        graph = create_pipeline_graph_with_map(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=3)
        output_seq = graph.output(invocation_id, "generate_seq")
        self.assertEqual(sorted(output_seq), [0, 1, 2])
        output_sq = graph.output(invocation_id, "square")
        self.assertEqual(sorted(output_sq), [0, 1, 4])

    @parameterized.parameterized.expand([(False), (True)])
    def test_map_reduce_operation(self, is_remote):
        graph = create_pipeline_graph_with_map_reduce(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=3)
        output_sum_sq = graph.output(invocation_id, "sum_of_squares")
        self.assertEqual(len(output_sum_sq), 1)
        self.assertEqual(output_sum_sq[0].val, 5)
        output_str = graph.output(invocation_id, "make_it_string")
        self.assertEqual(output_str, ["5"])

    @parameterized.parameterized.expand([(False), (True)])
    def test_map_reduce_operation_with_json_encoding(self, is_remote):
        graph = create_pipeline_graph_with_map_reduce_with_json_encoder(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=3)
        output_square_sq_with_json_encoding = graph.output(
            invocation_id, "square_with_json_encoder"
        )
        self.assertEqual(output_square_sq_with_json_encoding, [9])
        output_sum_sq_with_json_encoding = graph.output(
            invocation_id, "sum_of_squares_with_json_encoding"
        )
        self.assertEqual(output_sum_sq_with_json_encoding, [{"val": 9}])

    @parameterized.parameterized.expand([(False), (True)])
    def test_graph_with_different_encoders(self, is_remote=False):
        graph = create_pipeline_graph_with_different_encoders(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request="a")
        simple_function_with_json_encoder = graph.output(
            invocation_id, "simple_function_with_json_encoder"
        )
        simple_function_output = graph.output(
            invocation_id, "simple_function_with_str_as_input"
        )
        print(f"simple_fn_multiple_input_output: {simple_function_with_json_encoder}")
        self.assertEqual(simple_function_with_json_encoder, ["ab"])
        self.assertEqual(simple_function_output, ["abcc"])

    @parameterized.parameterized.expand([(False), (True)])
    def test_router_graph_behavior(self, is_remote):
        graph = create_router_graph(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=3)

        output_add_two = graph.output(invocation_id, "add_two")
        self.assertEqual(output_add_two, [7])
        try:
            graph.output(invocation_id, "add_three")
        except Exception as e:
            self.assertEqual(
                str(e),
                f"no results found for fn add_three on graph {test_graph_name(self)}",
            )

        output_str = graph.output(invocation_id, "make_it_string_from_int")
        self.assertEqual(output_str, ["7"])

    @parameterized.parameterized.expand([(False), (True)])
    def test_router_graph_behavior_cls(self, is_remote):
        graph = Graph(test_graph_name(self), start_node=SimpleRouterCtxCls)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(
            block_until_done=True, request=SimpleRouterCtxClsObject(x=1)
        )
        output = graph.output(invocation_id, "SimpleFunctionCtxCls2")
        self.assertTrue(len(output) == 1)
        self.assertEqual(output[0].x, 3)

    @parameterized.parameterized.expand([(False), (True)])
    def test_invoke_file(self, is_remote):
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=handle_file
        )
        graph = remote_or_local_graph(graph, is_remote)
        import os

        data = Path(os.path.dirname(__file__) + "/test_file").read_text()
        file = File(data=data, metadata={"some_val": 2})

        invocation_id = graph.run(
            request=file,
            block_until_done=True,
        )

        output = graph.output(invocation_id, "handle_file")
        self.assertEqual(output, [12])

    @parameterized.parameterized.expand([(False), (True)])
    def test_pipeline(self, is_remote):
        graph: Graph = create_simple_pipeline_graph(self)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=3)
        output = graph.output(invocation_id, "make_it_string")
        self.assertEqual(output, ["5"])

    @parameterized.parameterized.expand([(False), (True)])
    def test_ignore_none_in_map(self, is_remote):
        graph = Graph(
            test_graph_name(self),
            description="test",
            start_node=generate_seq,
        )
        graph.add_edge(generate_seq, return_none_if_arg_odd)
        graph.add_edge(return_none_if_arg_odd, add_two_int_arg)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=5)
        output = graph.output(invocation_id, "add_two_int_arg")
        self.assertEqual(sorted(output), [2, 4, 6])

    @parameterized.parameterized.expand([(False), (True)])
    def test_graph_context(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=simple_function_ctx,
        )
        graph.add_edge(simple_function_ctx, simple_function_ctx_b)
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(
            request=SimpleModelObjectStr(x="a"), block_until_done=True
        )
        output2 = graph.output(invocation_id, "simple_function_ctx_b")
        self.assertEqual(output2[0], 11)
        graph1 = Graph(
            name=test_graph_name(self) + "1",
            description="test",
            start_node=simple_function_ctx,
        )
        graph1.add_edge(simple_function_ctx, SimpleFunctionCtxC)
        graph1 = remote_or_local_graph(graph1, is_remote)
        invocation_id = graph1.run(
            request=SimpleModelObjectStr(x="a"), block_until_done=True
        )
        output2 = graph1.output(invocation_id, "SimpleFunctionCtxC")
        self.assertEqual(len(output2), 1)
        self.assertEqual(output2[0], 11)

    @parameterized.parameterized.expand([(False), (True)])
    def test_graph_router_start_node(self, is_remote):
        graph = Graph(
            name=test_graph_name(self), description="test", start_node=route_if_even
        )
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=Sum(val=2))
        output = graph.output(invocation_id, "add_three")
        self.assertEqual(output, [5])

    @parameterized.parameterized.expand([(False), (True)])
    def test_return_pydantic_base_model_json(self, is_remote):
        """
        This test also serves as an example of how to use Pydantic BaseModel as JSON input and output.
        """
        graph = Graph(
            name=test_graph_name(self),
            description="test",
            start_node=return_pydantic_base_model_json,
        )
        graph.add_edge(
            return_pydantic_base_model_json, return_field_from_pydantic_base_model_json
        )
        graph = remote_or_local_graph(graph, is_remote)
        invocation_id = graph.run(block_until_done=True, request=1)
        output = graph.output(
            invocation_id, return_field_from_pydantic_base_model_json.name
        )
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0], 1)

    @parameterized.parameterized.expand([(False), (True)])
    def test_unreachable_graph_nodes(self, is_remote):
        graph = Graph(
            name=test_graph_name(self),
            description="test unreachable nodes in the graph",
            start_node=simple_function,
        )
        graph.add_edge(add_two, add_three)
        if is_remote:
            self.assertRaises(Exception, remote_or_local_graph, graph, is_remote)
        else:
            graph = remote_or_local_graph(graph, is_remote)
            self.assertRaises(
                Exception,
                graph.run,
                request=SimpleModelObjectStr(x="a"),
                block_until_done=True,
            )


if __name__ == "__main__":
    unittest.main()
