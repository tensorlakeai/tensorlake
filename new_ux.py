import ast
import inspect
from typing import Any, Iterable, List

from tensorlake import (
    Graph,
    GraphRequestContext,
    TensorlakeCompute,
    TensorlakeFunctionCall,
    tensorlake_function,
)

# Overall goal:
# Make the coding experience as close to native Python as possible. This would result in easier onboarding and
# better DevEX. Also LLMs and IDE support will be much better.
#
# Overall idea:
# User don't need to specify edges in the graph, all routing is dynamic now.
# Use regular function calls using () operator to chain functions in the graph together.
# A @tensorlake_function calls other Tensorlake functions by returning a list of function call closures (basically futures).
# When a @tensorlake_function returns a non future value it means that this function actually produced output data.
# User doesn't need to specify which functions are in the graph as by default all functions decorated with
# @tensorlake_function register themselfs in a global list and all of them are considered to be part of all graphs created in
# the customer code base (all currently loaded Python modules).
# We'll provide an optional argument to Graph constructor to manually specify a list of functions that are part of the graph.
# There's also a PoC (see `get_called_tensorlake_computes`) on now to detect this automatically via AST traversal but I think
# that making every @tensorlake_function in the code base callable in the graph actually makes sense as it's more natural Python
# coding experience.

# Some technical preliminary details of the implementation:
# Delete TensorlakeCompute from SDK interface. Use @tensorlake_function(initializer=...) attribute instead for initialization.
# @tensorlake_function returns an instance of Callable that knows all the function attributes.
# Using a Callable with () operator would result in better IDE and LLM support and much more natural coding experience.
# The () call returns TensorlakeFunctionCall object that knows the function name and args, kwargs this is the future I mentioned above.

# Note: initially we don't need to make Server aware about TensorlakeFunctionCall, we can just serialize it as function output
# but later when we do cross graph calls or cross language calls we'll need that.

big_model_file_descriptor: int = None


def load_model() -> None:
    """
    This function is called once per process to load the model.
    It can be used to load any resources needed by the functions.
    """
    global big_model_file_descriptor
    big_model_file_descriptor = 1  # Simulating a file descriptor for a big model


# input_encoder will be "json" by default
# API function must have a single user supplied argument, this is HTTP calling convention where we deserialized
# HTTP request body into the first user supplied argument of the function.
# inject_ctx attribute can be deleted because we can just inspect type of the first argument of the function to determine
# if it's needed.
@tensorlake_function(input_encoder="json", initializer=load_model)
def test_graph_api(
    ctx: GraphRequestContext, d: dict
) -> TensorlakeFunctionCall:  # Actual return type, all return types are ignored by SDK
    print(f"Received request with key1: {d.get('key1')}, key2: {d.get('key2')}")
    print("Big model file descriptor is already loaded:", big_model_file_descriptor)
    ctx.request_state.set("key1", d.get("key1"))
    # Require explicit passing of the context, this aligns with its semantic that it's the same context per request.
    return test_graph_function_2_fanout(ctx, d, times=10)


# Non API functions can have multiple arguments
@tensorlake_function()
def test_graph_function_2_fanout(
    ctx: GraphRequestContext, d: dict, times: int
) -> Iterable[
    TensorlakeFunctionCall
]:  # The returned list can have both function calls and outputs for this function, no problem with that.
    print(
        f"Processing data in test_graph_function_2 with key1: {d.get('key1')}, key2: {d.get('key2')}, times: {times}"
    )
    return [test_graph_function_3(ctx, d, ix) for ix in range(times)]
    # Or tuple:
    # return (test_graph_function_3(ctx, d, 0), test_graph_function_3(ctx, d, 1), test_graph_function_3(ctx, d, 2))
    # Or map:
    # return map(lambda ix: test_graph_function_3(ctx, d, ix), range(times))


@tensorlake_function()
def test_graph_function_3(
    ctx: GraphRequestContext, d: dict, ix: int
) -> TensorlakeFunctionCall:
    print(
        f"Processing data in test_graph_function_3_fanout with key1: {d.get('key1')}, key2: {d.get('key2')}, ix: {ix}"
    )
    if ix % 2 == 0:
        # if accumulator is supplied here then it's ignored. We can also raise runtime or validation error if this happens.
        return test_graph_function_5_fanin(ctx, value1=d["key1"], ix=ix)
    else:
        # Do extra processing before sending data to fanin.
        return test_graph_function_4(ctx, value2=d["key2"], ix=ix)


@tensorlake_function()
def test_graph_function_4(
    ctx: GraphRequestContext, value2: str, ix: int, accumulator: str = ""
) -> TensorlakeFunctionCall:
    print(f"Processing data in test_graph_function_4 with value2: {value2}, ix: {ix}")
    # Turn "value2" into "value1" and fanin.
    return test_graph_function_5_fanin(ctx, value1=value2.replace("1", "2"), ix=ix)


# If the function is reducer:
# * It has accumulator kwarg.
# * The accumulator kwarg must have default value.
# * Its return value is interpreted as a single value even if it's iterable.
@tensorlake_function()
def test_graph_function_5_fanin(
    ctx: GraphRequestContext,
    value1: str,
    ix: int,
    is_last_value: bool,
    accumulator: str = "",
) -> str | TensorlakeFunctionCall:
    print(ctx.request_state.get("key1"))
    new_accumulator = accumulator + value1 + str(ix) + " "
    if is_last_value:
        # Pass the final reduced value to the next function.
        return test_graph_function_6(new_accumulator)
    else:
        # This function returns actual data as its outputs while it's reducing.
        return new_accumulator


@tensorlake_function()
def test_graph_function_6(
    accumulator: str,
) -> str:
    print("Final accumulator value:", accumulator)
    # This function returns actual data as its outputs.
    return accumulator


def main():
    # We parse api function AST recursively to figure out which functions are in the graph.
    # We don't store any edges in graph definition anymore, all routing is dynamic now.
    test_graph = Graph(name="test_graph", description="test", api=test_graph_api)
    request_id: str = test_graph.run(
        request={"key1": "value1", "key2": "value2"}, block_until_done=True
    )
    api_func_output: List[Any] = test_graph.function_output(
        request_id, test_graph_api.name
    )
    # api_func_output == [TensorlakeFunctionCall(test_graph_function_2_fanout, d, 10)]
    func_2_fanout_output: List[Any] = test_graph.function_output(
        request_id, test_graph_function_2_fanout.name
    )
    # func_2_fanout_output is List[TensorlakeFunctionCall(...), ...]
    func_3_output: List[Any] = test_graph.function_output(
        request_id, test_graph_function_3.name
    )
    # func_3_output == [TensorlakeFunctionCall(...)]
    func_4_output: List[Any] = test_graph.function_output(
        request_id, test_graph_function_4.name
    )
    # func_4_output == [TensorlakeFunctionCall(...)]
    func_5_fanin_output: List[str] = test_graph.function_output(
        request_id, test_graph_function_5_fanin.name
    )
    # func_5_fanin_output == ["value10 value11 value12 ... value19 "]


if __name__ == "__main__":
    # main()
    pass


# We'll allow customers to specify a list of functions that are part of the graph as an optional feature.
# Users will be able to use it if this automatic approach doesn't work due to an unknown yet reason.
def get_called_tensorlake_computes(
    caller: TensorlakeCompute,
) -> List[TensorlakeCompute]:
    """
    Returns a list of TensorlakeCompute (or decorated function) instances that are called
    directly in the body of the given function.
    Only detects direct function calls, not method calls (e.g., d.get(...)).
    """
    source: str = inspect.getsource(caller.run)

    # Parse the source code into an AST
    tree = ast.parse(source)

    # Find all function calls in the AST
    class CallVisitor(ast.NodeVisitor):
        def __init__(self):
            self.calls = []

        def visit_Call(self, node):
            # Only consider direct calls (not attribute calls like obj.method())
            if isinstance(node.func, ast.Name):
                self.calls.append(node.func.id)
            # Do NOT include ast.Attribute (which are method calls)
            self.generic_visit(node)

    visitor = CallVisitor()
    visitor.visit(tree)

    called_tensorlake_computes: List[TensorlakeCompute] = []
    # Get the globals from the function's module
    # caller is an instance of a dynamically created class, so __globals__ is not present on the instance.
    # Instead, use caller.run.__func__.__globals__ for staticmethod, or caller.run.__globals__ for function.
    run_fn = caller.run
    if hasattr(run_fn, "__func__"):
        fn_globals = run_fn.__func__.__globals__
    elif hasattr(run_fn, "__globals__"):
        fn_globals = run_fn.__globals__
    else:
        fn_globals = {}

    for call_name in visitor.calls:
        obj = fn_globals.get(call_name)
        if obj is not None:
            # Check if it's a TensorlakeCompute or a function decorated with @tensorlake_function
            if hasattr(obj, "_created_by_decorator") and getattr(
                obj, "_created_by_decorator"
            ):
                called_tensorlake_computes.append(obj)
            elif isinstance(obj, TensorlakeCompute):
                called_tensorlake_computes.append(obj)

    return called_tensorlake_computes


if __name__ == "__main__":
    # Call test_graph_api as it returns a TensorlakeCompute instance.
    called_funcs: List[TensorlakeCompute] = get_called_tensorlake_computes(
        test_graph_function_3()
    )
    print([func.name for func in called_funcs])
