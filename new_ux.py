from typing import Any, Iterable, List

import tensorlake

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
# @tensorlake_function returns an instance of Callable that knows all the function attributes.
# Using a Callable with () operator would result in better IDE and LLM support and much more natural coding experience.
# The () call returns TensorlakeFunctionCall object that knows the function name and args, kwargs this is the future I mentioned above.
#
# Server will need to be aware about function calls but not about a lot of things. Just function name. The rest (kwargs, args, etc) is
# SDK side concern and Server has no visibility into that data.


# input_encoder will be "json" by default. When deserializing an input from a json string we'll try a heuristic approach so e.g.
# a "{'foo': 'bar'}" string is not deserialized into a regular Python dict() object but it's deserialized into data type used in the
# function parameter type annotation if it has a constructor from dict() that we deserialized from json.
#
# API function must have a single user supplied argument caller `request`, this is HTTP calling convention. If the first argument
# is called `ctx` then RequestContext is injected. User has to pass it to all functions that it calls if they have ctx parameter too,
# This is mainly to make this code look like Python and thus get all the IDE and LLM support smoothly.
#
# The decorator attributes are the same as @tensorlake.function + attributes for defining the graph.
@tensorlake.graph_api(graph_name="test_graph", graph_description="test", version="2.0")
def test_graph_function_1_api(
    ctx: tensorlake.RequestContext, request: dict
) -> tensorlake.FunctionCall:  # Actual return type, all return types are ignored by SDK
    print(
        f"Received request with key1: {request.get('key1')}, key2: {request.get('key2')}"
    )
    ctx.state.set("key1", request.get("key1"))
    # Require explicit passing of the context, this aligns with its semantic that it's the same context per request.
    return test_graph_function_2_map(ctx, request, times=10)


# Non API functions can have multiple arguments
@tensorlake.function()
def test_graph_function_2_map(
    ctx: tensorlake.RequestContext, d: dict, times: int
) -> Iterable[
    tensorlake.FunctionCall
]:  # The returned list can have both function calls and outputs for this function, no problem with that.
    print(
        f"Processing data in test_graph_function_2 with key1: {d.get('key1')}, key2: {d.get('key2')}, times: {times}"
    )
    return [TestGraphFunction3().run(ctx, d, ix) for ix in range(times)]
    # Or tuple:
    # return (TestGraphFunction3().run(ctx, d, 0), TestGraphFunction3().run(ctx, d, 1), TestGraphFunction3().run(ctx, d, 2))
    # Or map:
    # return map(lambda ix: TestGraphFunction3().run(ctx, d, ix), range(times))


# When @tensorlake.function() is applied to a class it uses its __init__(self) as one time container initialization hook.
# The class must have def run(self, ...) method. This is the actual function body.
@tensorlake.function()
# Also available:
# @tensorlake.reducer(...)
# @tensorlake.graph_api(...)
class TestGraphFunction3:
    # The class must have an empty constructor.
    # This is because when this class is called by other functions using `TestGraphFunction3().run(...)` we don't support
    # different constructor arguments for different requests. In the future we can support it by e.g. hashing constructor
    # args and kwargs and adding this to the function name but this is too complex to support right now.

    # The decorator makes actual class constructor body empty when ppl call TestGraphFunction3(). When however we're running
    # a graph the original __init__ is actually called.
    def __init__(self):
        # Load a big model here
        import time

        time.sleep(100)
        self.magic_number: int = 2

    # This is not a @staticmethod because a static method doesn't have self. And I assume that ppl are much less used to using static methods.
    # So we want to give them a friendly instance method here.
    #
    # Not using __call__(self) here because I assume that people are not generally familiar with operator overloading in Python and also
    # because it'd result in an odd syntax at call sites: `TestGraphFunction3()(ctx, d, ix)`.
    def run(self, ctx: tensorlake.RequestContext, d: dict, ix: int):
        print(
            f"Processing data in TestGraphFunction3.run with key1: {d.get('key1')}, key2: {d.get('key2')}, ix: {ix}"
        )
        if ix % self.magic_number == 0:
            # if accumulator is supplied here then it's ignored. We can also raise runtime or validation error if this happens.
            return test_graph_function_5_reduce(ctx, value1=d["key1"], ix=ix)
        else:
            # Do extra processing before sending data to fanin.
            return test_graph_function_4(ctx, value2=d["key2"], ix=ix)


@tensorlake.function()
def test_graph_function_4(
    ctx: tensorlake.RequestContext,
    value2: str,
    ix: int,
) -> tensorlake.FunctionCall:
    print(f"Processing data in test_graph_function_4 with value2: {value2}, ix: {ix}")
    # Turn "value2" into "value1" and fanin.
    return test_graph_function_5_reduce(ctx, value1=value2.replace("1", "2"), ix=ix)


# If the function is reducer:
# * It has accumulator and is_last_value kwargs.
# * `accumulator`` kwarg must have default value of accumulator init value.
# * `is_last_value` kwarg must have a default value of False.
# * Its return value is interpreted as reduced accumulator value for the supplied input.
@tensorlake.reducer()
def test_graph_function_5_reduce(
    ctx: tensorlake.RequestContext,
    value1: str,
    ix: int,
    is_last_value: bool = False,
    accumulator: str = "",
) -> str | tensorlake.FunctionCall:
    print(ctx.request_state.get("key1"))
    new_accumulator = accumulator + value1 + str(ix) + " "
    if is_last_value:
        # Pass the final reduced value to the next function.
        return test_graph_function_6(new_accumulator)
    else:
        # This function returns actual data as its outputs while it's reducing.
        return new_accumulator


@tensorlake.function()
def test_graph_function_6(
    accumulator: str,
) -> str:
    print("Final accumulator value:", accumulator)
    # This function returns actual data as its outputs.
    return accumulator


def main():
    # We don't store any edges in graph definition anymore, all routing is dynamic now.
    # To run graph in local mode just use the function decorated with @tensorlake.graph.
    test_graph = tensorlake.LocalGraph(test_graph_function_1_api)
    # or test_graph = tensorlake.RemoteGraphClient(test_graph_function_1_api)
    # or test_graph = tensorlake.RemoteGraphClient("test_graph")
    # To deploy a remote graph:
    # tensorlake.RemoteGraphClient(test_graph_function_1_api).deploy()

    # run(block_until_done=True) by default.
    request_id: str = test_graph.run(request={"key1": "value1", "key2": "value2"})
    api_func_output: List[Any] = test_graph.function_output(
        request_id,
        test_graph_function_1_api,  # Or "test_graph_function_1_api" - there's no separate function.name attribute anymore
    )
    # api_func_output == [TensorlakeFunctionCall(test_graph_function_2_fanout, d, 10)]
    func_2_fanout_output: List[Any] = test_graph.function_output(
        request_id, test_graph_function_2_map
    )
    # func_2_fanout_output is List[TensorlakeFunctionCall(...), ...]
    func_3_output: List[Any] = test_graph.function_output(
        request_id, TestGraphFunction3.run
    )
    # func_3_output == [TensorlakeFunctionCall(...)]
    func_4_output: List[Any] = test_graph.function_output(
        request_id, test_graph_function_4
    )
    # func_4_output == [TensorlakeFunctionCall(...)]
    func_5_fanin_output: List[str] = test_graph.function_output(
        request_id, test_graph_function_5_reduce
    )
    # func_5_fanin_output == ["value10 value11 value12 ... value19 "]


if __name__ == "__main__":
    # main()
    pass
