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
# API function must have a single user supplied argument called `request`, this is HTTP calling convention. If the first argument
# is called `ctx` then RequestContext is injected. User has to pass it to all functions that it calls if they have ctx parameter too,
# This is mainly to make this code look like Python and thus get all the IDE and LLM support smoothly.
#
# The API function needs to have both @tensorlake.api and @tensorlake.function decorators.
# `@tensorlake.function` decorator configures all typical function attributes.
@tensorlake.api(
    description="test", version="2.0"
)  # The API name is the name of the function.
@tensorlake.function(cpu=1.0, memory=1.0)
def my_api(
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


# Every class that has a @tensorlake.function() decorator on its method(s) must have a constructor with no arguments (except self).
# This is because when this class is called by other functions using `TestGraphFunction3().run(...)` we don't support
# different constructor arguments for different requests. In the future we can support it by e.g. hashing constructor
# args and kwargs and adding this to the function name but this is too complex to support right now.
# @tensorlake.cls() decorator makes actual class constructor body empty when ppl call TestGraphFunction3(). When however we're running
# a graph the original __init__ is actually called.
@tensorlake.cls()
class TestGraphFunction3:
    def __init__(self):
        # Load a big model here
        import time

        time.sleep(100)
        self.magic_number: int = 2

    # When @tensorlake.function() is applied to a class method it uses its __init__(self) as one time container initialization hook.
    #
    # This is not a @staticmethod because a static method doesn't have self. And I assume that ppl are much less used to using static methods.
    # So we want to give them a friendly instance method here.
    # Not using __call__(self) here because I assume that people are not generally familiar with operator overloading in Python and also
    # because it'd result in an odd syntax at call sites: `TestGraphFunction3()(ctx, d, ix)`.
    @tensorlake.function()
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

    # Any number of class methods can be decorated with @tensorlake.function().
    @tensorlake.function()
    def another_method(self, ctx: tensorlake.RequestContext, d: dict) -> str:
        return (
            f"Another method called with key1: {d.get('key1')}, key2: {d.get('key2')}"
        )


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
@tensorlake.function()
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
    test_api_local_runner = tensorlake.LocalRunner(my_api)
    # run(block_until_done=True) by default.
    request: tensorlake.Request = test_api_local_runner.run(
        request={"key1": "value1", "key2": "value2"}
    )
    # request: tensorlake.Request = my_api.run_local(request={"key1": "value1", "key2": "value2"})
    # request: tensorlake.Request = my_api.run_remote(request={"key1": "value1", "key2": "value2"})
    test_api_remote_runner = tensorlake.RemoteRunner(
        my_api
    )  # or tensorlake.RemoteRunner("my_api")
    # test_api_remote_runner.run(request={"key1": "value1", "key2": "value2"})

    # To deploy the graph:
    my_api.deploy()

    api_func_output: List[Any] = request.function_output(
        my_api,  # Or "my_api" - there's no separate function.name attribute anymore
    )
    # api_func_output == [TensorlakeFunctionCall(test_graph_function_2_fanout, d, 10)]
    func_2_fanout_output: List[Any] = request.function_output(test_graph_function_2_map)
    # func_2_fanout_output is List[TensorlakeFunctionCall(...), ...]
    func_3_output: List[Any] = request.function_output(TestGraphFunction3.run)
    # func_3_output == [TensorlakeFunctionCall(...)]
    func_4_output: List[Any] = request.function_output(test_graph_function_4)
    # func_4_output == [TensorlakeFunctionCall(...)]
    func_5_fanin_output: List[str] = request.function_output(
        test_graph_function_5_reduce
    )
    # func_5_fanin_output == ["value10 value11 value12 ... value19 "]


if __name__ == "__main__":
    # main()
    pass


# Extra examples outside of the main graph above ^:
@tensorlake.function()
def multy_function_call(
    ctx: tensorlake.RequestContext, foo: str
) -> List[tensorlake.FunctionCall]:
    # This return statement starts two request branches from here.
    # This is essentially the same as map but with different functions instead of one reducer function.
    return [
        TestGraphFunction3().run(ctx, {"key1": foo}, ix=0),
        test_graph_function_4(ctx, {"key2": foo}, ix=1),
    ]


# Wait for up to `max_batch_wait` seconds for batch of size up to `max_batch_size` before calling the function.
# To support batching the function needs to have all its arguments being lists.
# When the function is called each list item at index X corresponds to the same request. The function should return
# a list with each item of it at index X being the output of the request at index X.
# If the output list length is not equal to the inputs length then SDK fails each alloc in the batch with a retriable
# exception (not RequestError). We need to be strict about this because each input item can be for a different request
# and each request can be for different customers of the users code. So we should exclude risk of a request
# getting data from a different request due to a coding error in customer code.
@tensorlake.batched(max_size=5, max_wait=3.0)
@tensorlake.function()
def batched_function(
    ctxs: List[tensorlake.RequestContext], inputs: List[str], targets: List[int]
) -> List[str | tensorlake.FunctionCall]:
    outputs: List[str | tensorlake.FunctionCall] = []
    for ctx, input, target in zip(ctxs, inputs, targets):
        outputs.append(multy_function_call(ctx, input + str(target)))
        # Once we support yield this would be:
        # yield multy_function_call(ctx, input + str(target))
    return outputs


# Calling batched function looks like:
@tensorlake.function()
def batched_function_caller(
    ctx: tensorlake.RequestContext,
) -> List[str | tensorlake.FunctionCall]:
    # The lists don't have to have len() == 1, just need to be all the same length.
    return batched_function([ctx], ["foo"], [0])


# Batched reducers are also supported by composing the decorators. This is a pretty advanced use case though.
# The implementation of decorators should not assume any ordering, i.e. the decorators just set fields in the
# internal constructed object and as all of them set different fields then we're good here.
@tensorlake.batched(max_size=5, max_wait=3.0)
@tensorlake.reducer()
@tensorlake.function()
def batched_reducer(
    ctxs: List[tensorlake.RequestContext],
    value1s: List[str],
    ixs: List[int],
    is_last_values: List[bool] = [False],
    accumulators: List[str] = [""],
) -> List[str | tensorlake.FunctionCall]:
    outputs: List[str | tensorlake.FunctionCall] = []

    for ctx, value1, ix, is_last_value, accumulator in zip(
        ctxs, value1s, ixs, is_last_values, accumulators
    ):
        print(ctx.request_state.get("key1"))
        new_accumulator = accumulator + value1 + str(ix) + " "
        if is_last_value:
            # Pass the final reduced value to the next function.
            outputs.append(test_graph_function_6(new_accumulator))
        else:
            # This function returns actual data as its outputs while it's reducing.
            outputs.append(new_accumulator)

    return outputs
