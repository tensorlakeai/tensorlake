import os
import unittest
from typing import Any, List

from pydantic import BaseModel

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake


# TypedDict works too because it's just a regular dict() in runtime and dict() is json serializable and deserializable.
class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


# The payload type hint is required for SDK to deserialize the request payload into the
# Pydantic model class. Without it payload parameter will be a dict { "numbers": [...] }.
@tensorlake.api()
@tensorlake.function(cpu=1.0, memory=1.0, description="test API function")
def test_graph_api(ctx: tensorlake.RequestContext, payload: TestGraphRequestPayload):
    print(f"Received request with numbers: {payload.numbers}")
    ctx.state.set("numbers_count", len(payload.numbers))

    number_generators = [parse_number(ctx, number) for number in payload.numbers]
    # TODO: make it clear in runtime that these FunctionCalls() when used locally generate a warning + RequestException().
    # TODO: Add tensorlake.reduce with the same interface as https://docs.python.org/3/library/functools.html#functools.reduce.
    # .      The binding of the arguments to tensorlake.reduce is via positional args only.
    # TODO: Remove tensorlake.reducer decorator.

    # All the output values produced by parse_number calls are gradually supplied into sum_numbers reducer calls.
    # This allows users to quite explicitly control and understand what values are sent into reducer.
    #
    # We're going to support graph traversal on Server side. So we're supporting unbonded depth of the call tree.
    return foo(
        tensorlake.reduce(
            sum_numbers,
            number_generators,
            Accumulator(total=777),
            max_inputs_per_call=10,
        )
    )
    # This approach will also be available to other non-reducer functions that want to read values generated
    # by other functions without reducing these inputs. This is a natural fan-in use-case.
    # Example:
    # return print_numbers(numbers=number_generators, fmt_string="foo bar buzz %s")
    #
    # For such non-reducer functions we can also allow multiple such parameters (doesn't work well for reducers).
    # Example:
    # return multiply_and_print_numbers(a_numbers=number_generators, b_numbers=number_generators, fmt_string="foo bar buzz %s")
    #
    # Important: all values returned by any of the functions calls and their sub calls are supplied into the reducer/function.


@tensorlake.function()
def parse_number(ctx: tensorlake.RequestContext, number: str):
    print(f"parsing number '{number}'")
    # Raises ValueError if not a number.
    parsed_number = int(number)
    if parsed_number % 2 == 0:
        return MultiplierFunction().multiply(ctx, number=parsed_number)
    else:
        return MultiplierFunction().multiply(ctx, number=parsed_number - 1)


@tensorlake.cls()
class MultiplierFunction:
    def __init__(self):
        self.multiplier: int = 2

    @tensorlake.function()
    def multiply(self, ctx: tensorlake.RequestContext, number: int):
        print(f"Multiplying number: {number}, multiplier: {self.multiplier}")
        return number * self.multiplier


class Accumulator(BaseModel):
    total: int


# The type hints are only required to detect that the returned values are Pydantic models
# when deserializing the function value outputs from their json.
@tensorlake.function()
def sum_numbers(
    numbers: List[int],
    accumulator: Accumulator = Accumulator(total=0),
) -> tuple[Accumulator, tensorlake.FunctionCall]:
    print(f"adding numbers {numbers} to accumulator {accumulator}")
    accumulator.total = accumulator.total + sum(numbers)
    return accumulator, print_and_return_value(str(accumulator))


@tensorlake.function()
def print_and_return_value(value: str) -> str:
    print("Printed value:", value)
    return value


@tensorlake.function()
def format_number(
    number: int,
) -> str:
    print(f"Formatting number: {number}")
    return store_as_file(str(number).encode(), "text/plain"), str(number)


# bytes are not json-serializable
@tensorlake.function(input_serializer="pickle")
def store_as_file(content: bytes, content_type: str) -> tensorlake.File:
    print(f"Storing file with content type: {content_type}")
    return tensorlake.File(content=content, content_type=content_type)


class TestComplexGraph(unittest.TestCase):
    def test_local_function_call_of_complex_graph_produces_expected_outputs(self):
        # Any function can be called in local mode, not only API function.
        # This eases debugging for people.
        request: tensorlake.Request = tensorlake.call_local_function(
            test_graph_api(
                ctx=tensorlake.RequestContextPlaceholder(),
                payload=TestGraphRequestPayload(
                    numbers=[str(i) for i in range(10, 20)]
                ),
            )
        )

        test_graph_api_output: List[Any] = request.function_outputs(test_graph_api)
        self.assertEqual(test_graph_api_output, [])

        parse_number_output: List[Any] = request.function_outputs(parse_number)
        self.assertEqual(parse_number_output, [])

        # Or MultiplierFunction.multiply
        multiply_output: List[Any] = request.function_outputs(
            "MultiplierFunction.multiply"
        )
        self.assertEqual(multiply_output, [])

        expected_sum_numbers_outputs: List[Any] = []
        expected_print_and_return_value_outputs: List[Any] = []
        accumulator: int = 0
        for call_index in range(10):
            number: int = 10 + call_index
            if number % 2 != 0:
                number -= 1
            number *= 2
            accumulator += number
            if call_index != 9:
                expected_sum_numbers_outputs.append(Accumulator(total=accumulator))
                expected_print_and_return_value_outputs.append(f"total={accumulator}")
            else:
                expected_print_and_return_value_outputs.append(str(accumulator))

        sum_numbers_output: List[Any] = request.function_outputs(sum_numbers)
        self.assertEqual(sum_numbers_output, expected_sum_numbers_outputs)

        print_and_return_value_output: List[Any] = request.function_outputs(
            print_and_return_value
        )
        self.assertEqual(
            print_and_return_value_output, expected_print_and_return_value_outputs
        )

        format_number_output: List[Any] = request.function_outputs(format_number)
        self.assertEqual(
            format_number_output,
            ["280"],
        )

        store_as_file_output: List[Any] = request.function_outputs(store_as_file)
        self.assertEqual(
            store_as_file_output,
            [b"280"],
        )

    def test_local_api_call_of_complex_graph_produces_expected_outputs(self):
        request = tensorlake.call_local_api(
            "test_graph_api",
            TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
        )

        format_number_output: List[Any] = request.function_outputs(format_number)
        self.assertEqual(
            format_number_output,
            ["280"],
        )

    def test_remote_api_call_of_complex_graph_produces_expected_outputs(self):
        tensorlake.deploy(os.path.dirname(__file__))
        # TODO: implement.
        # request = tensorlake.call_remote_api(
        #     "test_graph_api",
        #     TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
        # )

        # format_number_output: List[Any] = request.function_output(format_number)
        # self.assertEqual(
        #     format_number_output,
        #     ["280"],
        # )


if __name__ == "__main__":
    unittest.main()
