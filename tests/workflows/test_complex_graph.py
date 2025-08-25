import unittest
from typing import Any, Dict, List, TypedDict

from pydantic import BaseModel

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake


class TestGraphRequestPayload(TypedDict):
    numbers: List[str]


@tensorlake.api(description="test")
@tensorlake.function(cpu=1.0, memory=1.0)
def test_graph_api(ctx: tensorlake.RequestContext, payload: TestGraphRequestPayload):
    print(f"Received request with numbers: {payload['numbers']}")
    ctx.state.set("numbers_count", len(payload["numbers"]))
    # We can't return Iterable here because e.g. str is iterable and a single value.
    return [parse_number(ctx, number) for number in payload["numbers"]]


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

    @tensorlake.function(output_serializer="cloudpickle")
    def multiply(self, ctx: tensorlake.RequestContext, number: int):
        print(f"Multiplying number: {number}, multiplier: {self.multiplier}")
        return sum_numbers(number * self.multiplier)


# Pydantic model can only be passed to sum_numbers if it uses Cloudpickle serializer.
class Accumulator(BaseModel):
    total: int


@tensorlake.reducer()
@tensorlake.function(input_serializer="cloudpickle", output_serializer="cloudpickle")
def sum_numbers(
    number: int,
    is_last_value: bool = False,
    accumulator: Accumulator = Accumulator(total=0),
):
    print(
        f"adding number {number} to accumulator {accumulator}, is_last_value: {is_last_value}"
    )
    accumulator.total = accumulator.total + number
    if is_last_value:
        return [
            format_number(accumulator.total),
            print_and_return_value(str(accumulator.total)),
        ]
    else:
        return [accumulator, print_and_return_value(str(accumulator))]


@tensorlake.function()
def print_and_return_value(value: str) -> str:
    print("Printed value:", value)
    return value


# Simulating the case when we want to return raw JSON output as a graph result.
@tensorlake.function(input_serializer="cloudpickle", output_serializer="json")
def format_number(
    number: int,
) -> str:
    print(f"Formatting number: {number}")
    return str(number)


class TestComplexGraph(unittest.TestCase):
    def test_local_complex_graph_produces_expected_outputs(self):
        # Any function can be called in local mode, not only API function.
        # This eases debugging for people.
        request: tensorlake.Request = tensorlake.local_run(
            test_graph_api(
                ctx=tensorlake.RequestContextPlaceholder(),
                payload=TestGraphRequestPayload(
                    numbers=[str(i) for i in range(10, 20)]
                ),
            )
        )

        test_graph_api_output: List[Any] = request.function_output(test_graph_api)
        self.assertEqual(
            test_graph_api_output,
            [
                tensorlake.FunctionCall(
                    class_name=None,
                    function_name="parse_number",
                    args=[tensorlake.RequestContextPlaceholder(), str(number)],
                    kwargs={},
                )
                for number in range(10, 20)
            ],
        )

        for call_index in range(10):
            number: int = 10 + call_index
            parse_number_output: List[Any] = request.function_output(
                parse_number, call_index=call_index
            )
            self.assertEqual(
                parse_number_output,
                [
                    tensorlake.FunctionCall(
                        class_name="MultiplierFunction",
                        function_name="MultiplierFunction.multiply",
                        args=[tensorlake.RequestContextPlaceholder()],
                        kwargs={"number": (number if number % 2 == 0 else number - 1)},
                    )
                ],
            )

        for call_index in range(10):
            accumulator: int = 0
            number: int = 10 + call_index
            if number % 2 != 0:
                number -= 1
            number *= 2

            # Or MultiplierFunction.multiply
            multiply_output: List[Any] = request.function_output(
                "MultiplierFunction.multiply", call_index=call_index
            )
            self.assertEqual(
                multiply_output,
                [
                    tensorlake.FunctionCall(
                        class_name=None,
                        function_name="sum_numbers",
                        args=[number],
                        # Default reducer argument values are used in kwargs of the caller function.
                        kwargs={},
                    )
                ],
            )
            accumulator += number

        accumulator: int = 0
        for call_index in range(10):
            number: int = 10 + call_index
            if number % 2 != 0:
                number -= 1
            number *= 2
            accumulator += number

            sum_numbers_output: List[Any] = request.function_output(
                sum_numbers, call_index=call_index
            )
            if call_index != 9:
                self.assertEqual(
                    sum_numbers_output,
                    [
                        Accumulator(total=accumulator),
                        tensorlake.FunctionCall(
                            class_name=None,
                            function_name="print_and_return_value",
                            args=[f"total={accumulator}"],
                            kwargs={},
                        ),
                    ],
                )
                print_and_return_value_output: List[Any] = request.function_output(
                    print_and_return_value, call_index=call_index
                )
                self.assertEqual(
                    print_and_return_value_output,
                    [f"total={accumulator}"],
                )
            else:
                self.assertEqual(
                    sum_numbers_output,
                    [
                        tensorlake.FunctionCall(
                            class_name=None,
                            function_name="format_number",
                            args=[accumulator],
                            kwargs={},
                        ),
                        tensorlake.FunctionCall(
                            class_name=None,
                            function_name="print_and_return_value",
                            args=[str(accumulator)],
                            kwargs={},
                        ),
                    ],
                )
                print_and_return_value_output: List[Any] = request.function_output(
                    print_and_return_value, call_index=call_index
                )
                self.assertEqual(
                    print_and_return_value_output,
                    [str(accumulator)],
                )

        format_number_output: List[Any] = request.function_output(format_number)
        self.assertEqual(
            format_number_output,
            ["280"],
        )

    def test_complex_graph_produces_expected_outputs_remote(self):
        tensorlake.deploy()
        request = tensorlake.remote_run(
            test_graph_api(
                ctx=tensorlake.RequestContextPlaceholder(),
                payload=TestGraphRequestPayload(
                    numbers=[str(i) for i in range(10, 20)]
                ),
            )
        )

        format_number_output: List[Any] = request.function_output(format_number)
        self.assertEqual(
            format_number_output,
            ["280"],
        )


if __name__ == "__main__":
    unittest.main()
