import unittest
from typing import List

from pydantic import BaseModel

# This import will be replaced by `import tensorlake` when we switch to the new SDK UX.
import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy

tensorlake.define_application(name="Test Complex Graph Application")


class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


# The payload type hint is required for SDK to deserialize the request payload into the
# Pydantic model class. Without it payload parameter will be a dict { "numbers": [...] }.
@tensorlake.api()
@tensorlake.function(cpu=1.0, memory=1.0, description="test API function")
def test_graph_api_fan_in(
    ctx: tensorlake.RequestContext, payload: TestGraphRequestPayload
) -> tensorlake.File:
    print(f"Received request with numbers: {payload.numbers}")
    ctx.state.set("numbers_count", len(payload.numbers))
    numbers = tensorlake.map(parse_and_multiply_number, payload.numbers)
    sum = sum_numbers_fan_in(ctx, numbers, initial=0)
    return store_sum_as_file(sum)


@tensorlake.api()
@tensorlake.function(cpu=1.0, memory=1.0, description="test API function")
def test_graph_api_reduce(
    ctx: tensorlake.RequestContext, payload: TestGraphRequestPayload
) -> tensorlake.File:
    print(f"Received request with numbers: {payload.numbers}")
    ctx.state.set("numbers_count", len(payload.numbers))
    numbers = [parse_and_multiply_number(ctx, number) for number in payload.numbers]
    sum = tensorlake.reduce(sum_numbers_reducer, numbers, 0)
    return store_sum_as_file(sum)


@tensorlake.function()
def parse_and_multiply_number(ctx: tensorlake.RequestContext, number: str) -> int:
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
    def multiply(self, ctx: tensorlake.RequestContext, number: int) -> int:
        print(f"Multiplying number: {number}, multiplier: {self.multiplier}")
        return number * self.multiplier


@tensorlake.function()
def sum_numbers_reducer(
    ctx: tensorlake.RequestContext,
    first: int,
    second: int,
) -> int:
    print(f"adding number {second} to accumulator {first}")
    print("numbers_count from ctx: ", ctx.state.get("numbers_count"))
    return first + second


@tensorlake.function()
def sum_numbers_fan_in(
    ctx: tensorlake.RequestContext, numbers: List[int], initial: int
) -> int:
    total: int = initial
    for number in numbers:
        print(f"adding number {number} to total {total}")
        total += number
    print("numbers_count from ctx: ", ctx.state.get("numbers_count"))
    return total


@tensorlake.function()
def store_sum_as_file(total: int) -> tensorlake.File:
    content = f"Total sum: {total}".encode("utf-8")
    content_type = "text/plain; charset=UTF-8"
    print(f"Storing file with content {content} and content type: {content_type}")
    return tensorlake.File(content=content, content_type=content_type)


class TestComplexGraph(unittest.TestCase):
    def test_local_function_call_of_complex_graph_produces_expected_outputs(self):
        # Any function can be called in local mode, not only API function.
        # This eases debugging for people.
        for function in [test_graph_api_reduce, test_graph_api_fan_in]:
            request: tensorlake.Request = tensorlake.call_local_function(
                function(
                    ctx=tensorlake.RequestContextPlaceholder(),
                    payload=TestGraphRequestPayload(
                        numbers=[str(i) for i in range(10, 20)]
                    ),
                )
            )

            file: tensorlake.File = request.output()
            self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
            self.assertEqual(file.content, b"Total sum: 280")

    def test_local_api_call_of_complex_graph_produces_expected_outputs(self):
        for function in ["test_graph_api_reduce", "test_graph_api_fan_in"]:
            request = tensorlake.call_local_api(
                function,
                TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
            )

            file: tensorlake.File = request.output()
            self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
            self.assertEqual(file.content, b"Total sum: 280")

    def test_remote_api_call_of_complex_graph_produces_expected_outputs(self):
        # pass
        deploy(__file__)
        # TODO: Implement.
        # for function in ["test_graph_api_reduce", "test_graph_api_fan_in"]:
        #     request = tensorlake.call_remote_api(
        #         function,
        #         TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
        #     )

        # file: tensorlake.File = request.output()
        # self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
        # self.assertEqual(file.content, b"Total sum: 280")


if __name__ == "__main__":
    unittest.main()
