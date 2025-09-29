import unittest
from typing import List

from pydantic import BaseModel

from tensorlake.applications import (
    File,
    Request,
    RequestContext,
    RequestContextPlaceholder,
    api,
    call_local_api,
    call_local_function,
    call_remote_api,
    cls,
    function,
)
from tensorlake.applications import map as tl_map
from tensorlake.applications import reduce as tl_reduce
from tensorlake.applications.remote.deploy import deploy


class TestGraphRequestPayload(BaseModel):
    numbers: List[str]


@api()
@function(cpu=1.0, memory=1.0, description="test API function")
def test_graph_api_fan_in(
    ctx: RequestContext, payload: TestGraphRequestPayload
) -> File:
    print(f"Received request with numbers: {payload.numbers}")
    ctx.state.set("numbers_count", len(payload.numbers))
    numbers = tl_map(parse_and_multiply_number, payload.numbers)
    sum = sum_numbers_fan_in(ctx, numbers, initial=0)
    return store_sum_as_file(sum)


@api()
@function(cpu=1.0, memory=1.0, description="test API function")
def test_graph_api_reduce(
    ctx: RequestContext, payload: TestGraphRequestPayload
) -> File:
    print(f"Received request with numbers: {payload.numbers}")
    ctx.state.set("numbers_count", len(payload.numbers))
    numbers = [parse_and_multiply_number(ctx, number) for number in payload.numbers]
    sum = tl_reduce(sum_numbers_reducer, numbers, 0)
    return store_sum_as_file(sum)


@function()
def parse_and_multiply_number(ctx: RequestContext, number: str) -> int:
    print(f"parsing number '{number}'")
    # Raises ValueError if not a number.
    parsed_number = int(number)
    if parsed_number % 2 == 0:
        return MultiplierFunction().multiply(ctx, number=parsed_number)
    else:
        return MultiplierFunction().multiply(ctx, number=parsed_number - 1)


@cls()
class MultiplierFunction:
    def __init__(self):
        self.multiplier: int = 2

    @function()
    def multiply(self, ctx: RequestContext, number: int) -> int:
        print(f"Multiplying number: {number}, multiplier: {self.multiplier}")
        return number * self.multiplier


@function()
def sum_numbers_reducer(
    ctx: RequestContext,
    first: int,
    second: int,
) -> int:
    print(f"adding number {second} to accumulator {first}")
    print("numbers_count from ctx: ", ctx.state.get("numbers_count"))
    return first + second


@function()
def sum_numbers_fan_in(ctx: RequestContext, numbers: List[int], initial: int) -> int:
    total: int = initial
    for number in numbers:
        print(f"adding number {number} to total {total}")
        total += number
    print("numbers_count from ctx: ", ctx.state.get("numbers_count"))
    return total


@function()
def store_sum_as_file(total: int) -> File:
    content = f"Total sum: {total}".encode("utf-8")
    content_type = "text/plain; charset=UTF-8"
    print(f"Storing file with content {content} and content type: {content_type}")
    return File(content=content, content_type=content_type)


class TestComplexGraph(unittest.TestCase):
    def test_local_function_call_of_complex_graph_produces_expected_outputs(self):
        # Any function can be called in local mode, not only API function.
        # This eases debugging for people.
        for function in [test_graph_api_reduce, test_graph_api_fan_in]:
            request: Request = call_local_function(
                function(
                    ctx=RequestContextPlaceholder(),
                    payload=TestGraphRequestPayload(
                        numbers=[str(i) for i in range(10, 20)]
                    ),
                )
            )

            file: File = request.output()
            self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
            self.assertEqual(file.content, b"Total sum: 280")

    def test_local_api_call_of_complex_graph_produces_expected_outputs(self):
        for function in ["test_graph_api_reduce", "test_graph_api_fan_in"]:
            request = call_local_api(
                function,
                TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
            )

            file: File = request.output()
            self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
            self.assertEqual(file.content, b"Total sum: 280")

    def test_remote_api_call_of_complex_graph_produces_expected_outputs(self):
        deploy(__file__)
        for function in ["test_graph_api_reduce", "test_graph_api_fan_in"]:
            request: Request = call_remote_api(
                function,
                TestGraphRequestPayload(numbers=[str(i) for i in range(10, 20)]),
            )

            file: File = request.output()
            self.assertEqual(file.content_type, "text/plain; charset=UTF-8")
            self.assertEqual(file.content, b"Total sum: 280")


if __name__ == "__main__":
    unittest.main()
