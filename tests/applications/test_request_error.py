import unittest

from tensorlake.applications import (
    Request,
    RequestError,
    api,
    call_local_api,
    call_remote_api,
    function,
)
from tensorlake.applications.remote.deploy import deploy


@api()
@function()
def start_func(cmd: str) -> str:
    if cmd == "fail_request":
        raise RequestError("Got command to fail the request")
    return end_func(f"start_func: {cmd}")


@function()
def end_func(_: str) -> str:
    return "end_func"


class TestRequestError(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_expected_message(self):
        request: Request = call_remote_api(
            start_func,
            "fail_request",
        )

        try:
            output = request.output()
            self.fail(
                f"Expected RequestError from start_func, but got output: {output}"
            )
        except RequestError as e:
            self.assertEqual(e.message, "Got command to fail the request")


if __name__ == "__main__":
    unittest.main()
