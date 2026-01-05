import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def start_func(cmd: str) -> str:
    if cmd == "fail_request":
        raise RequestError("Got command to fail the request")
    return end_func(f"start_func: {cmd}")


@function()
def end_func(_: str) -> str:
    return "end_func"


class TestRequestError(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_expected_exception(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            start_func,
            is_remote,
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
