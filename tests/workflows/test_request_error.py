import unittest

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


@tensorlake.api()
@tensorlake.function()
def start_func(cmd: str) -> str:
    if cmd == "fail_request":
        raise tensorlake.RequestError("Got command to fail the request")
    return end_func(f"start_func: {cmd}")


@tensorlake.function()
def end_func(_: str) -> str:
    return "end_func"


class TestRequestError(unittest.TestCase):
    def setUp(self):
        deploy(__file__)

    def test_expected_message(self):
        request: tensorlake.Request = tensorlake.call_remote_api(
            start_func,
            "fail_request",
        )

        try:
            output = request.output()
            self.fail(
                f"Expected RequestError from start_func, but got output: {output}"
            )
        except tensorlake.RequestError as e:
            self.assertEqual(e.message, "Got command to fail the request")


if __name__ == "__main__":
    unittest.main()
