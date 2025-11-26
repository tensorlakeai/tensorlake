import io
import sys
import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestFailed,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def application_function(_: str) -> str:
    raise RuntimeError("Fail!")


class TestRequestFailedRaisedOnFunctionError(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_expected_exception(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_function,
            "whatever",
            remote=is_remote,
        )
        try:
            request.output()
            self.fail("Expected RequestFailed exception")
        except RequestFailed as e:
            self.assertEqual(str(e), "function_error")

    def test_original_function_exception_is_pretty_printed_in_local_mode(self):
        # In remote mode FE prints the function exception to FE stdout so we can't
        # see it here. But in local mode the same exception should be printed to our
        # stdout.

        captured_stderr = io.StringIO()
        sys.stderr = captured_stderr

        try:
            request: Request = run_application(
                application_function,
                "magic_string",
                remote=False,
            )
            self.assertRaises(RequestFailed, request.output)
        finally:
            sys.stderr = sys.__stderr__

        stderr: str = captured_stderr.getvalue()
        self.assertIn(
            "FunctionError: Tensorlake Function Call application_function(\n"
            "  'magic_string',\n"
            ") failed due to exception: \n"
            "Traceback (most recent call last):\n",
            stderr,
        )
        self.assertIn('raise RuntimeError("Fail!")\n', stderr)
        self.assertIn("RuntimeError: Fail!", stderr)


if __name__ == "__main__":
    unittest.main()
