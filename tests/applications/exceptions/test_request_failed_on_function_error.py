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


if __name__ == "__main__":
    unittest.main()
