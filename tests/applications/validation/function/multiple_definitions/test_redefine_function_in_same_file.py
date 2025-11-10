import unittest

import parameterized

from tensorlake.applications import (
    Request,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.applications.validation import validate_loaded_applications


@application()
@function()
def function_1(_: str) -> str:
    return "function_1"


@application()
@function()
def function_1(_: str) -> str:
    return "function_1_redefined"


class TestRedefineFunctionInSameFile(unittest.TestCase):
    def test_applications_are_valid(self):
        self.assertEqual(validate_loaded_applications(), [])

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_function_redefined_successfully(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)
        request: Request = run_application("function_1", 1, remote=is_remote)
        self.assertEqual(request.output(), "function_1_redefined")


if __name__ == "__main__":
    unittest.main()
