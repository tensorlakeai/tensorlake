import unittest

import parameterized

from tensorlake.applications import (
    SDKUsageError,
    function,
)
from tensorlake.applications.applications import run_application


@function()
def regular_function(foo: int) -> int:
    return foo


class TestRunRegularFunctionAsApplication(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_raises_sdk_usage_error(self, _: str, is_remote: bool):
        with self.assertRaises(SDKUsageError) as assert_context:
            run_application(regular_function, is_remote, 1)
        self.assertEqual(
            str(assert_context.exception),
            "Tensorlake Function 'regular_function' is not an application function and cannot be run as an application. "
            "To make it an application function, add @application() decorator to it.",
        )


if __name__ == "__main__":
    unittest.main()
