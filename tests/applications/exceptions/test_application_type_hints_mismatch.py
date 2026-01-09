import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Request,
    RequestFailed,
    SerializationError,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


# In this test we're only testing scenarios when actual objects accepted/returned by
# application functions do not match their type hints. The validity of type hints is
# checked during pre-deployment validation, so we don't need to cover those cases here.


@application()
@function()
def application_with_list_of_integers_arg_type_hint(l: list[int]) -> str:
    return "success"


@application()
@function()
def application_returning_object_that_mismatch_return_type_hint() -> None:
    return 23456


class TestApplicationTypeHintsMismatch(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_application_function_with_wrong_payload(self, _, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        # The request didn't start because its application payload couldn't be serialized.
        with self.assertRaises(SerializationError) as context:
            run_application(
                application_with_list_of_integers_arg_type_hint,
                is_remote,
                ["this", "is", "not", "a", "list", "of", "integers"],
            )
        self.assertIn(
            "Failed to serialize '['this', 'is', 'not', 'a', 'list', 'of', 'integers']' as 'list[int]' to json",
            str(context.exception),
        )

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_call_application_function_that_returns_wrong_value(
        self, _, is_remote: bool
    ):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            application_returning_object_that_mismatch_return_type_hint,
            is_remote,
        )
        with self.assertRaises(RequestFailed) as context:
            request.output()
        # Request and the function failed when the function returned wrong value.
        self.assertEqual(str(context.exception), "function_error")


if __name__ == "__main__":
    unittest.main()
