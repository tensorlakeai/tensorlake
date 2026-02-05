import io
import sys
import unittest

import parameterized
import validate_all_applications

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.interface.exceptions import SDKUsageError
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()


@application()
@function()
def test_update_progress(values: tuple[int | float, int | float]) -> str:
    ctx: RequestContext = RequestContext.get()
    ctx.progress.update(current=values[0], total=values[1])
    return "success"


@application()
@function()
def test_update_progress_with_parameters(
    values: tuple[int | float, int | float],
) -> str:
    ctx: RequestContext = RequestContext.get()
    ctx.progress.update(
        current=values[0],
        total=values[1],
        message="Updating progress",
        attributes={"key": "value"},
    )
    return "success"


class TestProgress(unittest.TestCase):
    def setUp(self):
        """Capture stdout before each test."""
        self.captured_output = io.StringIO()
        sys.stdout = self.captured_output

    def tearDown(self):
        """Restore stdout after each test."""
        sys.stdout = sys.__stdout__

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(test_update_progress, is_remote, (10, 100))
        self.assertEqual("success", request.output())

    def test_update_progress_local_default_message(self):
        request: Request = run_application(test_update_progress, False, (12.3, 20))
        self.assertEqual("success", request.output())

        output = self.captured_output.getvalue().strip()
        self.assertTrue(
            output.startswith("Progress Update:"),
        )
        self.assertIn(
            "'function_name': 'test_update_progress'",
            output,
        )
        self.assertIn(
            "'message': 'test_update_progress: executing step 12.3 of 20'",
            output,
        )
        self.assertIn(
            "'step': 12.3,",
            output,
        )
        self.assertIn(
            "'total': 20,",
            output,
        )
        self.assertIn("'attributes': None", output)

    def test_update_progress_local_custom_message(self):
        request: Request = run_application(
            test_update_progress_with_parameters, False, (10, 100)
        )
        self.assertEqual("success", request.output())

        output = self.captured_output.getvalue().strip()
        self.assertTrue(
            output.startswith("Progress Update:"),
        )
        self.assertIn(
            "'function_name': 'test_update_progress_with_parameters'",
            output,
        )
        self.assertIn(
            "'message': 'Updating progress'",
            output,
        )
        self.assertIn(
            "'step': 10,",
            output,
        )
        self.assertIn(
            "'total': 100,",
            output,
        )
        self.assertIn("'attributes': {'key': 'value'}", output)


@application()
@function()
def test_update_progress_raises_expected_error(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()

    attributes = {"key": 123}
    try:
        ctx.progress.update(
            current=values[0],
            total=values[1],
            message="Updating progress",
            attributes=attributes,
        )
    except SDKUsageError as e:
        assert str(e) == "'attributes' value 123 for key 'key' needs to be a string"
    return "success"


class TestProgressRaisesError(unittest.TestCase):
    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        if is_remote:
            deploy_applications(__file__)

        request: Request = run_application(
            test_update_progress_raises_expected_error, is_remote, (10, 100)
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
