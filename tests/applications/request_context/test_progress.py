import io
import sys
import unittest
from unittest.mock import patch

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
from tensorlake.applications.request_context.progress import print_progress_update

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
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    def setUp(self):
        """Capture stdout before each test."""
        self.captured_output = io.StringIO()
        sys.stdout = self.captured_output

    def tearDown(self):
        """Restore stdout after each test."""
        sys.stdout = sys.__stdout__

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
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


class TestProgressLogging(unittest.TestCase):
    def test_progress_logging_is_best_effort_when_stdout_is_broken(self):
        with (
            patch(
                "tensorlake.applications.request_context.progress.print_cloud_event",
                side_effect=OSError("stdout is closed"),
            ),
            patch("builtins.print", side_effect=OSError("stdout is closed")),
        ):
            print_progress_update(
                request_id="request",
                function_name="function",
                function_run_id="function-run",
                allocation_id="allocation",
                current=1.0,
                total=2.0,
                message=None,
                attributes=None,
                local_mode=False,
            )


@application()
@function()
def test_update_progress_raises_expected_error(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()

    invalid_updates = [
        (
            {
                "current": values[0],
                "total": values[1],
                "message": "Updating progress",
                "attributes": {"key": 123},
            },
            "'attributes' value 123 for key 'key' needs to be a string",
        ),
        (
            {"current": -1, "total": values[1]},
            "'current' needs to be a number",
        ),
        (
            {"current": 10**1000, "total": values[1]},
            "'current' needs to be a number",
        ),
        (
            {"current": values[0], "total": values[1], "message": 123},
            "'message' needs to be a string",
        ),
        (
            {"current": values[0], "total": values[1], "attributes": ["invalid"]},
            "'attributes' needs to be a dictionary",
        ),
    ]
    for arguments, expected in invalid_updates:
        try:
            ctx.progress.update(**arguments)
        except SDKUsageError as error:
            assert expected in str(error)
        else:
            raise AssertionError(f"invalid progress update was accepted: {arguments}")
    return "success"


class TestProgressRaisesError(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        deploy_applications(__file__)

    @parameterized.parameterized.expand([("remote", True), ("local", False)])
    def test_update_progress(self, _: str, is_remote: bool):
        request: Request = run_application(
            test_update_progress_raises_expected_error, is_remote, (10, 100)
        )
        self.assertEqual(request.output(), "success")


if __name__ == "__main__":
    unittest.main()
