import io
import sys
import unittest

import parameterized

from tensorlake.applications import (
    Request,
    RequestContext,
    application,
    function,
)
from tensorlake.applications.applications import run_application
from tensorlake.applications.remote.deploy import deploy_applications


@application()
@function()
def test_update_progress(values: tuple[int, int]) -> str:
    ctx: RequestContext = RequestContext.get()
    ctx.progress.update(current=values[0], total=values[1])
    return "success"


@application()
@function()
def test_update_progress_with_parameters(values: tuple[int, int]) -> str:
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

        request: Request = run_application(
            test_update_progress, (10, 100), remote=is_remote
        )
        self.assertEqual("success", request.output())

    def test_update_progress_local_default_message(self):
        request: Request = run_application(
            test_update_progress, (10, 100), remote=False
        )
        self.assertEqual("success", request.output())

        output = self.captured_output.getvalue().strip().split("\n")
        self.assertIn("executing step 10 of 100", output)

    def test_update_progress_local_custom_message(self):
        request: Request = run_application(
            test_update_progress_with_parameters, (10, 100), remote=False
        )
        self.assertEqual("success", request.output())

        output = self.captured_output.getvalue().strip().split("\n")
        self.assertIn(
            'executing step 10 of 100: Updating progress. {"key": "value"}', output
        )


if __name__ == "__main__":
    unittest.main()
