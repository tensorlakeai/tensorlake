import unittest
from unittest.mock import MagicMock

from tensorlake.applications.interface.exceptions import SDKUsageError
from tensorlake.applications.request_context.http_client.progress import (
    FunctionProgressHTTPClient,
)


class ProgressValidationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.progress = FunctionProgressHTTPClient(
            request_id="request",
            allocation_id="allocation",
            function_name="function",
            function_run_id="function-run",
            http_client=MagicMock(),
        )

    def test_rejects_unrepresentable_integer_values_as_sdk_usage_errors(self) -> None:
        with self.assertRaises(SDKUsageError):
            self.progress.update(10**1000, 1)
        with self.assertRaises(SDKUsageError):
            self.progress.update(1, 10**1000)


if __name__ == "__main__":
    unittest.main()
