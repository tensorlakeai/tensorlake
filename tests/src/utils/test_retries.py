import unittest
from unittest.mock import ANY, Mock, patch

from tensorlake.utils.retries import exponential_backoff


class TestExponentialBackoff(unittest.TestCase):
    def test_successful_execution(self):
        @exponential_backoff(retryable_exceptions=(ValueError,))
        def always_succeeds():
            return "Success"

        result = always_succeeds()
        self.assertEqual(result, "Success")

    def test_retry_on_exception(self):
        mock_func = Mock(side_effect=[ValueError("Fail"), "Success"])

        @exponential_backoff(retryable_exceptions=(ValueError,))
        def flaky_function():
            return mock_func()

        result = flaky_function()
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 2)

    @patch("time.sleep", return_value=None)
    def test_max_retries_exceeded(self, mock_sleep):
        mock_func = Mock(side_effect=ValueError("Fail"))

        @exponential_backoff(retryable_exceptions=(ValueError,), max_retries=3)
        def always_fails():
            return mock_func()

        with self.assertRaises(ValueError):
            always_fails()
        self.assertEqual(mock_func.call_count, 3)

    @patch("time.sleep", return_value=None)
    def test_is_retryable(self, mock_sleep):
        mock_func = Mock(side_effect=ValueError("Fail"))

        @exponential_backoff(
            retryable_exceptions=(ValueError,),
            max_retries=3,
            is_retryable=lambda e: False,
        )
        def always_fails():
            return mock_func()

        with self.assertRaises(ValueError):
            always_fails()
        self.assertEqual(mock_func.call_count, 1)

    @patch("time.sleep", return_value=None)
    def test_on_retry_callback(self, mock_sleep):
        fail_exception = ValueError("Fail")
        mock_func = Mock(side_effect=[fail_exception, "Success"])
        mock_callback = Mock()

        @exponential_backoff(retryable_exceptions=(ValueError,), on_retry=mock_callback)
        def flaky_function():
            return mock_func()

        result = flaky_function()
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 2)
        self.assertEqual(mock_callback.call_count, 1)
        mock_callback.assert_called_with(fail_exception, ANY, 1)


if __name__ == "__main__":
    unittest.main()
