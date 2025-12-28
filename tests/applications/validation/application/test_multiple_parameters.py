import unittest

from tensorlake.applications import application, function
from tensorlake.applications.validation import (
    ValidationMessage,
    validate_loaded_applications,
)


@application()
@function()
def application_function(bar: str, buzz: int) -> str:
    return bar


class TestMultipleParameters(unittest.TestCase):
    def test_passes_validation(self):
        """Multiple parameters are now supported for application functions."""
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        # No validation errors - multiple parameters are allowed
        self.assertEqual(len(validation_messages), 0)


if __name__ == "__main__":
    unittest.main()
