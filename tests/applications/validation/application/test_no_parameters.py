import unittest

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def application_function() -> str:
    return "Hello, world!"


class TestNoParameters(unittest.TestCase):
    def test_passes_validation(self):
        """Zero parameters are now supported for application functions."""
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        # No validation errors - zero parameters are allowed
        self.assertEqual(len(validation_messages), 0)


if __name__ == "__main__":
    unittest.main()
