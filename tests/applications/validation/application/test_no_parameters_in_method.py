import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls()
class MyApplicationClass:
    @application()
    @function()
    def application_function(self) -> str:
        return "Hello, world!"


class TestNoParametersInMethod(unittest.TestCase):
    def test_passes_validation(self):
        """Zero parameters (excluding self) are now supported for class method application functions."""
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        # No validation errors - zero parameters are allowed
        self.assertEqual(len(validation_messages), 0)


if __name__ == "__main__":
    unittest.main()
