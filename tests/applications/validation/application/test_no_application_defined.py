import unittest

from tensorlake.applications import function
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@function()
def regular_function(bar: str) -> str:
    return bar


class TestNoApplicationDefined(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "No application function is defined. Please add at least one application function by adding @application() decorator to it.",
            validation_message.message,
        )
        self.assertIsNone(validation_message.details)


if __name__ == "__main__":
    unittest.main()
