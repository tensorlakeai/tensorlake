import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.validation import (
    ValidationMessage,
    validate_loaded_applications,
)


@cls()
class MyApplicationClass:
    @application()
    @function()
    def application_function(self) -> str:
        return "Hello, world!"


class TestNoParametersInMethod(unittest.TestCase):
    def test_all_applications_pass_validation(self):
        # Validate all test applications to ensure no false positive validation errors.
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 0)


if __name__ == "__main__":
    unittest.main()
