import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls()
class ApplicationClass:
    @application()
    @function()
    def application_function(self, bar: str) -> str:
        return bar

    @function()
    def regular_function(bar, foo: int) -> int:
        return foo


class TestMethodWithMisspeledSelfParameter(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Function is a class method so it needs a 'self' parameter. Please add 'self' as the first parameter of the function.",
            validation_message.message,
        )
        self.assertIsNotNone(validation_message.details)
        self.assertEqual(
            validation_message.details.name,
            "ApplicationClass.regular_function",
        )
        self.assertEqual(validation_message.details.module_import_name, __name__)
        self.assertEqual(validation_message.details.class_name, "ApplicationClass")
        self.assertEqual(
            validation_message.details.class_method_name, "regular_function"
        )
        self.assertEqual(validation_message.details.source_file_path, __file__)
        self.assertEqual(validation_message.details.source_file_line, 18)


if __name__ == "__main__":
    unittest.main()
