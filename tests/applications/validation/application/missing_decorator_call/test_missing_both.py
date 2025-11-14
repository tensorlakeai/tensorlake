import unittest

from tensorlake.applications import application, function
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@function
@application
def function_1(foo: int) -> int:
    return foo


class TestMissingApplicationFunctionDecoratorCall(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 2)

        messages: list[str] = []
        for validation_message in validation_messages:
            messages.append(validation_message.message)
            self.assertEqual(
                validation_message.severity,
                ValidationMessageSeverity.ERROR,
            )
            self.assertIsNotNone(validation_message.details)
            self.assertEqual(
                validation_message.details.name,
                "function_1",
            )
            self.assertEqual(validation_message.details.module_import_name, __name__)
            self.assertEqual(validation_message.details.class_name, None)
            self.assertEqual(validation_message.details.class_method_name, None)
            self.assertEqual(validation_message.details.source_file_path, __file__)
            self.assertEqual(validation_message.details.source_file_line, 11)

        self.assertIn(
            "@application decorator is missing its parenthesis. Please replace it with @application().",
            messages,
        )
        self.assertIn(
            "@function decorator is missing its parenthesis. Please replace it with @function().",
            messages,
        )


if __name__ == "__main__":
    unittest.main()
