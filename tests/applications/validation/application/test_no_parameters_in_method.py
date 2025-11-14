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
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function needs to have exactly two parameters (self and request input). "
            "Please change the function parameters. Non-application functions don't have this limitation.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "MyApplicationClass.application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, "MyApplicationClass")
        self.assertEqual(function_details.class_method_name, "application_function")
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 14)


if __name__ == "__main__":
    unittest.main()
