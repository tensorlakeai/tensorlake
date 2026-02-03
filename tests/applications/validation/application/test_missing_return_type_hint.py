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
def application_function(bar: str):
    return bar


class TestMissingReturnTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.INFO,
        )
        self.assertEqual(
            "It is recommended to add a return type hint for the application function. "
            "This helps to ensure that the returned value is properly serialized and that it matches the type hint. "
            "This also helps generating a JSON schema for the return type of the function.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 12)


if __name__ == "__main__":
    unittest.main()
