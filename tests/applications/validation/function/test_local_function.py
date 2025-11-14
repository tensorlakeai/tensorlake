import unittest

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


class TestLocalFunction(unittest.TestCase):
    def test_fails_validation(self):
        @function()
        @application()
        def application_function(param: int) -> int:
            return param

        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Only module level functions are supported. Please move the function to module level by i.e. "
            "moving it outside of the function where it is defined.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "TestLocalFunction.test_fails_validation.<locals>.application_function",
        )
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 14)


if __name__ == "__main__":
    unittest.main()
