import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


class MyFunctionClass:
    @cls()
    class MyNestedClass:
        @function()
        @application()
        def application_function(self, param: int) -> int:
            return param


class TestNestedClassMethod(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Function is defined inside a nested class. Nested classes are not supported. Please move the function to a top level class.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "MyFunctionClass.MyNestedClass.application_function",
        )
        self.assertEqual(function_details.class_name, "MyFunctionClass.MyNestedClass")
        self.assertEqual(function_details.class_method_name, "application_function")
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 15)


if __name__ == "__main__":
    unittest.main()
