import unittest

from tensorlake.applications import File, application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def valid_application_function(bar: File, buzz: File) -> File:
    pass


@application()
@function()
def not_valid_application_function(
    bar: list[File], buzz: tuple[File, str]
) -> File | None:
    pass


class TestFileTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 3)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function parameter 'bar' uses a File object in a complex type hint 'list[tensorlake.applications.interface.file.File]'. "
            "Only simple type hints like 'bar: File' are supported for File objects in application function parameters. A File object cannot be "
            "embedded inside other type hints like 'bar: list[File]', 'bar: dict[str, File]', etc or be a part of union type hint like 'bar: File | None'.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "not_valid_application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 18)

        validation_message: ValidationMessage = validation_messages[1]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function parameter 'buzz' uses a File object in a complex type hint 'tuple[tensorlake.applications.interface.file.File, str]'. "
            "Only simple type hints like 'buzz: File' are supported for File objects in application function parameters. A File object cannot be "
            "embedded inside other type hints like 'buzz: list[File]', 'buzz: dict[str, File]', etc or be a part of union type hint like 'buzz: File | None'.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "not_valid_application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 18)

        validation_message: ValidationMessage = validation_messages[2]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function return type hint 'tensorlake.applications.interface.file.File | None' uses a File object in a complex type hint. "
            "Only simple type hints like 'foo: File' are supported for File objects in application function return types. A File object cannot be "
            "embedded inside other type hints like 'foo: list[File]', 'foo: dict[str, File]', etc or be a part of union type hint like 'foo: File | None'.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "not_valid_application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 18)


if __name__ == "__main__":
    unittest.main()
