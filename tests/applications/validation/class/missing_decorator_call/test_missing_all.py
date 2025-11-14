import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import ClassDetails, FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls
class MyClass:
    @application
    @function
    def my_method(self, payload: int) -> None:
        pass


class TestMissingDecoratorCall(unittest.TestCase):
    def test_applications_fail_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertTrue(
            len(validation_messages) == 3,
        )

        for validation_message in validation_messages:
            self.assertEqual(
                validation_message.severity,
                ValidationMessageSeverity.ERROR,
            )

            if "@cls" in validation_message.message:
                self.assertEqual(
                    validation_message.message,
                    "@cls decorator is missing its parenthesis. Please replace it with @cls().",
                )
                self.assertIsNotNone(validation_message.details)
                class_details: ClassDetails = validation_message.details
                self.assertEqual(
                    class_details.class_name,
                    "MyClass",
                )
                self.assertEqual(
                    class_details.module_import_name,
                    __name__,
                )
                self.assertEqual(
                    class_details.source_file_path,
                    __file__,
                )
                self.assertEqual(
                    class_details.source_file_line,
                    12,
                )
            elif "@application" in validation_message.message:
                self.assertEqual(
                    validation_message.message,
                    "@application decorator is missing its parenthesis. Please replace it with @application().",
                )
                self.assertIsNotNone(validation_message.details)
                details: FunctionDetails = validation_message.details
                self.assertEqual(
                    details.name,
                    "MyClass.my_method",
                )
                self.assertEqual(
                    details.class_name,
                    "MyClass",
                )
                self.assertEqual(
                    details.class_method_name,
                    "my_method",
                )
                self.assertEqual(
                    details.module_import_name,
                    __name__,
                )
                self.assertEqual(
                    details.source_file_path,
                    __file__,
                )
                self.assertEqual(
                    details.source_file_line,
                    14,
                )
            elif "@function" in validation_message.message:
                self.assertEqual(
                    validation_message.message,
                    "@function decorator is missing its parenthesis. Please replace it with @function().",
                )
                self.assertIsNotNone(validation_message.details)
                details: FunctionDetails = validation_message.details
                self.assertEqual(
                    details.name,
                    "MyClass.my_method",
                )
                self.assertEqual(
                    details.class_name,
                    "MyClass",
                )
                self.assertEqual(
                    details.class_method_name,
                    "my_method",
                )
                self.assertEqual(
                    details.module_import_name,
                    __name__,
                )
                self.assertEqual(
                    details.source_file_path,
                    __file__,
                )
                self.assertEqual(
                    details.source_file_line,
                    14,
                )
            else:
                self.fail("Unexpected validation message.")


if __name__ == "__main__":
    unittest.main()
