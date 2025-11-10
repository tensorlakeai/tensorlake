import unittest

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


class MyClass:
    @application()
    @function()
    def my_method(self, payload: int) -> None:
        pass


class TestNoClsDecorator(unittest.TestCase):
    def test_applications_fail_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertTrue(
            len(validation_messages) == 1,
        )

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )

        self.assertEqual(
            validation_message.message,
            "Please add @cls() decorator to class 'MyClass' where the function is defined.",
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
            13,
        )


if __name__ == "__main__":
    unittest.main()
