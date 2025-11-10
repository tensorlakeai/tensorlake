import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls()
def function_1():
    pass


@application()
@function()
def function_2(payload: int) -> None:
    pass


class TestDecoratorAppliedToFunction(unittest.TestCase):
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
            "@cls() is applied to function. Please use @cls() only on classes.",
        )
        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "function_1",
        )
        self.assertIsNone(function_details.class_name)
        self.assertIsNone(function_details.class_method_name)
        self.assertEqual(
            function_details.module_import_name,
            __name__,
        )
        self.assertEqual(
            function_details.source_file_path,
            __file__,
        )
        self.assertEqual(
            function_details.source_file_line,
            12,
        )


if __name__ == "__main__":
    unittest.main()
