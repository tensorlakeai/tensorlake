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


@cls()
@function()
def function_2():
    pass


@function()
@cls()
def function_3():
    pass


@application()
@function()
def application_function(payload: int) -> None:
    pass


class TestDecoratorAppliedToFunction(unittest.TestCase):
    def test_applications_fail_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 3)

        for validation_message in validation_messages:
            validation_message: ValidationMessage
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

            if function_details.name == "function_1":
                self.assertEqual(
                    function_details.source_file_line,
                    12,
                )
            elif function_details.name == "function_2":
                self.assertEqual(
                    function_details.source_file_line,
                    17,
                )
            elif function_details.name == "function_3":
                self.assertEqual(
                    function_details.source_file_line,
                    23,
                )
            else:
                self.fail(f"Unexpected function name: {function_details.name}")

        self.assertEqual(len(set(msg.details.name for msg in validation_messages)), 3)


if __name__ == "__main__":
    unittest.main()
