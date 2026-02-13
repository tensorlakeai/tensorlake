import unittest

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.interface.futures import Future
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def application_function_with_simple_future_type_hint() -> Future:
    return other_function.tail_call()


@function()
def other_function() -> str:
    return "Hello, World!"


@application()
@function()
def application_function_with_complex_future_type_hint() -> Future | None:
    return None


# Tests a special scenario when users use Future as a return type hint when
# they use tail calls. Give them a more friendly validation error in this case.


class TestFutureInReturnTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 2)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function return type hint is a Future. "
            "Instead of using Future as a return type hint, please use type hint of the value returned by the Future. "
            "For example, if the Future (once resolved) returns str, then please use 'str' in the return type hint of "
            "the application function instead of the Future.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function_with_simple_future_type_hint",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 13)

        validation_message: ValidationMessage = validation_messages[1]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            f"Application function return type hint '{Future | None}' uses a Future. "
            "Instead of using Future in the return type hint, please use type hint of the value returned by the Future. "
            "For example, if the Future (once resolved) returns str, then please use 'str' in the return type hint of "
            "the application function instead of the Future.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function_with_complex_future_type_hint",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 24)


if __name__ == "__main__":
    unittest.main()
