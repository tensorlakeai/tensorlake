import unittest

from tensorlake.applications import Awaitable, application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def application_function_with_simple_awaitable_type_hint() -> Awaitable:
    return other_function.awaitable()


@function()
def other_function() -> str:
    return "Hello, World!"


@application()
@function()
def application_function_with_complex_awaitable_type_hint() -> Awaitable | None:
    return None


# Tests a special scenario when users use Awaitable as a return type hint when
# they use tail calls. Give them a more friendly validation error in this case.


class TestAwaitableReturnTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 2)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Application function return type hint is an Awaitable. "
            "Instead of using Awaitable as a return type hint, please use type hint of the value returned by the Awaitable. "
            "For example, if the Awaitable (once resolved) returns str, then please use 'str' in the return type hint of "
            "the application function instead of the Awaitable.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function_with_simple_awaitable_type_hint",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 12)

        validation_message: ValidationMessage = validation_messages[1]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            f"Application function return type hint '{Awaitable | None}' uses an Awaitable object. "
            "Instead of using Awaitable in the return type hint, please use type hint of the value returned by the Awaitable. "
            "For example, if the Awaitable (once resolved) returns str, then please use 'str' in the return type hint of "
            "the application function instead of the Awaitable.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function_with_complex_awaitable_type_hint",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 23)


if __name__ == "__main__":
    unittest.main()
