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
def function_with_default_parameter_value(factor: float = 1.0) -> str:
    return f"Factor is {factor}"


@application()
@function()
def function_with_mismatched_default_parameter_value(factor2: float = "1.0") -> str:
    # The default value type (str) doesn't match the type hint (float).
    return f"Factor is {factor2}"


class TestDefaultParameterValueTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            f"Application function parameter 'factor2' has a default value that can't be serialized with "
            f"the type hint of the parameter {float}. "
            f"Please follow Pydantic documentation and the following Pydantic error message to make the default value serializable "
            "to JSON format.\nPydantic error message:\n{e}\n",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "function_with_mismatched_default_parameter_value",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 18)


if __name__ == "__main__":
    unittest.main()
