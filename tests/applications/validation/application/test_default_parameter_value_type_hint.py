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
def function_with_mismatched_default_parameter_value(
    factor2: float = {"key": "value"},
) -> str:
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
            "Application function parameter 'factor2' has a default value that can't be serialized with the type hint of the parameter "
            "<class 'float'>. Please follow Pydantic documentation and the following SerializationError to make the default value serializable "
            "to JSON format:\nFailed to serialize '{'key': 'value'}' as '<class 'float'>' to json: 1 validation error for float\n  Input should "
            "be a valid number [type=float_type, input_value={'key': 'value'}, input_type=dict]\n    For further information visit https://errors.pydantic.dev/2.12/v/float_type\n",
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
