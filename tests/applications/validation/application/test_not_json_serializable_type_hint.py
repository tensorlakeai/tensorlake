import subprocess
import unittest
from typing import TextIO

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def application_function(foo: subprocess.Popen) -> TextIO:
    return foo


class TestNotJSONSerializableTypeHint(unittest.TestCase):
    def test_fails_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(len(validation_messages), 2)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Pydantic failed to generate CoreSchema for Application function parameter 'foo' with type hint '<class 'subprocess.Popen'>'. "
            "Please follow Pydantic documentation and the following Pydantic error message to make the type hint compatible."
            "\nPydantic error message:\nUnable to generate pydantic-core schema for <class 'subprocess.Popen'>. Set `arbitrary_types_allowed=True` "
            "in the model_config to ignore this error or implement `__get_pydantic_core_schema__` on your type to fully support it.\n\nIf you got "
            "this error by calling handler(<some type>) within `__get_pydantic_core_schema__` then you likely need to call `handler.generate_schema(<some type>)` "
            "since we do not call `__get_pydantic_core_schema__` on `<some type>` otherwise to avoid infinite recursion."
            "\n\nFor further information visit https://errors.pydantic.dev/2.12/u/schema-for-unknown-type\n",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 14)

        validation_message: ValidationMessage = validation_messages[1]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Pydantic failed to generate CoreSchema for Application function return type hint '<class 'typing.TextIO'>'. Please follow Pydantic documentation "
            "and the following Pydantic error message to make the type hint compatible.\nPydantic error message:\nUnable to generate pydantic-core schema for "
            "<class 'typing.TextIO'>. Set `arbitrary_types_allowed=True` in the model_config to ignore this error or implement `__get_pydantic_core_schema__` "
            "on your type to fully support it.\n\nIf you got this error by calling handler(<some type>) within `__get_pydantic_core_schema__` then you likely "
            "need to call `handler.generate_schema(<some type>)` since we do not call `__get_pydantic_core_schema__` on `<some type>` otherwise to avoid infinite "
            "recursion.\n\nFor further information visit https://errors.pydantic.dev/2.12/u/schema-for-unknown-type\n",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "application_function",
        )
        self.assertEqual(function_details.module_import_name, __name__)
        self.assertEqual(function_details.class_name, None)
        self.assertEqual(function_details.class_method_name, None)
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(function_details.source_file_line, 14)


if __name__ == "__main__":
    unittest.main()
