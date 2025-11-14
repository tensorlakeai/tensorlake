import unittest

import function_1

from tensorlake.applications import (
    application,
    function,
)
from tensorlake.applications.function.introspect import FunctionDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@application()
@function()
def function_1(_: str) -> str:
    return "function_1"


class TestRedefineFunctionInDifferentFiles(unittest.TestCase):
    def test_applications_validation_fails(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertEqual(
            len(validation_messages),
            1,
        )
        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertIn(
            "Function 'function_1' is defined in files: ",
            validation_message.message,
        )
        self.assertIn(
            "Functions with the same names can't be defined in different files. Please rename the functions.",
            validation_message.message,
        )
        self.assertIsNotNone(validation_message.details)

        function_details: FunctionDetails = validation_message.details
        self.assertEqual(
            function_details.name,
            "function_1",
        )
        self.assertEqual(
            function_details.class_name,
            None,
        )
        self.assertEqual(
            function_details.class_method_name,
            None,
        )
        self.assertEqual(function_details.source_file_path, __file__)
        self.assertEqual(
            function_details.source_file_line,
            17,
        )
        self.assertEqual(
            function_details.module_import_name,
            __name__,
        )


if __name__ == "__main__":
    unittest.main()
