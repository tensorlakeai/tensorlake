import unittest

import class_1

from tensorlake.applications import (
    application,
    cls,
    function,
)
from tensorlake.applications.function.introspect import ClassDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls()
class Class1:
    @application()
    @function()
    def method(self, _: str) -> str:
        return "Class1.method"


class TestRedefineClassInDifferentFiles(unittest.TestCase):
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
            "Class 'Class1' is defined in files: ",
            validation_message.message,
        )
        self.assertIn(
            "Classes with the same names can't be defined in different files. Please rename the classes.",
            validation_message.message,
        )
        self.assertIsNotNone(validation_message.details)
        class_details: ClassDetails = validation_message.details
        self.assertEqual(
            class_details.class_name,
            "Class1",
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
            18,
        )


if __name__ == "__main__":
    unittest.main()
