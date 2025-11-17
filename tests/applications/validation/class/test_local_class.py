import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import ClassDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


class TestLocalClass(unittest.TestCase):
    def test_fails_validation(self):
        @cls()
        class MyApplication:
            @function()
            @application()
            def application_function(self, param: int) -> int:
                return param

        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        for msg in validation_messages:
            print(msg.message)
        self.assertEqual(len(validation_messages), 1)

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            "Only module level classes are supported. Please move the class to module level by i.e. "
            "moving it outside of the function where it is defined.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        class_details: ClassDetails = validation_message.details
        self.assertEqual(
            class_details.class_name,
            "TestLocalClass.test_fails_validation.<locals>.MyApplication",
        )
        self.assertEqual(class_details.module_import_name, __name__)
        self.assertEqual(class_details.source_file_path, __file__)
        self.assertEqual(class_details.source_file_line, 14)


if __name__ == "__main__":
    unittest.main()
