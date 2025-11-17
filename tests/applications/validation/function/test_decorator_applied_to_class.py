import unittest

from tensorlake.applications import application, function
from tensorlake.applications.function.introspect import ClassDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@function()
class MyFunctionClass:
    pass


@function()
@application()
def application_function(param: int) -> int:
    return param


class TestFunctionDecoratorAppliedOnClass(unittest.TestCase):
    def test_fails_validation(self):
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
            "@function() decorator is applied to class. Please use @function() only on functions and class methods.",
            validation_message.message,
        )

        self.assertIsNotNone(validation_message.details)
        class_details: ClassDetails = validation_message.details
        self.assertEqual(
            class_details.class_name,
            "MyFunctionClass",
        )
        self.assertEqual(class_details.module_import_name, __name__)
        self.assertEqual(class_details.source_file_path, __file__)
        self.assertEqual(class_details.source_file_line, 12)


if __name__ == "__main__":
    unittest.main()
