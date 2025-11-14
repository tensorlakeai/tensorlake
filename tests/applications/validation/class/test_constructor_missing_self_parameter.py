import unittest

from tensorlake.applications import application, cls, function
from tensorlake.applications.function.introspect import ClassDetails
from tensorlake.applications.validation import (
    ValidationMessage,
    ValidationMessageSeverity,
    validate_loaded_applications,
)


@cls()
class MyClass:
    def __init__() -> None:
        self.param = param

    @application()
    @function()
    def my_method(self, payload: int) -> None:
        pass


class TestConstructorMissingSelfParameter(unittest.TestCase):
    def test_applications_fail_validation(self):
        validation_messages: list[ValidationMessage] = validate_loaded_applications()
        self.assertTrue(
            len(validation_messages) == 1,
        )

        validation_message: ValidationMessage = validation_messages[0]
        self.assertEqual(
            validation_message.severity,
            ValidationMessageSeverity.ERROR,
        )
        self.assertEqual(
            validation_message.message,
            "'MyClass.__init__' is missing 'self' parameter. Please add 'self' parameter.",
        )
        self.assertIsNotNone(validation_message.details)
        class_details: ClassDetails = validation_message.details
        self.assertEqual(
            class_details.class_name,
            "MyClass",
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
            12,
        )


if __name__ == "__main__":
    unittest.main()
