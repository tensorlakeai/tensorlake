import unittest

from tensorlake.applications.validation import (
    ValidationMessage,
    has_error_message,
    validate_loaded_applications,
)


def define_test() -> unittest.TestCase:
    class TestAllApplicationsAreValid(unittest.TestCase):
        def test_all_applications_are_valid(self):
            # Validate all test applications to ensure no false positive validation errors.
            self.assertEqual(validate_loaded_applications(), [])

    return TestAllApplicationsAreValid


def define_no_validation_errors_test() -> unittest.TestCase:
    class TestAllApplicationsHaveNoValidationErrors(unittest.TestCase):
        def test_all_applications_have_no_validation_errors(self):
            # Validate all test applications to ensure no validation errors.
            # Non-errors like  warnings and infos are allowed.
            validation_messages: list[ValidationMessage] = (
                validate_loaded_applications()
            )
            self.assertFalse(has_error_message(validation_messages))

    return TestAllApplicationsHaveNoValidationErrors
