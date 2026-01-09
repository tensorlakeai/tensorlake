import unittest

from tensorlake.applications.validation import validate_loaded_applications


def define_test() -> unittest.TestCase:
    class TestAllApplicationsAreValid(unittest.TestCase):
        def test_all_applications_are_valid(self):
            # Validate all test applications to ensure no false positive validation errors.
            self.assertEqual(validate_loaded_applications(), [])

    return TestAllApplicationsAreValid
