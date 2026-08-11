import math
import unittest

from tensorlake.applications import Retries, application, function
from tensorlake.applications.registry import restore_registry, snapshot_registry
from tensorlake.applications.validation import (
    ValidationMessageSeverity,
    validate_loaded_applications,
)


def invalid_configuration() -> str:
    return "invalid"


class TestFunctionConfigurationValidation(unittest.TestCase):
    def setUp(self) -> None:
        self.registry_before = snapshot_registry()
        configured_function = function(
            cpu=0,
            memory=math.nan,
            ephemeral_disk=-1,
            timeout=0,
            secrets=[""],
            gpu="H100:0",
            retries=Retries(max_retries=-1),
            region="invalid-region",  # type: ignore[arg-type]
            warm_containers=2,
            min_containers=3,
            max_containers=1,
        )(invalid_configuration)
        application(
            tags={"": "value", "invalid-value": 1},  # type: ignore[dict-item]
            retries=Retries(max_retries=11),
            region="invalid-region",  # type: ignore[arg-type]
        )(configured_function)

    def tearDown(self) -> None:
        restore_registry(self.registry_before)

    def test_rejects_configuration_outside_shared_sdk_limits(self) -> None:
        messages = validate_loaded_applications()

        self.assertTrue(messages)
        self.assertTrue(
            all(
                message.severity == ValidationMessageSeverity.ERROR
                for message in messages
            )
        )
        self.assertEqual(
            {message.message for message in messages},
            {
                "Function cpu must be a finite number greater than zero.",
                "Function memory must be a finite number greater than zero.",
                "Function ephemeral_disk must be a finite number greater than zero.",
                "Function timeout must be an integer between 1 and 86400 seconds.",
                "Function secrets must be a list of non-empty strings.",
                "Function gpu must be a GPU model or list of GPU models, optionally followed by a positive integer count.",
                "Function region must be 'us-east-1', 'eu-west-1', or None.",
                "Function min_containers cannot exceed max_containers.",
                "Function warm_containers cannot exceed max_containers.",
                "Function retries max_retries must be an integer between 0 and 10.",
                "Application retries max_retries must be an integer between 0 and 10.",
                "Application tags require non-empty string keys and string values.",
                "Application region must be 'us-east-1', 'eu-west-1', or None.",
            },
        )


if __name__ == "__main__":
    unittest.main()
