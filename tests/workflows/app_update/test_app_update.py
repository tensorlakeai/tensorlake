import unittest
from importlib import reload

import update_code_v1
import update_code_v2

import tensorlake.workflows.interface as tensorlake
from tensorlake.workflows.remote.deploy import deploy


class TestApplicationUpdate(unittest.TestCase):
    def test_running_request_gets_updated_to_new_code_version(self):
        # Updates the version randomly.
        tensorlake.define_application(
            "TestApplicationUpdate.test_running_request_gets_updated_to_new_code_version"
        )

        # Update functions to v1.
        reload(update_code_v1)

        deploy(__file__, upgrade_running_requests=False)

        # The request is sleeping in start_func.
        request_v1: tensorlake.Request = tensorlake.call_remote_api(
            "update_code_start_func", 10
        )

        # Update the app version to a new random value.
        tensorlake.define_application(
            "TestApplicationUpdate.test_running_request_gets_updated_to_new_code_version"
        )
        # Update functions to v2.
        reload(update_code_v2)

        deploy(__file__, upgrade_running_requests=True)

        # The request should be updated by Server to call the updated graph version with v2 update_code_end_func
        # which returns a different value than v1.
        end_func_output: str = request_v1.output()
        self.assertEqual(end_func_output, "update_code_end_func_v2")

    def test_running_request_doesnt_get_updated_to_new_code_version(self):
        # Updates the version randomly.
        tensorlake.define_application(
            "TestApplicationUpdate.test_running_request_doesnt_get_updated_to_new_code_version"
        )
        # Update functions to v1.
        reload(update_code_v1)

        deploy(__file__, upgrade_running_requests=False)

        # The request is sleeping in start_func.
        request_v1: tensorlake.Request = tensorlake.call_remote_api(
            "update_code_start_func", 10
        )

        # Update the app version to a new random value.
        tensorlake.define_application(
            "TestApplicationUpdate.test_running_request_doesnt_get_updated_to_new_code_version"
        )
        # Update functions to v2.
        reload(update_code_v2)

        deploy(__file__, upgrade_running_requests=False)

        # The request should not be updated by Server so the request should still call v1 update_code_end_func.
        end_func_output: str = request_v1.output()
        self.assertEqual(end_func_output, "update_code_end_func_v1")


if __name__ == "__main__":
    unittest.main()
