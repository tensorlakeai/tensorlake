import importlib
import os
import unittest

from tensorlake.applications import Function, Request, run_remote_application
from tensorlake.applications.remote.deploy import deploy_applications
from tensorlake.vendor.nanoid import generate as nanoid

# This test simulates user behavior of updating application code
# and deploying the updated code to the server.


END_FUNC_CODE_VERSION_PLACEHOLDER_NAME: str = "END_FUNC_CODE_VERSION_PLACEHOLDER"
GENERATED_CODE_FILE_NAME: str = "generated_code.py"


def update_generated_code(version: str, reload: bool = True) -> None:
    current_file_dir: str = os.path.dirname(__file__)
    template_path = os.path.join(current_file_dir, "code_template.py")
    with open(template_path, "r") as template_file:
        code_template: str = template_file.read()

    generated_code_content = code_template.replace(
        END_FUNC_CODE_VERSION_PLACEHOLDER_NAME, version
    )

    generated_code_path = os.path.join(current_file_dir, GENERATED_CODE_FILE_NAME)
    with open(generated_code_path, "w") as generated_code_file:
        generated_code_file.write(generated_code_content)

    if reload:
        importlib.reload(generated_code)
        # Hacky way to update the application version to a new random value.
        func: Function = generated_code.code_update_start_func
        func._application_config.version = version


update_generated_code(nanoid(), reload=False)
import generated_code


class TestApplicationUpdate(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        # Delete generated_code.py so it doesn't get checked into the repo.
        # We can't use .gitignore because then generated_code.py wouldn't be
        # added to application code ZIP.
        generated_code_path = os.path.join(
            os.path.dirname(__file__), GENERATED_CODE_FILE_NAME
        )
        if os.path.exists(generated_code_path):
            os.remove(generated_code_path)
        return super().tearDownClass()

    def test_running_request_gets_updated_to_new_code_version(self):
        v1 = nanoid()
        update_generated_code(v1)
        deploy_applications(__file__, upgrade_running_requests=False)

        # The request is sleeping in start_func.
        request_v1: Request = run_remote_application(
            generated_code.code_update_start_func, 10
        )

        v2 = nanoid()
        update_generated_code(v2)
        deploy_applications(__file__, upgrade_running_requests=True)

        # The request should be updated by Server to call the updated application version
        #  with code_update_end_func returning v2.
        end_func_output: str = request_v1.output()
        self.assertEqual(end_func_output, v2)

    def test_running_request_doesnt_get_updated_to_new_code_version(self):
        v1 = nanoid()
        update_generated_code(v1)
        deploy_applications(__file__, upgrade_running_requests=False)

        # The request is sleeping in start_func.
        request_v1: Request = run_remote_application(
            generated_code.code_update_start_func, 10
        )

        v2 = nanoid()
        update_generated_code(v2)
        deploy_applications(__file__, upgrade_running_requests=False)

        # The request should not be updated by Server so the request should still
        # call code_update_end_func that returns v1.
        end_func_output: str = request_v1.output()
        self.assertEqual(end_func_output, v1)


if __name__ == "__main__":
    unittest.main()
