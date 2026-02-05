import os
import unittest

import validate_all_applications

from tensorlake.applications import (
    Request,
    application,
    cls,
    function,
    run_remote_application,
)
from tensorlake.applications.remote.deploy import deploy_applications

# Makes the test case discoverable by unittest framework.
ValidateAllApplicationsTest: unittest.TestCase = validate_all_applications.define_test()

# The tests in this file verify publicly stated performance critical behaviors of Tensorlake Applications.

cached_pipe_in_fd: int | None = None
cached_pipe_out_fd: int | None = None


@application()
@function()
def fd_caching_function(action: str) -> str:
    global cached_pipe_in_fd
    global cached_pipe_out_fd

    if action == "create_fd":
        cached_pipe_in_fd, cached_pipe_out_fd = os.pipe()
    elif action == "write_fd":
        if cached_pipe_out_fd is None:
            raise ValueError("cached_pipe_out_fd is None")
        os.write(cached_pipe_out_fd, "write_to_cacheable_fd\n".encode())
    elif action == "read_fd":
        if cached_pipe_in_fd is None:
            raise ValueError("cached_pipe_in_fd is None")
        return os.read(cached_pipe_in_fd, 1024).decode()
    else:
        raise ValueError("Invalid action")

    return "success"


class TestFileDescriptorCaching(unittest.TestCase):
    def setUp(self) -> None:
        deploy_applications(__file__)

    def test_second_write_goes_to_cached_file_descriptor_if_same_func(self):
        request: Request = run_remote_application(fd_caching_function, "create_fd")
        output: str = request.output()
        self.assertEqual(output, "success")

        # Fails if the file descriptor is not cached between different invocations of
        # the same function version. File descriptor caching is required to e.g. not
        # load a model into GPU on each invocation.
        request = run_remote_application(fd_caching_function, "write_fd")
        output: str = request.output()
        self.assertEqual(output, "success")

        # Fail if the write to the cached file descriptor didn's happen for any reason.
        # This verifies that the file descriptor state is not altered between invocations
        # of the same function version.
        request = run_remote_application(fd_caching_function, "read_fd")
        output: str = request.output()
        self.assertEqual(output, "write_to_cacheable_fd\n")


@cls()
class FunctionClass:
    def __init__(self):
        global function_class_constructor_calls
        function_class_constructor_calls += 1

        global cached_function_class_instance
        cached_function_class_instance = self

    @application()
    @function()
    def run(self, action: str) -> str:
        global function_class_constructor_calls
        global cached_function_class_instance

        if action == "check":
            if function_class_constructor_calls != 1:
                raise ValueError(
                    f"FunctionClass constructor called {function_class_constructor_calls} times"
                )
            if cached_function_class_instance is None:
                raise ValueError("cached_function_class_instance is None")
            if cached_function_class_instance is not self:
                raise ValueError(
                    "cached_function_class_instance is not the currently running object"
                )
        else:
            raise ValueError("Invalid action")

        return "success"


function_class_constructor_calls: int = 0
cached_function_class_instance: FunctionClass | None = None


class TestFunctionClassInstanceCaching(unittest.TestCase):
    def setUp(self):
        deploy_applications(__file__)

    def test_function_class_instance_caching(self):
        # Run many times to ensure that the behavior is repeatable over many invocations.
        for i in range(5):
            # The object is created once and cached in memory.
            request: Request = run_remote_application(FunctionClass.run, "check")
            output: str = request.output()
            self.assertEqual(output, "success")

            # Every new request will reuse the cached compute object.
            request = run_remote_application(FunctionClass.run, "check")
            output = request.output()
            self.assertEqual(output, "success")


if __name__ == "__main__":
    unittest.main()
