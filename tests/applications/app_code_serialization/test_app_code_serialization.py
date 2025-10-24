import unittest
from typing import List

# Import from local package using absolute import path.
from hello_world.hello_world import hello_world
from hello_world.subpackage.subpackage import (
    TensorlakeFunctionSubpackageHelloWorld,
    subpackage_hello_world,
    tensorlake_function_subpackage_hello_world,
)

# Import local module using absolute import path, works because the the code dir zip is added to PYTHONPATH.
from repeat import repeat_hello_world

# Check that symlinks inside the code dir works.
from test_complex_graph_symlink import test_graph_api_fan_in

from tensorlake.applications import (
    Request,
    application,
    function,
)
from tensorlake.applications.remote.deploy import deploy_applications

# Warning: this test file is not a part of Python package so relative import from it are not possible.
# Some subdirs in this code directory are Python packages (they have __init__.py file) and some are not.
# The Python modules (files) inside Python packages can do relative imports and we test it there.

# Absolute imports work in this file because the code directory is added to sys.path both when running
# the test file as a script using poetry and when running graph code zip in Function Executor.


@application()
@function()
def call_hello_world(_: str) -> str:
    return hello_world()


@application()
@function()
def call_repeat_hello_world(times: int) -> List[str]:
    return repeat_hello_world(times)


@application()
@function()
def call_hello_world_from_subpackage(_: str) -> str:
    return subpackage_hello_world()


@application()
@function()
def function_from_symlink_is_available(_: str) -> bool:
    return True if test_graph_api_fan_in else False


@application()
@function()
def import_from_subdir_fails(_: str) -> bool:
    try:
        from hello_world.subdir.subdir import foo

        return False
    except ImportError:
        return True


class TestApplicationCodeSerialization(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        deploy_applications(__file__)

    def test_call_hello_world(self):
        request: Request = call_hello_world.remote("test")
        self.assertEqual(request.output(), hello_world())

    def test_call_repeat_hello_world(self):
        request: Request = call_repeat_hello_world.remote(3)
        self.assertEqual(request.output(), repeat_hello_world(3))

    def test_call_hello_world_from_subpackage(self):
        request: Request = call_hello_world_from_subpackage.remote("test")
        self.assertEqual(request.output(), subpackage_hello_world())

    def test_function_from_symlink_is_available(self):
        request: Request = function_from_symlink_is_available.remote("test")
        self.assertTrue(request.output())

    def test_import_from_subdir_fails(self):
        # Direct import from subdir works when this test file is directly executed by Python.
        # This is some known peculiar behavior of Python import system.
        # There's no clear explanation/reference on why it works this way.
        from hello_world.subdir.subdir import foo

        self.assertIsNotNone(foo)

        # However when this file is imported as a module, the direct import fails.
        # Test that this happens indeed.
        request: Request = import_from_subdir_fails.remote("test")
        self.assertTrue(request.output())

    def test_imported_tensorlake_function(self):
        # Check that the function imported from the module works.
        request: Request = tensorlake_function_subpackage_hello_world.remote("test")
        self.assertEqual(request.output(), subpackage_hello_world())

    def test_imported_tensorlake_class(self):
        # Check that the function imported from the module works.
        request: Request = TensorlakeFunctionSubpackageHelloWorld().run.remote("test")
        self.assertEqual(request.output(), subpackage_hello_world())


# TODO: Add test case that validates that multiprocessing works.


if __name__ == "__main__":
    unittest.main()
