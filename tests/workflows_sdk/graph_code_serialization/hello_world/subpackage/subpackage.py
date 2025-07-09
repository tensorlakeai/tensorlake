# Import from local module from this package using absolute import path. The absolute import works because the graph
# code dir is added to PYTHONPATH. We can import the module from this dir because the dir is a Python package that has
# __init__.py file.
from hello_world.subpackage.const import (
    WORLD_NAME,
)

from tensorlake import TensorlakeCompute, tensorlake_function

# Import from parent module using relative import path. We can do this because this Python file is part of a package
# and the parent directory is a Python package too.
from ..hello_world import hello_world

# Import from local module using relative import path. We can do this because this Python file is part of a package.
from .const import WORLD_NAME as WORLD_NAME_2


def subpackage_hello_world() -> str:
    return hello_world() + " from " + WORLD_NAME + " and " + WORLD_NAME_2


@tensorlake_function(name="foo")
def tensorlale_function_subpackage_hello_world() -> str:
    return subpackage_hello_world()


class TensorlakeComputeSubpackageHelloWorld(TensorlakeCompute):
    name = "tensorlake_compute_subpackage_hello_world"

    def run(self) -> str:
        return subpackage_hello_world()
