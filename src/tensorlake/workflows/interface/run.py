from ..local.runner import LocalRunner
from ..package import get_user_defined_or_tmp_package
from ..remote.runner import RemoteRunner
from .function_call import FunctionCall
from .package import Package
from .request import Request


def local_run(function_call: FunctionCall) -> Request:
    """Run a function locally and return the request."""
    # TODO: validate the graph first.
    package: Package = get_user_defined_or_tmp_package()
    return LocalRunner(package, function_call).run()


def remote_run(function_call: FunctionCall) -> Request:
    """Run a function on remotely (i.e. on Tensorlake Cloud) and returns the request."""
    # TODO: validate the graph first.
    package: Package = get_user_defined_or_tmp_package()
    return RemoteRunner(package, function_call).run()
