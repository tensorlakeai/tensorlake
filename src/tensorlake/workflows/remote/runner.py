from ..interface.function_call import FunctionCall
from ..interface.package import Package
from ..interface.request import Request


class RemoteRunner:
    def __init__(self, package: Package, function_call: FunctionCall):
        self.package = package
        self.function_call = function_call

    def run(self) -> Request:
        raise NotImplementedError("RemoteRunner is not implemented yet.")
