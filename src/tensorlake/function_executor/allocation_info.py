from dataclasses import dataclass

from .allocation_runner.allocation_runner import AllocationRunner
from .proto.function_executor_pb2 import Allocation


@dataclass
class AllocationInfo:
    """Tracks information about an allocation inside FunctionExecutor."""

    allocation: Allocation
    runner: AllocationRunner
