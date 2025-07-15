from typing import Optional

from tensorlake.functions_sdk.data_objects import TensorlakeData

from ..proto.function_executor_pb2 import TaskAllocationInput


class FunctionInput:
    def __init__(
        self,
        task_allocation_input: TaskAllocationInput,
        input: TensorlakeData,
        init_value: Optional[TensorlakeData] = None,
    ):
        self.task_allocation_input: TaskAllocationInput = task_allocation_input
        self.input: TensorlakeData = input
        self.init_value: Optional[TensorlakeData] = init_value
