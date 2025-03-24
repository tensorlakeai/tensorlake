from typing import List

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.functions import FunctionCallResult, RouterCallResult
from tensorlake.functions_sdk.object_serializer import get_serializer

from ...proto.function_executor_pb2 import (
    FunctionOutput,
    Metrics,
    RouterOutput,
    RunTaskResponse,
    SerializedObject,
)


class ResponseHelper:
    """Helper class for generating RunFunctionResponse."""

    def __init__(self, task_id: str, output_encoding: str):
        self._task_id = task_id
        self._output_encoding = output_encoding

    def function_response(
        self,
        result: FunctionCallResult,
        is_reducer: bool,
        stdout: str = "",
        stderr: str = "",
    ) -> RunTaskResponse:
        if result.traceback_msg is None:
            metrics = Metrics(
                timers=result.metrics.timers,
                counters=result.metrics.counters,
            )
            return RunTaskResponse(
                task_id=self._task_id,
                function_output=self._to_function_output(
                    result.ser_outputs, self._output_encoding
                ),
                router_output=None,
                stdout=stdout,
                stderr=stderr,
                is_reducer=is_reducer,
                success=True,
                metrics=metrics,
            )
        else:
            return self.failure_response(
                message=result.traceback_msg,
                stdout=stdout,
                stderr=stderr,
            )

    def router_response(
        self,
        result: RouterCallResult,
        stdout: str = "",
        stderr: str = "",
    ) -> RunTaskResponse:
        if result.traceback_msg is None:
            metrics = Metrics(
                timers={},
                counters={},
            )
            return RunTaskResponse(
                task_id=self._task_id,
                function_output=None,
                router_output=RouterOutput(edges=result.edges),
                stdout=stdout,
                stderr=stderr,
                is_reducer=False,
                success=True,
                metrics=metrics,
            )
        else:
            return self.failure_response(
                message=result.traceback_msg,
                stdout=stdout,
                stderr=stderr,
            )

    def failure_response(
        self, message: str, stdout: str, stderr: str
    ) -> RunTaskResponse:
        stderr = "\n".join([stderr, message])
        return RunTaskResponse(
            task_id=self._task_id,
            function_output=None,
            router_output=None,
            stdout=stdout,
            stderr=stderr,
            is_reducer=False,
            success=False,
        )

    def _to_function_output(
        self, outputs: List[TensorlakeData], encoding: str
    ) -> FunctionOutput:
        output = FunctionOutput(outputs=[], output_encoding=encoding)
        for ix_data in outputs:
            serialized_object: SerializedObject = SerializedObject(
                content_type=get_serializer(ix_data.encoder).content_type,
            )
            if isinstance(ix_data.payload, bytes):
                serialized_object.bytes = ix_data.payload
            elif isinstance(ix_data.payload, str):
                serialized_object.string = ix_data.payload
            else:
                raise ValueError(f"Unsupported payload type: {type(ix_data.payload)}")

            output.outputs.append(serialized_object)
        return output
