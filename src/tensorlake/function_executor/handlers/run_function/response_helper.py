from typing import List, Optional

from tensorlake.functions_sdk.data_objects import Failure, FailureScope, TensorlakeData
from tensorlake.functions_sdk.functions import FunctionCallResult
from tensorlake.functions_sdk.object_serializer import get_serializer

from ...proto.function_executor_pb2 import FailureScope as FailureScopeProto
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
        if result.failure is None:
            metrics = Metrics(
                timers=result.metrics.timers,
                counters=result.metrics.counters,
            )
            return RunTaskResponse(
                task_id=self._task_id,
                function_output=self._to_function_output(
                    result.ser_outputs, self._output_encoding
                ),
                router_output=self._to_router_output(result.edges),
                stdout=stdout,
                stderr=stderr,
                is_reducer=is_reducer,
                success=True,
                metrics=metrics,
            )
        else:
            return self.failure_response(
                failure=result.failure,
                stdout=stdout,
                stderr=stderr,
            )

    def failure_response(
        self, failure: Failure, stdout: str, stderr: str
    ) -> RunTaskResponse:
        stderr = "\n".join([stderr, failure.trace])
        response = RunTaskResponse(
            task_id=self._task_id,
            function_output=None,
            stdout=stdout,
            stderr=stderr,
            is_reducer=False,
            success=False,
        )

        response.failure.scope = FailureScopeProto.FAILURE_SCOPE_TASK
        if failure.scope == FailureScope.InvocationArgument:
            response.failure.scope = FailureScopeProto.FAILURE_SCOPE_INVOCATION_ARGUMENT

        response.failure.cls = failure.cls
        response.failure.msg = failure.msg
        response.failure.trace = failure.trace

        return response

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

    def _to_router_output(self, edges: Optional[List[str]]) -> RouterOutput:
        if edges is None:
            return None
        return RouterOutput(edges=edges)
