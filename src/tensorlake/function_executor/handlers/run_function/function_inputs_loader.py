from typing import Optional, Union

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...proto.function_executor_pb2 import (
    RunTaskRequest,
    SerializedObject,
    SerializedObjectEncoding,
)


class FunctionInputs:
    def __init__(
        self, input: TensorlakeData, init_value: Optional[TensorlakeData] = None
    ):
        self.input = input
        self.init_value = init_value


class FunctionInputsLoader:
    def __init__(self, request: RunTaskRequest):
        self._request = request

    def load(self) -> FunctionInputs:
        return FunctionInputs(
            input=self._function_input(),
            init_value=self._accumulator_input(),
        )

    def _function_input(self) -> TensorlakeData:
        return _to_tensorlake_data(
            self._request.graph_invocation_id, self._request.function_input
        )

    def _accumulator_input(self) -> Optional[TensorlakeData]:
        return (
            _to_tensorlake_data(
                self._request.graph_invocation_id, self._request.function_init_value
            )
            if self._request.HasField("function_init_value")
            else None
        )


def _to_tensorlake_data(
    input_id: str, serialized_object: SerializedObject
) -> TensorlakeData:
    data: Union[str, bytes] = None
    encoder: str = None
    if (
        serialized_object.encoding
        == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE
    ):
        data = serialized_object.data
        encoder = CloudPickleSerializer.encoding_type
    elif (
        serialized_object.encoding
        == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON
    ):
        data = serialized_object.data.decode("utf-8")
        encoder = JsonSerializer.encoding_type
    else:
        raise ValueError(
            f"Unsupported serialized object encoding: {SerializedObjectEncoding.Name(serialized_object.encoding)}"
        )

    return TensorlakeData(
        input_id=input_id,
        payload=data,
        encoder=encoder,
    )
