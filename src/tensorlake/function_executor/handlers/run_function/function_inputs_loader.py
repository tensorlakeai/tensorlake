import time
from typing import Any, Optional, Union

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...blob_store.blob_store import BLOBStore
from ...proto.function_executor_pb2 import (
    RunTaskRequest,
    SerializedObjectEncoding,
)


class FunctionInputs:
    def __init__(
        self, input: TensorlakeData, init_value: Optional[TensorlakeData] = None
    ):
        self.input = input
        self.init_value = init_value


class FunctionInputsLoader:
    def __init__(self, request: RunTaskRequest, blob_store: BLOBStore, logger: Any):
        self._request: RunTaskRequest = request
        self._blob_store: BLOBStore = blob_store
        self._logger: Any = logger.bind(module=__name__)

    def load(self) -> FunctionInputs:
        start_time = time.monotonic()
        self._logger.info("downloading function inputs")
        function_input: TensorlakeData = self._function_input()
        init_value: Optional[TensorlakeData] = self._accumulator_input()
        self._logger.info(
            "function inputs downloaded",
            duration_sec=time.monotonic() - start_time,
        )

        return FunctionInputs(
            input=function_input,
            init_value=init_value,
        )

    def _function_input(self) -> TensorlakeData:
        data: bytes = self._blob_store.get(
            uri=self._request.function_input.blob.uri,
            offset=self._request.function_input.offset,
            size=self._request.function_input.manifest.size,
            logger=self._logger,
        )
        return _to_tensorlake_data(
            input_id=self._request.allocation_id,
            encoding=self._request.function_input.manifest.encoding,
            data=data,
        )

    def _accumulator_input(self) -> Optional[TensorlakeData]:
        if not self._request.HasField("function_init_value"):
            return None

        data: bytes = self._blob_store.get(
            uri=self._request.function_init_value.blob.uri,
            offset=self._request.function_init_value.offset,
            size=self._request.function_init_value.manifest.size,
            logger=self._logger,
        )

        return _to_tensorlake_data(
            input_id=self._request.allocation_id,
            encoding=self._request.function_init_value.manifest.encoding,
            data=data,
        )


def _to_tensorlake_data(
    input_id: str, encoding: SerializedObjectEncoding, data: bytes
) -> TensorlakeData:
    data: Union[str, bytes]
    encoder: str = None
    if encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_BINARY_PICKLE:
        encoder = CloudPickleSerializer.encoding_type
    elif encoding == SerializedObjectEncoding.SERIALIZED_OBJECT_ENCODING_UTF8_JSON:
        data = data.decode("utf-8")  # str
        encoder = JsonSerializer.encoding_type
    else:
        raise ValueError(
            f"Unsupported serialized object encoding: {SerializedObjectEncoding.Name(encoding)}"
        )

    return TensorlakeData(
        input_id=input_id,
        payload=data,
        encoder=encoder,
    )
