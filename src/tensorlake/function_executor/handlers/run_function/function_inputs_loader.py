import hashlib
import time
from typing import Optional, Union

from tensorlake.functions_sdk.data_objects import TensorlakeData
from tensorlake.functions_sdk.object_serializer import (
    CloudPickleSerializer,
    JsonSerializer,
)

from ...blob_store.blob_store import BLOBStore
from ...logger import FunctionExecutorLogger
from ...proto.function_executor_pb2 import (
    SerializedObjectEncoding,
    Task,
)


class FunctionInputs:
    def __init__(
        self, input: TensorlakeData, init_value: Optional[TensorlakeData] = None
    ):
        self.input: TensorlakeData = input
        self.init_value: Optional[TensorlakeData] = init_value


class FunctionInputsLoader:
    def __init__(
        self,
        task: Task,
        blob_store: BLOBStore,
        logger: FunctionExecutorLogger,
    ):
        self._task: Task = task
        self._blob_store: BLOBStore = blob_store
        self._logger: FunctionExecutorLogger = logger.bind(module=__name__)

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
            blob=self._task.request.function_input_blob,
            offset=self._task.request.function_input.offset,
            size=self._task.request.function_input.manifest.size,
            logger=self._logger,
        )

        data_hash: str = _sha256_hexdigest(data)
        if data_hash != self._task.request.function_input.manifest.sha256_hash:
            self._logger.error(
                "function input data hash mismatch",
                data_hash=data_hash,
                expected_hash=self._task.request.function_input.manifest.sha256_hash,
            )
            raise ValueError(
                f"Function input data hash {data_hash} does not match expected hash {self._task.request.function_input.manifest.sha256_hash}."
            )

        return _to_tensorlake_data(
            input_id=self._task.allocation_id,
            encoding=self._task.request.function_input.manifest.encoding,
            data=data,
        )

    def _accumulator_input(self) -> Optional[TensorlakeData]:
        if not self._task.request.HasField("function_init_value"):
            return None

        data: bytes = self._blob_store.get(
            blob=self._task.request.function_init_value_blob,
            offset=self._task.request.function_init_value.offset,
            size=self._task.request.function_init_value.manifest.size,
            logger=self._logger,
        )

        data_hash: str = _sha256_hexdigest(data)
        if data_hash != self._task.request.function_init_value.manifest.sha256_hash:
            self._logger.error(
                "reducer init value data hash mismatch",
                data_hash=data_hash,
                expected_hash=self._task.request.function_init_value.manifest.sha256_hash,
            )
            raise ValueError(
                f"Reducer init value data hash {data_hash} does not match expected hash {self._task.request.function_init_value.manifest.sha256_hash}."
            )

        return _to_tensorlake_data(
            input_id=self._task.allocation_id,
            encoding=self._task.request.function_init_value.manifest.encoding,
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


def _sha256_hexdigest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
