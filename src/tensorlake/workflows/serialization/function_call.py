from typing import Dict, List

from pydantic import BaseModel, ConfigDict

from ..interface.function_call import FunctionCall
from .user_data_serializer import UserDataSerializer


class SerializableFunctionCall(BaseModel):
    class_name: str | None
    function_name: str
    args: List[bytes]
    kwargs: Dict[str, bytes]

    # Serialize binary user data as base64 encoded json strings.
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")

    @classmethod
    def from_function_call(
        cls, function_call: FunctionCall, user_serializer: UserDataSerializer
    ) -> "SerializableFunctionCall":
        return cls(
            class_name=function_call.class_name,
            function_name=function_call.function_name,
            args=[user_serializer.serialize(arg) for arg in function_call.args],
            kwargs={
                k: user_serializer.serialize(v) for k, v in function_call.kwargs.items()
            },
        )

    def to_function_call(self, user_serializer: UserDataSerializer) -> FunctionCall:
        return FunctionCall(
            class_name=self.class_name,
            function_name=self.function_name,
            args=[user_serializer.deserialize(arg) for arg in self.args],
            kwargs={k: user_serializer.deserialize(v) for k, v in self.kwargs.items()},
        )

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    def deserialize(data: bytes) -> "SerializableFunctionCall":
        return SerializableFunctionCall.model_validate_json(data.decode("utf-8"))
