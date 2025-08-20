from enum import Enum

from pydantic import BaseModel, ConfigDict

from .function_call import SerializableFunctionCall


class FunctionOutputType(Enum):
    VALUE = 1
    FUNCTION_CALL = 2


class FunctionOutput(BaseModel):
    type: FunctionOutputType
    # Not None if function output is a function call.
    function_call: SerializableFunctionCall | None
    # Not None if function output is a value.
    value: bytes | None

    # Serialize binary user data as base64 encoded json strings.
    model_config = ConfigDict(ser_json_bytes="base64", val_json_bytes="base64")

    def serialize(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def deserialize(cls, data: bytes) -> "FunctionOutput":
        json_data = data.decode("utf-8")
        return cls.model_validate_json(json_data)
