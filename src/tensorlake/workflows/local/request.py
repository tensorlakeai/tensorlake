from typing import Any, Dict, List

from ..interface.function import Function
from ..interface.request import Request
from ..registry import get_function
from ..serialization.function_call import SerializableFunctionCall
from ..serialization.function_output import FunctionOutput, FunctionOutputType
from ..serialization.user_data_serializer import UserDataSerializer, serializer_by_name


class LocalRequest(Request):
    def __init__(self, id: str, outputs: Dict[str, List[List[bytes]]]):
        super().__init__(id)
        # Function name -> list of serialized FunctionOutput.
        self._outputs = outputs

    def function_output(self, function: str | Function, call_index: int = 0) -> Any:
        if isinstance(function, str):
            function_name: str = function
        elif isinstance(function, Function):
            function_name = function._function_config.function_name
        else:
            raise TypeError(
                f"Function must be a string or a @tensorlake_function, got {type(function)}"
            )

        try:
            function: Function = get_function(function_name)
            serialized_outputs: List[bytes] = self._outputs[function_name][call_index]
        except KeyError:
            raise ValueError(f"Function output not found for {function_name}")
        except IndexError:
            raise ValueError(
                f"Function output not found for {function_name} at call index {call_index}"
            )

        try:
            user_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.output_serializer
            )
            return [
                self._deserialize_function_output(user_serializer, so)
                for so in serialized_outputs
            ]
        except Exception as e:
            raise ValueError(
                f"Failed to deserializer function output for {function_name} with serializer name {function.function_config.output_serializer}"
            ) from e

    def _deserialize_function_output(
        self, user_serializer: UserDataSerializer, serialized_output: bytes
    ) -> FunctionOutput:
        function_output: FunctionOutput = FunctionOutput.deserialize(serialized_output)
        if function_output.type == FunctionOutputType.VALUE:
            return user_serializer.deserialize(function_output.value)
        elif function_output.type == FunctionOutputType.FUNCTION_CALL:
            function_call: SerializableFunctionCall = function_output.function_call
            called_function: Function = get_function(function_call.function_name)
            called_function_input_serializer = serializer_by_name(
                called_function.function_config.input_serializer
            )
            return function_call.to_function_call(called_function_input_serializer)
        else:
            raise ValueError(f"Unknown function output type: {function_output.type}")
