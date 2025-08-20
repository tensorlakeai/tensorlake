from typing import Any, Dict, List

from ..function_call import function_return_type_hint
from ..interface.file import File
from ..interface.function import Function
from ..interface.request import Request
from ..registry import get_function
from ..user_data_serializer import UserDataSerializer, serializer_by_name


class LocalRequest(Request):
    def __init__(self, id: str, function_output_values: Dict[str, List[bytes | File]]):
        super().__init__(id)
        # Function name -> serialized output values.
        self._function_output_values: Dict[str, List[bytes | File]] = (
            function_output_values
        )

    def function_outputs(self, function: str | Function) -> List[Any]:
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
            function_output_values: List[bytes] = self._function_output_values[
                function_name
            ]
        except KeyError:
            raise ValueError(f"Function value outputs not found for {function_name}")
        except IndexError:
            raise ValueError(f"Function value outputs not found for {function_name}")

        try:
            function_output_serializer: UserDataSerializer = serializer_by_name(
                function.function_config.output_serializer
            )
            outputs: List[Any] = []
            for value in function_output_values:
                if isinstance(value, File):
                    outputs.append(value.content)
                else:
                    outputs.append(
                        function_output_serializer.deserialize(
                            value, function_return_type_hint(function)
                        )
                    )

            return outputs
        except Exception as e:
            raise ValueError(
                f"Failed to deserializer function output value for {function_name} with serializer name {function.function_config.output_serializer}"
            ) from e
