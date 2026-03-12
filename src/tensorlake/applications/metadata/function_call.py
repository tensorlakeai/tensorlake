from enum import Enum
from typing import Any

from pydantic import BaseModel


class FunctionCallArgumentMetadata(BaseModel):
    # ID of Future or value from which the argument value is coming from.
    value_id: str


class SPLITTER_INPUT_MODE(Enum):
    """Mode for how map/reduce splitter function receives inputs."""

    ITEM_PER_ARG = 0  # Each items is passed as a separate argument
    ITEMS_IN_ONE_ARG = 1  # All items are passed as a single list argument


class FunctionCallMetadata(BaseModel):
    # ID of the function call, uniquness guarantees depend on how the field is set.
    id: str
    # The name of the called function in the application.
    # For special function calls this is the name of the parent function which initiated the map/reduce operation.
    function_name: str
    # Not None if output serialization format is overridden for this function call.
    # This is used when the output of this function call is used as output of another function call
    # with a different output serializer.
    output_serializer_name_override: str | None
    # This is used when the output of this function call is used as output of another function call.
    # In this case the type hint of the outer function call are applied to the inner function call output.
    output_type_hint_override: Any
    has_output_type_hint_override: bool
    # Positional arg ix -> Arg metadata.
    args: list[FunctionCallArgumentMetadata]
    # Keyword Arg name -> Arg metadata.
    kwargs: dict[str, FunctionCallArgumentMetadata]
    # Special function call settings.
    is_map_splitter: bool
    is_reduce_splitter: bool
    splitter_function_name: str | None
    splitter_input_mode: SPLITTER_INPUT_MODE | None
    is_map_concat: bool
