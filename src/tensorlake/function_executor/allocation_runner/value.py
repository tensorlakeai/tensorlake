from dataclasses import dataclass
from typing import Any

from tensorlake.applications.metadata import ValueMetadata


@dataclass
class SerializedValue:
    # None for application function call arguments (not serialized by SDK).
    metadata: ValueMetadata | None
    data: bytearray | bytes | memoryview
    # Not None if the data is using raw serialization format.
    content_type: str | None = None


@dataclass
class Value:
    # None for application function call arguments (not serialized by SDK).
    metadata: ValueMetadata | None
    object: Any
    # Index in the input argument list provided by Server.
    input_ix: int
