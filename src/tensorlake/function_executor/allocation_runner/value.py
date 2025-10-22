from dataclasses import dataclass
from typing import Any

from tensorlake.applications.metadata import ValueMetadata


@dataclass
class SerializedValue:
    metadata: ValueMetadata | None
    data: bytes
    # Not None if the data is using raw serialization format.
    content_type: str | None = None


@dataclass
class Value:
    metadata: ValueMetadata | None
    object: Any
    # Index in the input argument list provided by Server.
    input_ix: int
