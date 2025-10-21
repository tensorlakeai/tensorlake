from dataclasses import dataclass

from tensorlake.applications.metadata import ValueMetadata


@dataclass
class SerializedValue:
    metadata: ValueMetadata | None
    data: bytes
