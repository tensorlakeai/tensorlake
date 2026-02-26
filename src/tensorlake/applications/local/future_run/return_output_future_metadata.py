from dataclasses import dataclass


@dataclass
class ReturnOutputFutureMetadata:
    output_serializer_name_override: str | None
    has_output_type_hint_override: bool
    output_type_hint_override: type | None
