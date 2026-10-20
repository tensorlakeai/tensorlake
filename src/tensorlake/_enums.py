"""Strict SDK enums with explicit, tolerant response decoding."""

from enum import Enum
from typing import TypeVar

ResponseEnumT = TypeVar("ResponseEnumT", bound="ResponseEnum")


class ResponseEnum(str, Enum):
    """Construct strictly; map unfamiliar response values to UNKNOWN."""

    @classmethod
    def from_response(cls: type[ResponseEnumT], value: object) -> ResponseEnumT:
        try:
            return cls(value)
        except (ValueError, TypeError):
            return cls.__members__["UNKNOWN"]

    def __str__(self) -> str:
        return self.value
