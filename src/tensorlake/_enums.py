"""Shared handling for SDK enums that accept future server values."""

from enum import Enum


class UnknownStrEnum(str, Enum):
    """Map unrecognized strings to the subclass's UNKNOWN member."""

    @classmethod
    def _missing_(cls, value: object):
        if isinstance(value, str):
            return cls.__members__["UNKNOWN"]
        return None

    def __str__(self) -> str:
        return self.value
