"""Strict SDK enums with explicit, tolerant response decoding."""

from enum import Enum, EnumMeta
from typing import TypeVar

ResponseEnumT = TypeVar("ResponseEnumT", bound="ResponseEnum")


class _ResponseEnumMeta(EnumMeta):
    def __new__(metacls, name, bases, namespace, **kwargs):
        cls = super().__new__(metacls, name, bases, namespace, **kwargs)
        if any(isinstance(base, _ResponseEnumMeta) for base in bases):
            if "UNKNOWN" not in cls.__members__:
                raise TypeError(f"{name} must define an UNKNOWN member")
        return cls


class ResponseEnum(str, Enum, metaclass=_ResponseEnumMeta):
    """Construct strictly; map unfamiliar response values to UNKNOWN."""

    @classmethod
    def from_response(cls: type[ResponseEnumT], value: object) -> ResponseEnumT:
        try:
            return cls(value)
        except (ValueError, TypeError):
            return cls.__members__["UNKNOWN"]

    def __str__(self) -> str:
        return self.value
