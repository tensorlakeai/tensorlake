from dataclasses import dataclass
from enum import Enum

from ..function.introspect import (
    ClassDetails,
    FunctionDetails,
)


class ValidationMessageSeverity(Enum):
    ERROR = 1
    WARNING = 2
    INFO = 3


@dataclass
class ValidationMessage:
    message: str
    severity: ValidationMessageSeverity
    # None if the message is not related to a specific function or class (i.e. Application level message).
    details: FunctionDetails | ClassDetails | None


def has_error_message(
    messages: list[ValidationMessage],
) -> bool:
    """Returns True if there is at least one error message in the list."""
    return any(
        message.severity == ValidationMessageSeverity.ERROR for message in messages
    )
