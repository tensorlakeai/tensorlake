from ..function.introspect import (
    ClassDetails,
    FunctionDetails,
)
from .message import ValidationMessage, ValidationMessageSeverity


def format_validation_messages(
    messages: list[ValidationMessage],
) -> list[dict]:
    """Returns the validation messages as a list of structured dicts."""
    result = []
    for message in messages:
        result.append(_format_validation_message(message))
    return result


def _format_validation_message(message: ValidationMessage) -> dict:
    severity: str = ""
    if message.severity == ValidationMessageSeverity.ERROR:
        severity = "error"
    elif message.severity == ValidationMessageSeverity.WARNING:
        severity = "warning"
    elif message.severity == ValidationMessageSeverity.INFO:
        severity = "info"

    location: str = ""
    if isinstance(message.details, FunctionDetails):
        if message.details.class_name is None:
            function: str = message.details.name
        else:
            function = (
                f"{message.details.class_name}.{message.details.class_method_name}"
            )

        location = ":".join(
            [
                message.details.module_import_name,
                str(message.details.source_file_line),
                function,
                " ",
            ]
        )
    elif isinstance(message.details, ClassDetails):
        location = ":".join(
            [
                message.details.module_import_name,
                str(message.details.source_file_line),
                message.details.class_name,
                " ",
            ]
        )

    return {"severity": severity, "message": message.message, "location": location}
