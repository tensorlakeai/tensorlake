import click

from ..function.introspect import (
    ClassDetails,
    FunctionDetails,
)
from .message import ValidationMessage, ValidationMessageSeverity


def print_validation_messages(
    messages: list[ValidationMessage],
) -> None:
    """Prints the validation messages in a nice human-readable format."""
    for message in messages:
        _print_validation_message(message)


def _print_validation_message(message: ValidationMessage) -> None:
    severity: str = ""
    color: str | None = None
    if message.severity == ValidationMessageSeverity.ERROR:
        severity = "‼️  Error: "
        color = "bright_red"
    elif message.severity == ValidationMessageSeverity.WARNING:
        severity = "⚠️  Warning: "
        color = "bright_yellow"
    elif message.severity == ValidationMessageSeverity.INFO:
        severity = "ℹ️  Info: "

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

    click.echo(
        f"{severity}{location}\n{click.style(message.message, fg=color)}\n\n",
        err=(message.severity == ValidationMessageSeverity.ERROR),
        nl=False,
    )
