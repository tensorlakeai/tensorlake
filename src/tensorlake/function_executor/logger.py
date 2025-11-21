import io
import json
import sys
import traceback
from enum import Enum
from typing import Any, Dict

from .cloud_events import new_cloud_event

# Logger with interface similar to structlog library.
# We need a separate logging library to make sure that no customer code is using it so
# we don't leak customer data into FE logs. The FE logs are currently logged to stdout
# and are augmented with context information which allows separating them from customer logs.


class FunctionExecutorLogger:
    def __init__(self, context: Dict[str, Any], log_file: io.TextIOWrapper):
        self._context: Dict[str, Any] = context
        self._log_file: io.TextIOWrapper = log_file

    @classmethod
    def get_logger(cls, **kwargs) -> "FunctionExecutorLogger":
        """Gets the root logger with the given context.

        Doesn't raise any exceptions.
        """
        return FunctionExecutorLogger(
            context=kwargs,
            log_file=sys.stdout,
        )

    def bind(self, **kwargs) -> "FunctionExecutorLogger":
        """Binds additional context to the logger.

        Doesn't raise any exceptions.
        """
        context = self._context.copy()
        context.update(kwargs)
        return FunctionExecutorLogger(context=context, log_file=self._log_file)

    def info(self, message: str, **kwargs):
        """Logs an info level message.

        Doesn't raise any exceptions.
        """
        self._log("info", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Logs an error level message.

        Doesn't raise any exceptions.
        """
        self._log("error", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Logs a debug level message.

        Doesn't raise any exceptions.
        """
        self._log("debug", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Logs a warning level message.

        Doesn't raise any exceptions.
        """
        self._log("warning", message, **kwargs)

    def _log(self, level: str, message: str, **kwargs):
        """Logs a message with the given level and context.

        Doesn't raise any exceptions.
        """
        formatted_message = self._format_message(level, message, **kwargs)
        self._log_file.write(formatted_message + "\n")

    def _format_message(self, level: str, message: str, **kwargs) -> str:
        """Formats the log message with context and additional key-value pairs.

        The format is the same json format as structlog uses.
        Internal FE logger must be absolutely reliable.
        Especially given that it's called in most exception handling code paths.
        Doesn't raise any exceptions.
        """
        context: Dict[str, Any] = self._context.copy()
        context.update(kwargs)
        context["level"] = level
        context["event"] = message

        if "exc_info" in context:
            context["exception"] = "".join(
                traceback.format_exception(context["exc_info"])
            )
            del context["exc_info"]

        # Convert non json-serializable values to strings.
        for key, value in context.items():
            if not isinstance(
                value, (str, int, float, bool, type(None), list, dict, tuple, Enum)
            ):
                context[key] = str(value)

        try:
            return json.dumps(
                new_cloud_event(context, source="/tensorlake/function_executor/logger")
            )
        except Exception as e:
            print(
                "Failed to serialize Function Executor internal log message to JSON",
                "original context:",
                context,
                "exception:",
                str(e),
            )
