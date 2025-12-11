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


class InternalLogger:
    class LOG_FILE(Enum):
        STDOUT = 1
        STDERR = 2
        NULL = 3

    """Picklable internal logger for use in FE and SDK."""

    def __init__(self, context: Dict[str, Any], destination: LOG_FILE):
        self._context: Dict[str, Any] = context
        self._destination: InternalLogger.LOG_FILE = destination
        self._log_file: io.TextIOWrapper | None = None

        if destination == InternalLogger.LOG_FILE.STDOUT:
            self._log_file = sys.stdout
        elif destination == InternalLogger.LOG_FILE.STDERR:
            self._log_file = sys.stderr

    def __getstate__(self):
        """Get the state for pickling."""
        # This is called when i.e. user creates a new subprocess to capture the logger state for pickling.
        # When a user creates a new child thread, this is not called.
        return {
            "context": self._context,
            "destination": self._destination,
        }

    def __setstate__(self, state: dict[str, Any]):
        """Set the state for unpickling."""
        self.__init__(
            context=state["context"],
            destination=state["destination"],
        )

    @classmethod
    def get_logger(cls, **kwargs) -> "InternalLogger":
        """Gets the root logger with the given context.

        Doesn't raise any exceptions.
        """
        return InternalLogger(
            context=kwargs,
            destination=InternalLogger.LOG_FILE.STDOUT,
        )

    def bind(self, **kwargs) -> "InternalLogger":
        """Binds additional context to the logger.

        Doesn't raise any exceptions.
        """
        context = self._context.copy()
        context.update(kwargs)
        return InternalLogger(context=context, destination=self._destination)

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

        Doesn't raise any exceptions as Internal FE logger must be absolutely reliable.
        Especially given that it's called in most exception handling code paths.
        """
        if self._log_file is None:
            return

        try:
            formatted_message: str = self._format_message(level, message, **kwargs)
            self._log_file.write(formatted_message + "\n")
            self._log_file.flush()
        except Exception as e:
            # This can easily happen if i.e. a message context in kwargs is not json-serializable.
            try:
                print(
                    "Failed to log internal logger message",
                    message,
                    "context",
                    str(kwargs),
                    "exception:",
                    str(e),
                    flush=True,
                )
            except Exception:
                # Fallback in case the print fallback failed.
                print("Internal log message context is lost", message, flush=True)

    def _format_message(self, level: str, message: str, **kwargs) -> str:
        """Formats the log message with context and additional key-value pairs.

        The format is the same json format as structlog uses.
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

        return json.dumps(
            new_cloud_event(context, source="/tensorlake/function_executor/logger")
        )
