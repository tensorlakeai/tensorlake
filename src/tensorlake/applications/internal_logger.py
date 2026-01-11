import io
import json
import sys
import traceback
from enum import Enum
from typing import Any, Dict

from .cloud_events import event_time, new_cloud_event

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

    def __init__(
        self,
        context: Dict[str, Any],
        destination: LOG_FILE,
        _dict_traceback: bool = False,
        _as_cloud_event: bool = True,
    ):
        self._context: Dict[str, Any] = context
        self._destination: InternalLogger.LOG_FILE = destination
        self._dict_traceback: bool = _dict_traceback
        self._as_cloud_event: bool = _as_cloud_event
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
            "_dict_traceback": self._dict_traceback,
            "_as_cloud_event": self._as_cloud_event,
        }

    def __setstate__(self, state: dict[str, Any]):
        """Set the state for unpickling."""
        self.__init__(
            context=state["context"],
            destination=state["destination"],
            _dict_traceback=state.get("_dict_traceback", False),
            _as_cloud_event=state.get("_as_cloud_event", True),
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
        return InternalLogger(
            context=context,
            destination=self._destination,
            _dict_traceback=self._dict_traceback,
            _as_cloud_event=self._as_cloud_event,
        )

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
            exc_info = context["exc_info"]
            # Handle exc_info=True (capture current exception from sys.exc_info())
            if exc_info is True:
                exc_info = sys.exc_info()
            # Handle exc_info as an exception object
            elif isinstance(exc_info, BaseException):
                exc_info = (type(exc_info), exc_info, exc_info.__traceback__)

            if exc_info and exc_info != (None, None, None):
                if self._dict_traceback:
                    context["exception"] = self._format_exception_dict(exc_info)
                else:
                    context["exception"] = "".join(
                        traceback.format_exception(*exc_info)
                    )
            del context["exc_info"]

        # Convert non json-serializable values to strings.
        for key, value in context.items():
            if not isinstance(
                value, (str, int, float, bool, type(None), list, dict, tuple, Enum)
            ):
                context[key] = str(value)

        if self._as_cloud_event:
            return json.dumps(
                new_cloud_event(context, source="/tensorlake/function_executor/logger")
            )
        else:
            context["timestamp"] = event_time()
            return json.dumps(context)

    def _format_exception_dict(self, exc_info: tuple) -> Dict[str, Any]:
        """Formats exception info as a structured dictionary.

        Similar to structlog's dict_tracebacks processor, transforms exception
        information into a machine-readable dictionary suitable for JSON output.
        """
        exc_type, exc_value, exc_tb = exc_info
        if not exc_type:
            return {}

        frames = []
        tb = exc_tb
        while tb is not None:
            frame = tb.tb_frame
            frames.append(
                {
                    "filename": frame.f_code.co_filename,
                    "lineno": tb.tb_lineno,
                    "name": frame.f_code.co_name,
                    "locals": {
                        k: str(v)
                        for k, v in frame.f_locals.items()
                        if not k.startswith("_")
                    },
                }
            )
            tb = tb.tb_next

        return {
            "exc_type": exc_type.__name__ if exc_type else "Unknown",
            "exc_value": str(exc_value) if exc_value else "",
            "frames": frames,
        }
