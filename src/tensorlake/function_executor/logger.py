import datetime
import io
import json
import sys
import traceback
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
    def get_logger(self, **kwargs) -> "FunctionExecutorLogger":
        return FunctionExecutorLogger(
            context=kwargs,
            log_file=sys.stdout,
        )

    def bind(self, **kwargs) -> "FunctionExecutorLogger":
        context = self._context.copy()
        context.update(kwargs)
        return FunctionExecutorLogger(context=context, log_file=self._log_file)

    def info(self, message: str, **kwargs):
        self._log("info", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log("error", message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log("debug", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log("warning", message, **kwargs)

    def _log(self, level: str, message: str, **kwargs):
        formatted_message = self._format_message(level, message, **kwargs)
        self._log_file.write(formatted_message + "\n")

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

        return json.dumps(
            new_cloud_event(context, source="/tensorlake/function_executor/logger")
        )
