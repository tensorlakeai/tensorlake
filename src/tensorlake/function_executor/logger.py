import datetime
import io
import json
import tempfile
import traceback
from typing import Any, Dict

# Logger with interface similar to structlog library.
# We need a separate logging library to make sure that no customer code is using it so
# we don't leak customer data into FE logs. The FE logs are logged to a file which is
# sent back to Executor in RPC responses. We don't log FE logs to FE stdout to not confuse
# customers with logs not from their functions.
#
# TODO: When FE streaming protocol is available send the logs to the Executor immediately
# so we don't miss FE logs when FE crashes and FE logs are received when they are generated
# (for easier debugging).


class FunctionExecutorLogger:
    def __init__(self, context: Dict[str, Any], log_file: io.TextIOWrapper):
        self._context: Dict[str, Any] = context
        self._log_file: io.TextIOWrapper = log_file

    @classmethod
    def get_logger(self, **kwargs) -> "FunctionExecutorLogger":
        # The file doesn't have paths in filesystem so they get deleted on process exit including crashes.
        log_file = tempfile.TemporaryFile(mode="w+", encoding="utf-8")
        return FunctionExecutorLogger(
            context=kwargs,
            log_file=log_file,
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

    def end(self) -> int:
        """Returns the position at the current end of the log."""
        return self._log_file.tell()

    def read_till_the_end(self, start: int) -> str:
        self._log_file.flush()
        end: int = self.end()
        self._log_file.seek(start)
        text: str = self._log_file.read(end - start)
        self._log_file.seek(0, io.SEEK_END)  # Move the position back to the end
        return text

    def _log(self, level: str, message: str, **kwargs):
        formatted_message = self._format_message(level, message, **kwargs)
        # Uncomment this line to start sending FE logs to Executor where it'll log them.
        # self._log_file.write(formatted_message + "\n")
        # TODO: Stop printing FE logs once their streaming to host or a logging service is implemented.
        print(formatted_message, flush=True)

    def _format_message(self, level: str, message: str, **kwargs) -> str:
        """Formats the log message with context and additional key-value pairs.

        The format is the same json format as structlog uses.
        """
        context: Dict[str, Any] = self._context.copy()
        context.update(kwargs)
        context["level"] = level
        context["timestamp"] = (
            datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        )
        context["event"] = message
        if "exc_info" in context:
            context["exception"] = "".join(
                traceback.format_exception(context["exc_info"])
            )
            del context["exc_info"]
        return json.dumps(context)
