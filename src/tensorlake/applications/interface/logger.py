"""Public logger for applications with structured output."""

from ..internal_logger import InternalLogger


class Logger:
    """Public logger for applications with structured exception output.

    This logger outputs the message in a plain JSON object, alongside level,
    timestamp, and any other key/value pairs added to the logger's context.

    It also transforms exception tracebacks into a structured, machine-readable
    dictionary format suitable for JSON output and log aggregators.

    The output is formatted to put all the attributes in the root of the JSON
    object to differentiate them from the internal CloudEvent wrapped messages
    that Tensorlake produces through the application's lifecycle.
    """

    def __init__(
        self,
        context: dict,
    ):
        """Initialize Logger.

        Args:
            context: Dictionary of context variables to include in logs
        """
        self._logger = InternalLogger(
            context,
            InternalLogger.LOG_FILE.STDOUT,
            as_cloud_event=False,
        )

    @classmethod
    def get_logger(cls, **kwargs) -> "Logger":
        """Gets the root logger with the given context.

        Doesn't raise any exceptions.
        """
        return cls(context=kwargs)

    def bind(self, **kwargs) -> "Logger":
        """Binds additional context to the logger.

        Doesn't raise any exceptions.
        """
        bound_logger = self._logger.bind(**kwargs)
        result = self.__class__.__new__(self.__class__)
        result._logger = bound_logger
        return result

    def info(self, message: str, **kwargs):
        """Logs an info level message."""
        return self._logger.info(message, **kwargs)

    def error(self, message: str, **kwargs):
        """Logs an error level message."""
        return self._logger.error(message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Logs a debug level message."""
        return self._logger.debug(message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Logs a warning level message."""
        return self._logger.warning(message, **kwargs)
