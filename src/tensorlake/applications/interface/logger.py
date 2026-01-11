"""Public logger for applications with structured output."""

from ..internal_logger import InternalLogger


class Logger:
    """Public logger for applications with dict_traceback support enabled.

    This logger automatically transforms exception tracebacks into a structured,
    machine-readable dictionary format suitable for JSON output and log aggregators.

    Outputs plain JSON without CloudEvent wrapping.
    """

    def __init__(
        self,
        context: dict,
        destination=None,
    ):
        """Initialize Logger.

        Args:
            context: Dictionary of context variables to include in logs
            destination: Log file destination (internal use)
        """
        if destination is None:
            destination = InternalLogger.LOG_FILE.STDOUT

        self._logger = InternalLogger(
            context, destination, _dict_traceback=True, _as_cloud_event=False
        )

    def __getattr__(self, name):
        """Delegate all attribute access to the underlying logger."""
        return getattr(self._logger, name)

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
