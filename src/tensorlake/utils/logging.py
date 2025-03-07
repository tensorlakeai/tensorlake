import logging
import sys

import structlog

# Using this module allows us to be consistent with the logging configuration across all Python programs.


def configure_logging_early():
    """Configures standard Python logging module.

    By default 3rd party modules that are using standard Python logging module
    (logging.getLogger()) have their log lines dropped unless the module gets configured.

    Not dropping log lines from 3rd party modules is useful for debugging. E.g. this helps
    debugging errors in grpc servers if exceptions happen inside grpc module code.
    """
    logging.basicConfig(
        level=logging.WARNING,
        # This log message format is a bit similar to the default structlog format.
        format="%(asctime)s [%(levelname)s] %(message)s logger=%(name)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def configure_development_mode_logging():
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog_suppressor,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(),
    ]
    structlog.configure(
        processors=processors,
    )


def configure_production_mode_logging():
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog_suppressor,
        structlog.processors.add_log_level,
        structlog.dev.set_exc_info,
        structlog.processors.format_exc_info,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),
        structlog.processors.JSONRenderer(),
    ]
    structlog.configure(processors=processors)


_suppress_logging = False


def structlog_suppressor(logger, name, event_dict):
    global _suppress_logging
    if _suppress_logging:
        raise structlog.DropEvent
    else:
        return event_dict


def suppress():
    global _suppress_logging
    _suppress_logging = True
    logging.getLogger().setLevel(logging.CRITICAL)
