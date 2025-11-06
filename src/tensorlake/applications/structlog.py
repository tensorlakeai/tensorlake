import tensorlake.vendor.structlog.structlog as structlog


def configure_logging():
    """Configure structlog to output in JSON format"""
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,  # Add log level
            structlog.processors.StackInfoRenderer(),  # Add stack info for exceptions
            structlog.processors.dict_tracebacks,  # Formats exception info
            structlog.processors.JSONRenderer(),  # Render the log entry as JSON
        ],
        cache_logger_on_first_use=True,
    )


def get_logger(info):
    """Get a logger instance"""
    return structlog.get_logger(info)
