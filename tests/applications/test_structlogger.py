import io
import json
import sys
import unittest

from pydantic import BaseModel

from tensorlake.applications import (
    Logger,
    Request,
    application,
    function,
    run_local_application,
)


class DummyPayload(BaseModel):
    pass


@application()
@function(description="test logging info")
def test_logging_info(payload: DummyPayload) -> str:
    logger = Logger.get_logger(module="test")
    logger.info("info message")
    return "info"


@application()
@function(description="test logging warning")
def test_logging_warning(payload: DummyPayload) -> str:
    logger = Logger.get_logger(module="test")
    logger.warning("warning message")
    return "warning"


@application()
@function(description="test logging debug")
def test_logging_debug(payload: DummyPayload) -> str:
    logger = Logger.get_logger(module="test")
    logger.debug("debug message")
    return "debug"


@application()
@function(description="test logging error")
def test_logging_error(payload: DummyPayload) -> str:
    logger = Logger.get_logger(module="test")
    logger.error("error message")
    return "error"


@application()
@function(description="test logging error with exception")
def test_logging_error_exception(payload: DummyPayload) -> str:
    logger = Logger.get_logger(module="test")
    try:
        raise ValueError("test error")
    except Exception as e:
        logger.error("error with exception", exc_info=e)
    return "error_exception"


class TestInternalLoggerLogging(unittest.TestCase):
    def setUp(self):
        self.captured_output = io.StringIO()
        sys.stdout = self.captured_output

    def tearDown(self):
        sys.stdout = sys.__stdout__

    def test_info_log(self):
        request: Request = run_local_application(
            test_logging_info,
            DummyPayload(),
        )
        self.assertEqual(request.output(), "info")
        output = self.captured_output.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)
        log_entry = json.loads(lines[0])
        self.assertEqual(log_entry["level"], "info")
        self.assertEqual(log_entry["event"], "info message")

    def test_warning_log(self):
        request: Request = run_local_application(
            test_logging_warning,
            DummyPayload(),
        )
        self.assertEqual(request.output(), "warning")
        output = self.captured_output.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)
        log_entry = json.loads(lines[0])
        self.assertEqual(log_entry["level"], "warning")
        self.assertEqual(log_entry["event"], "warning message")

    def test_debug_log(self):
        request: Request = run_local_application(
            test_logging_debug,
            DummyPayload(),
        )
        self.assertEqual(request.output(), "debug")
        output = self.captured_output.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)
        log_entry = json.loads(lines[0])
        self.assertEqual(log_entry["level"], "debug")
        self.assertEqual(log_entry["event"], "debug message")

    def test_error_log(self):
        request: Request = run_local_application(
            test_logging_error,
            DummyPayload(),
        )
        self.assertEqual(request.output(), "error")
        output = self.captured_output.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)
        log_entry = json.loads(lines[0])
        self.assertEqual(log_entry["level"], "error")
        self.assertEqual(log_entry["event"], "error message")

    def test_error_exception_log(self):
        request: Request = run_local_application(
            test_logging_error_exception,
            DummyPayload(),
        )
        self.assertEqual(request.output(), "error_exception")
        output = self.captured_output.getvalue()
        lines = output.strip().split("\n")
        self.assertEqual(len(lines), 1)
        log_entry = json.loads(lines[0])
        self.assertEqual(log_entry["level"], "error")
        self.assertEqual(log_entry["event"], "error with exception")
        self.assertIn("exception", log_entry)


if __name__ == "__main__":
    unittest.main()
