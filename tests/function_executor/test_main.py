import sys
import unittest
from unittest.mock import MagicMock, patch

import tensorlake.function_executor.main as function_executor_main


class _ExitCalled(Exception):
    pass


class TestFunctionExecutorMain(unittest.TestCase):
    def test_does_not_flush_application_replaced_streams_after_shutdown(self) -> None:
        logger = MagicMock()
        logger.bind.return_value = logger
        server = MagicMock()
        stdout = MagicMock()
        stderr = MagicMock()

        with (
            patch.object(
                sys,
                "argv",
                [
                    "function-executor",
                    "--executor-id",
                    "executor",
                    "--address",
                    "127.0.0.1:0",
                ],
            ),
            patch.object(sys, "stdout", stdout),
            patch.object(sys, "stderr", stderr),
            patch.object(
                function_executor_main.InternalLogger,
                "get_logger",
                return_value=logger,
            ),
            patch.object(function_executor_main, "Server", return_value=server),
            patch.object(
                function_executor_main.os,
                "_exit",
                side_effect=_ExitCalled,
            ) as exit_process,
        ):
            with self.assertRaises(_ExitCalled):
                function_executor_main.main()

        server.run.assert_called_once_with()
        stdout.flush.assert_not_called()
        stderr.flush.assert_not_called()
        exit_process.assert_called_once_with(0)


if __name__ == "__main__":
    unittest.main()
