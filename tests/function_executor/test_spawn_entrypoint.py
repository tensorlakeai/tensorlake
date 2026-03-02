from pathlib import Path
import runpy
import unittest
from unittest.mock import patch


SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2]
    / "tensorlake.data"
    / "scripts"
    / "function-executor"
)


class TestFunctionExecutorEntrypoint(unittest.TestCase):
    def test_spawn_does_not_execute_main(self):
        with patch("tensorlake.function_executor.main.main") as main_mock:
            runpy.run_path(str(SCRIPT_PATH), run_name="__mp_main__")
            main_mock.assert_not_called()

    def test_direct_execution_calls_main(self):
        with patch("tensorlake.function_executor.main.main") as main_mock:
            runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
            main_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
