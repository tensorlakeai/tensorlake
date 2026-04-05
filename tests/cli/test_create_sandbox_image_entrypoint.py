import runpy
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT_PATH: Path = (
    Path(__file__).resolve().parents[2]
    / "tensorlake.data"
    / "scripts"
    / "tensorlake-create-sandbox-image"
)


class TestCreateSandboxImageEntrypoint(unittest.TestCase):
    def test_spawn_does_not_execute_entrypoint(self):
        with patch(
            "tensorlake.cli.create_sandbox_image.create_sandbox_image_entrypoint"
        ) as entrypoint_mock:
            runpy.run_path(str(SCRIPT_PATH), run_name="__mp_main__")
            entrypoint_mock.assert_not_called()

    def test_direct_execution_calls_entrypoint(self):
        with patch(
            "tensorlake.cli.create_sandbox_image.create_sandbox_image_entrypoint"
        ) as entrypoint_mock:
            runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
            entrypoint_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main()
