import stat
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path

from tensorlake.applications.remote.code.zip import (
    CODE_ZIP_MANIFEST_FILE_NAME,
    zip_code,
)


class TestCodeZip(unittest.TestCase):
    def test_application_zip_is_canonical(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            nested = root / "nested"
            nested.mkdir()
            (root / "z.py").write_text("Z = 1\n")
            (root / "a.py").write_text("A = 1\n")
            (nested / "m.py").write_text("M = 1\n")

            first = zip_code(str(root), set(), [])
            second = zip_code(str(root), set(), [])

        self.assertEqual(first, second)
        with zipfile.ZipFile(BytesIO(first)) as archive:
            self.assertEqual(
                archive.namelist(),
                [
                    CODE_ZIP_MANIFEST_FILE_NAME,
                    "a.py",
                    "z.py",
                    "nested/m.py",
                ],
            )
            for entry in archive.infolist():
                self.assertEqual(entry.date_time, (1980, 1, 1, 0, 0, 0))
                self.assertEqual(entry.create_system, 3)
                self.assertTrue(stat.S_ISREG(entry.external_attr >> 16))


if __name__ == "__main__":
    unittest.main()
