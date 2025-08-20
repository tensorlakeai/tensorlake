import io
import os
import pathlib
import stat
import zipfile
from pathlib import Path
from typing import List

import click

from ...interface.application import Application

# If only application Python code is put into the ZIP archive without external dependencies then
# the code size should be much smaller than 5 MB.
_MAX_APPLICATION_CODE_SIZE_BYTES = 5 * 1024 * 1024
# Allow soft links in application code directory. This allows users to include dirs and files
# into application code directory that are not really inside the directory.
# This might result in infinite recursion but we protect from it by checking the size of
# the ZIP archive as we go.
_FOLLOW_LINKS = True


def zip_application_code(application: Application, code_dir_path: str) -> bytes:
    """Returns ZIP archive with all Python source files from application code directory.

    Raises ValueError if failed to create the ZIP archive due to application or code directory issues.
    """
    code_dir_path = str(pathlib.Path(code_dir_path).resolve())

    zip_buffer = io.BytesIO()
    try:
        _zip_application_code(
            zip_buffer=zip_buffer,
            code_dir_path=code_dir_path,
        )
        return zip_buffer.getvalue()
    except Exception as e:
        _save_zip_for_debugging(zip_buffer)
        raise


def _detect_files_to_exclude(root_dir: str) -> List[str]:
    root = Path(root_dir).resolve()
    exclude_paths = set()

    venv_path = os.environ.get("VIRTUAL_ENV")
    if venv_path:
        venv_path = Path(venv_path).resolve()
        try:
            venv_path.relative_to(root)
            exclude_paths.add(str(venv_path))
        except ValueError:
            # venv is not inside root_dir, ignore
            pass

    # 2. Parse .gitignore if present
    gitignore_path = root / ".gitignore"
    if gitignore_path.exists():
        patterns = []
        with gitignore_path.open("r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                patterns.append(line)

        # For each pattern, use glob to find matches
        for pattern in patterns:
            if pattern.endswith("/"):
                pattern = pattern.rstrip("/")
                for match in root.glob(pattern):
                    if match.is_dir():
                        exclude_paths.add(str(match.resolve()))
            else:
                for match in root.glob(pattern):
                    if match.exists():
                        exclude_paths.add(str(match.resolve()))

    return list(exclude_paths)


def _zip_application_code(
    zip_buffer: io.BytesIO,
    code_dir_path: str,
) -> None:
    """Zips the application code directory and writes it to the ZIP buffer.

    Raises ValueError if failed to create the ZIP archive due to application code directory issues.
    """
    app_code_size: int = 0
    zip_infos: List[zipfile.ZipInfo] = []

    # Work with absolute paths to simplify comparisons
    code_dir_path = os.path.abspath(code_dir_path)
    exclude = set(_detect_files_to_exclude(code_dir_path))

    with zipfile.ZipFile(
        zip_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=False,
        compresslevel=5,
    ) as zipf:
        for dir_path, dir_names, file_names in os.walk(
            code_dir_path, followlinks=_FOLLOW_LINKS
        ):
            # Prevent walking into excluded directories
            dir_names[:] = [
                d for d in dir_names if os.path.join(dir_path, d) not in exclude
            ]
            for file_name in file_names:
                # Only include Python files.
                if not file_name.endswith(".py"):
                    continue

                file_path = os.path.join(dir_path, file_name)
                if file_path in exclude:
                    print(f"excluding file from zip: {str(file_path)}")
                    continue
                # The file is added to the ZIP archive with its original rwx/rwx/rwx permissions.
                # When unzipping the files owner and group are set to the current process uid, gid.
                # We need to check that file owner has read access on the file so the unzipping process
                # can load and run them.
                if not (os.stat(file_path).st_mode & stat.S_IRUSR):
                    raise ValueError(
                        f"Graph code file {file_path} is not readable by its owner. "
                        "Please change the file permissions."
                    )

                file_path_inside_code_dir = os.path.relpath(file_path, code_dir_path)
                zipf.write(file_path, file_path_inside_code_dir)
                zip_infos.append(zipf.getinfo(file_path_inside_code_dir))
                app_code_size += os.path.getsize(file_path)
                # Check application code size after adding each file to the ZIP archive to prevent infinite
                # recursion because we allow soft links in the application code directory for users' convenience.
                _check_app_code_size(app_code_size, zip_infos)


def _check_app_code_size(app_code_size: int, zip_infos: List[zipfile.ZipInfo]) -> None:
    """Checks if the size of the application code is less than _MAX_APPLICATION_CODE_SIZE_BYTES.

    If the size is greater than _MAX_APPLICATION_CODE_SIZE_BYTES, raises a ValueError.
    """
    if app_code_size <= _MAX_APPLICATION_CODE_SIZE_BYTES:
        return

    click.echo(f"Application code ZIP archive content:")
    for zip_info in zip_infos:
        click.echo(f"  {zip_info.filename}: {zip_info.file_size} bytes")
    raise ValueError(
        f"Application code size {app_code_size / 1024 / 1024} MB exceeds maximum size {_MAX_APPLICATION_CODE_SIZE_BYTES / 1024/ 1024} MB. "
        "Please check the application code ZIP archive content above to see if anything unexpected is included."
    )


def _save_zip_for_debugging(zip_buffer: io.BytesIO) -> None:
    zip_save_path: str = os.getenv("APPLICATION_CODE_ZIP_SAVE_PATH", "")
    if zip_save_path == "":
        return

    with open(zip_save_path, "wb") as f:
        f.write(zip_buffer.getvalue())
