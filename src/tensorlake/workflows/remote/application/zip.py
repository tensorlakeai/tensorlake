import inspect
import io
import os
import stat
import zipfile
from typing import Dict, List, Set

import click
from pydantic import BaseModel

from ...interface.function import Function
from ...registry import get_functions
from .loader import walk_application_code


class FunctionZIPManifest(BaseModel):
    # Name of the function in the graph.
    name: str
    # The name used to import the module where the function is defined.
    # The name is never relative but points at a module inside application code directory.
    module_import_name: str


class ApplicationZIPManifest(BaseModel):
    # The name of the function -> FunctionZIPManifest.
    functions: Dict[str, FunctionZIPManifest]


APPLICATION_ZIP_MANIFEST_FILE_NAME = "application_zip_manifest.json"


# If only application Python code is put into the ZIP archive without external dependencies then
# the code size should be much smaller than 5 MB.
_MAX_APPLICATION_CODE_SIZE_BYTES = 5 * 1024 * 1024


def zip_application_code(code_dir_path: str, ignored_absolute_paths: Set[str]) -> bytes:
    """Returns ZIP archive with all Python source files from application code directory.

    Raises ValueError if failed to create the ZIP archive due to application or code directory issues.
    """
    app_zip_manifest: ApplicationZIPManifest = _create_application_zip_manifest(
        code_dir_path=code_dir_path,
    )

    zip_buffer = io.BytesIO()
    try:
        _zip_application_code(
            zip_buffer=zip_buffer,
            app_zip_manifest=app_zip_manifest,
            code_dir_path=code_dir_path,
            ignored_absolute_paths=ignored_absolute_paths,
        )
        return zip_buffer.getvalue()
    except Exception:
        _save_zip_for_debugging(zip_buffer)
        raise


def _zip_application_code(
    zip_buffer: io.BytesIO,
    app_zip_manifest: ApplicationZIPManifest,
    code_dir_path: str,
    ignored_absolute_paths: Set[str],
) -> None:
    """Zips the application code directory and writes it to the ZIP buffer.

    Raises ValueError if failed to create the ZIP archive due to application code directory issues.
    """
    app_code_size: int = 0
    zip_infos: List[zipfile.ZipInfo] = []

    with zipfile.ZipFile(
        zip_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=False,
        compresslevel=5,
    ) as zipf:
        zipf.writestr(
            APPLICATION_ZIP_MANIFEST_FILE_NAME, app_zip_manifest.model_dump_json()
        )
        for file_path in walk_application_code(code_dir_path, ignored_absolute_paths):
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


def _create_application_zip_manifest(code_dir_path: str) -> ApplicationZIPManifest:
    function_manifests: Dict[str, FunctionZIPManifest] = {}
    # Functions defined in ignored files are not available in the registry.
    for function in get_functions():
        function: Function
        function_manifests[function.function_config.function_name] = (
            _create_function_zip_manifest(
                function=function,
                code_dir_path=code_dir_path,
            )
        )

    return ApplicationZIPManifest(
        functions=function_manifests,
    )


def _create_function_zip_manifest(
    function: Function, code_dir_path: str
) -> FunctionZIPManifest:
    function_name: str = function.function_config.function_name
    import_file_path: str = inspect.getsourcefile(function.original_function)
    if import_file_path is None:
        raise ValueError(
            f"Function {function_name} is not defined in any file. "
            "Please copy the function file into the graph code directory."
        )
    import_file_path = os.path.abspath(import_file_path)
    if not import_file_path.startswith(code_dir_path):
        raise ValueError(
            f"Function {function_name} is defined in {import_file_path} "
            f"which is not inside the graph code directory {code_dir_path}. "
            "Please copy or symlink the function file into the graph code directory."
        )

    import_file_path_in_code_dir: str = os.path.relpath(
        import_file_path, start=code_dir_path
    )
    # Converts relative path "foo/bar/buzz.py" to "foo.bar.buzz"
    # which is importable if code_dir_path is added to sys.path. Function Executor adds it.
    module_import_name: str = os.path.splitext(import_file_path_in_code_dir)[0].replace(
        os.sep, "."
    )

    return FunctionZIPManifest(
        name=function.function_config.function_name,
        module_import_name=module_import_name,
    )
