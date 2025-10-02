import importlib
import os
import sys
from typing import Generator, Set


def load_code(application_file_path: str) -> None:
    """Loads the supplied python file with application.

    The supplied path must be absolute.
    """

    if not os.path.isabs(application_file_path):
        raise ValueError(
            f"The deployed application file path `{application_file_path}` must be absolute."
        )

    code_dir_path: str = os.path.dirname(application_file_path)
    if code_dir_path not in sys.path:
        # Makes `import foo` work if foo.py is in the code directory.
        sys.path.insert(0, code_dir_path)

    py_file_path_inside_code_dir: str = os.path.relpath(
        application_file_path, code_dir_path
    )
    module_import_name: str = py_file_path_inside_code_dir.replace(os.path.sep, ".")[
        :-3
    ]  # Remove the ".py" suffix

    # Note: the same module can be imported multiple times using different import paths.
    # In this case user gets a warning message.
    importlib.import_module(module_import_name)


# Allow soft links in code directory. This allows users to include dirs and files
# into code directory that are not really inside the directory.
# This might result in infinite recursion but we protect from it by checking the size of
# the ZIP archive as we go.
_FOLLOW_LINKS = True


def walk_code(
    code_dir_or_file_path: str, ignored_absolute_paths: Set[str]
) -> Generator[str, None, None]:
    """Yields all absolute Python file paths from the code directory or just yields the supplied file path."""
    if os.path.isfile(code_dir_or_file_path):
        if code_dir_or_file_path.endswith(".py"):
            yield os.path.abspath(code_dir_or_file_path)
        return

    for dir_path, dir_names, file_names in os.walk(
        code_dir_or_file_path, followlinks=_FOLLOW_LINKS
    ):
        # Prevent walking into excluded directories
        dir_names[:] = [
            dir_name
            for dir_name in dir_names
            if os.path.join(dir_path, dir_name) not in ignored_absolute_paths
        ]
        for file_name in file_names:
            # Only include Python files.
            if not file_name.endswith(".py"):
                continue

            file_path = os.path.abspath(os.path.join(dir_path, file_name))
            if file_path in ignored_absolute_paths:
                continue

            yield file_path
