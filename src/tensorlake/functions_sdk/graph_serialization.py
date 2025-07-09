import inspect
import io
import os
import pathlib
import stat
import zipfile
from typing import Dict, List

import click
from pydantic import BaseModel

from .functions import TensorlakeCompute
from .graph import Graph
from .graph_definition import ComputeGraphMetadata


class FunctionManifest(BaseModel):
    # Name of the function in the graph.
    name: str
    # The name used to import the module where the function class is defined.
    # The name is not relative but points at a module inside graph code directory.
    module_import_name: str
    # The string used to import the function class from its module.
    class_import_name: str


GRAPH_MANIFEST_FILE_NAME = "graph_manifest.json"
GRAPH_METADATA_FILE_NAME = "graph_metadata.json"


class GraphManifest(BaseModel):
    version: str
    # The name of the function int the graph -> FunctionManifest.
    functions: Dict[str, FunctionManifest]


ZIPPED_GRAPH_CODE_CONTENT_TYPE = "application/zip"
# If only graph Python code is put into the ZIP archive without external dependencies then
# the code size should be muchs smaller than 5 MB.
_MAX_GRAPH_CODE_SIZE_BYTES = 5 * 1024 * 1024
# Allow soft links in graph code directory. This allows users to include dirs and files
# into graph code directory that are not really inside the directory.
# This might result in infinite recursion but we protect from it by checking the size of
# the ZIP archive as we go.
_FOLLOW_LINKS = True


def graph_code_dir_path(workflow_file_path: str) -> str:
    # The workflow file must be in the the graph code directory.
    return os.path.dirname(workflow_file_path)


def zip_graph_code(graph: Graph, code_dir_path: str) -> bytes:
    """Returns ZIP archive with all Python source files from graph code directory.

    Raises ValueError if failed to create the ZIP archive due to graph or code directory issues.
    """
    code_dir_path = str(pathlib.Path(code_dir_path).resolve())
    graph_manifest: GraphManifest = _create_graph_manifest(
        graph=graph,
        code_dir_path=code_dir_path,
    )

    zip_buffer = io.BytesIO()
    try:
        _zip_graph_code(
            zip_buffer=zip_buffer,
            graph_manifest=graph_manifest,
            graph_metadata=graph.definition(),
            code_dir_path=code_dir_path,
        )
        return zip_buffer.getvalue()
    except Exception as e:
        _save_zip_for_debugging(zip_buffer)
        raise


def _zip_graph_code(
    zip_buffer: io.BytesIO,
    graph_manifest: GraphManifest,
    graph_metadata: ComputeGraphMetadata,
    code_dir_path: str,
) -> None:
    """Zips the graph code directory and metadata and writes it to the ZIP buffer.

    Raises ValueError if failed to create the ZIP archive due to graph code directory issues.
    """
    graph_code_size: int = 0
    zip_infos: List[zipfile.ZipInfo] = []
    with zipfile.ZipFile(
        zip_buffer,
        "w",
        compression=zipfile.ZIP_DEFLATED,
        allowZip64=False,
        compresslevel=5,
    ) as zipf:
        zipf.writestr(GRAPH_MANIFEST_FILE_NAME, graph_manifest.model_dump_json())
        zipf.writestr(GRAPH_METADATA_FILE_NAME, graph_metadata.model_dump_json())
        for dir_path, _, file_names in os.walk(
            code_dir_path, followlinks=_FOLLOW_LINKS
        ):
            for file_name in file_names:
                # Only include Python files.
                if not file_name.endswith(".py"):
                    continue

                file_path = os.path.join(dir_path, file_name)

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
                graph_code_size += os.path.getsize(file_path)
                # Check graph code size after adding each file to the ZIP archive to prevent infinite
                # recursion because we allow soft links in the graph code directory for users' convenience.
                _check_graph_code_size(graph_code_size, zip_infos)


def _check_graph_code_size(
    graph_code_size: int, zip_infos: List[zipfile.ZipInfo]
) -> None:
    """Checks if the size of the graph code is less than _MAX_GRAPH_CODE_SIZE_BYTES.

    If the size is greater than _MAX_GRAPH_CODE_SIZE_BYTES, raises a ValueError.
    """
    if graph_code_size <= _MAX_GRAPH_CODE_SIZE_BYTES:
        return

    click.echo(f"Graph code ZIP archive content:")
    for zip_info in zip_infos:
        click.echo(f"  {zip_info.filename}: {zip_info.file_size} bytes")
    raise ValueError(
        f"Graph code size {graph_code_size / 1024 / 1024} MB exceeds maximum size {_MAX_GRAPH_CODE_SIZE_BYTES / 1024/ 1024} MB. "
        "Please check the graph code ZIP archive content above to see if anything unexpected is included."
    )


def _save_zip_for_debugging(zip_buffer: io.BytesIO) -> None:
    zip_save_path: str = os.getenv("GRAPH_CODE_ZIP_SAVE_PATH", "")
    if zip_save_path == "":
        return

    with open(zip_save_path, "wb") as f:
        f.write(zip_buffer.getvalue())


def _create_graph_manifest(
    graph: Graph,
    code_dir_path: str,
) -> GraphManifest:
    function_manifests: Dict[str, FunctionManifest] = {}
    for node in graph.nodes.values():
        function_manifests[node.name] = _create_function_manifest(node, code_dir_path)

    return GraphManifest(version="0.1.0", functions=function_manifests)


def _create_function_manifest(
    node: TensorlakeCompute,
    code_dir_path: str,
) -> FunctionManifest:
    if node._created_by_decorator:
        import_file_path: str = inspect.getsourcefile(node.run)
    else:
        import_file_path: str = inspect.getsourcefile(node)
    if import_file_path is None:
        raise ValueError(
            f"Function {node.name} is not defined in any file. "
            "Please copy the function file into the graph code directory."
        )
    import_file_path = os.path.abspath(import_file_path)
    if not import_file_path.startswith(code_dir_path):
        raise ValueError(
            f"Function {node.name} is defined in {import_file_path} "
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

    if node._created_by_decorator:
        # The class will be created by the function decorator when we import the module.
        class_import_name: str = node.run.__name__
    else:
        # The class is already defined in the module as is.
        class_import_name: str = node.__name__

    return FunctionManifest(
        name=node.name,
        module_import_name=module_import_name,
        class_import_name=class_import_name,
    )
