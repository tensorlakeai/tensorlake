import importlib
import pathlib
import sys
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, List, Optional, Set

from tensorlake.builder.client_v2 import BuildContext

from .graph import Graph
from .graph_serialization import graph_code_dir_path
from .image import Image


@dataclass
class ImageInfo:
    image: Image
    build_contexts: List[BuildContext]


@dataclass
class WorkflowModuleInfo:
    graphs: List[Graph]
    images: Dict[Image, ImageInfo]
    secret_names: Set[str]


def load_workflow_module_info(workflow_file_path: str) -> WorkflowModuleInfo:
    workflow_file_path = str(pathlib.Path(workflow_file_path).resolve())
    workflow_module: ModuleType = _load_workflow_module(workflow_file_path)
    return _load_workflow_module_info(workflow_module)


def _load_workflow_module(workflow_file_path: str) -> ModuleType:
    # Add graph code directory to the current Python module search path
    # because we're going to load the workflow file into the current interpreter.
    if graph_code_dir_path(workflow_file_path) not in sys.path:
        sys.path.insert(0, graph_code_dir_path(workflow_file_path))

    if not str(workflow_file_path).endswith(".py"):
        raise ValueError("Workflow must be a .py file")

    if not pathlib.Path(workflow_file_path).is_file():
        raise ValueError(f"Workflow file {workflow_file_path} is not a regular file")

    # Converts module path into importable module name, e.g.:
    # /path/to/workflow.py -> workflow.
    # Such import works because we already added the graph code directory /path/to to the Python
    # module search path.
    workflow_module_name: str = pathlib.Path(workflow_file_path).stem
    return importlib.import_module(workflow_module_name)


def _load_workflow_module_info(workflow_module: ModuleType) -> WorkflowModuleInfo:
    workflow_module_info: WorkflowModuleInfo = WorkflowModuleInfo(
        graphs=[],
        images={},
        secret_names=set(),
    )

    for name in dir(workflow_module):
        obj = getattr(workflow_module, name)
        if not isinstance(obj, Graph):
            continue

        graph: Graph = obj
        workflow_module_info.graphs.append(graph)
        for node_name, node_obj in graph.nodes.items():
            for secret in node_obj.secrets or []:
                workflow_module_info.secret_names.add(secret)

            image: Image = node_obj.image
            if image not in workflow_module_info.images:
                workflow_module_info.images[image] = ImageInfo(
                    image=image,
                    build_contexts=[],
                )

            workflow_module_info.images[image].build_contexts.append(
                BuildContext(
                    graph_name=graph.name,
                    graph_version=graph.version,
                    function_name=node_name,
                )
            )

    return workflow_module_info
