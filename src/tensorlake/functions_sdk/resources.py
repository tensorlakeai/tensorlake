import math
from typing import List, Optional, Union

from pydantic import BaseModel

from .functions import TensorlakeCompute


class GPUResourceMetadata(BaseModel):
    count: int
    model: str


def _parse_gpu_resource(gpu: str) -> GPUResourceMetadata:
    # Example: "A100-80GB:2", "H100", "A100-40GB:4"
    parts: List[str] = gpu.split(":")
    if len(parts) > 2:
        raise ValueError(
            f"Invalid GPU format: {gpu}. Expected format is 'GPU_MODEL:COUNT'."
        )

    gpu_model: str = parts[0]
    gpu_count: int = 1
    if len(parts) == 2:
        gpu_count = int(parts[1])

    return GPUResourceMetadata(count=gpu_count, model=gpu_model)


def _parse_gpu_resources(
    gpu: Optional[Union[str, List[str]]],
) -> List[GPUResourceMetadata]:
    """Parses GPU resources from `gpu` attribute of TensorlakeCompute."""
    if gpu is None:
        return []
    if isinstance(gpu, str):
        return [_parse_gpu_resource(gpu)]
    if isinstance(gpu, list):
        return [_parse_gpu_resource(g) for g in gpu]
    raise ValueError(f"Invalid GPU format: {gpu}. Expected str or List[str].")


class ResourceMetadata(BaseModel):
    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int
    gpus: List[GPUResourceMetadata] = []


def resource_metadata_for_graph_node(
    node: TensorlakeCompute,
) -> ResourceMetadata:
    return ResourceMetadata(
        cpus=node.cpu,
        memory_mb=math.ceil(node.memory * 1024),  # float GB to int MB
        ephemeral_disk_mb=math.ceil(node.ephemeral_disk * 1024),  # float GB to int MB
        gpus=_parse_gpu_resources(node.gpu),
    )
