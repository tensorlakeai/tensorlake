import math
from typing import List, Optional, Union

from pydantic import BaseModel

from .functions import TensorlakeCompute, TensorlakeRouter


class GPUResourceMetadata(BaseModel):
    count: int
    model: str


def _parse_gpu_resource_metadata(gpu: str) -> GPUResourceMetadata:
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


class ResourceMetadata(BaseModel):
    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int
    gpu: Optional[GPUResourceMetadata] = None


def resource_metadata_for_graph_node(
    node: Union[TensorlakeCompute, TensorlakeRouter]
) -> ResourceMetadata:
    return ResourceMetadata(
        cpus=node.cpu,
        memory_mb=math.ceil(node.memory * 1024),  # float GB to int MB
        ephemeral_disk_mb=math.ceil(node.ephemeral_disk * 1024),  # float GB to int MB
        gpu=(None if node.gpu is None else _parse_gpu_resource_metadata(node.gpu)),
    )
