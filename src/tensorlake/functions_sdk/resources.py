import math
from enum import Enum
from typing import Optional, Union

from pydantic import BaseModel

from .functions import TensorlakeCompute, TensorlakeRouter


class GPU_MODEL(str, Enum):
    """GPU models available in Tensorlake Cloud."""

    H100 = "H100"
    A100_40GB = "A100-40GB"
    A100_80GB = "A100-80GB"


_ALLOWED_GPU_MODELS = set(item.value for item in GPU_MODEL)


class GPUResourceMetadata(BaseModel):
    count: int
    model: GPU_MODEL


def _parse_gpu_resource_metadata(gpu: str) -> GPUResourceMetadata:
    parts = gpu.split(":")
    if len(parts) > 2:
        raise ValueError(
            f"Invalid GPU format: {gpu}. Expected format is 'GPU_MODEL:COUNT'."
        )

    count: int = 1
    if len(parts) == 2:
        count = int(parts[1])
        if count < 1 or count > 8:
            raise ValueError(
                f"Invalid GPU count: {count}. Count must be between 1 and 8."
            )

    gpu_model = parts[0]
    if gpu_model not in _ALLOWED_GPU_MODELS:
        raise ValueError(
            f"Unsupported GPU model: {gpu_model}. Supported models are: {', '.join(_ALLOWED_GPU_MODELS)}."
        )

    return GPUResourceMetadata(count=count, model=GPU_MODEL(gpu_model))


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
