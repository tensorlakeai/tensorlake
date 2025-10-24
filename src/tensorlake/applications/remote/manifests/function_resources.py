import math
from typing import List

from ...interface.function import Function
from .function_manifests import FunctionResourcesManifest, GPUResourceManifest


def _parse_gpu_resource(gpu: str) -> GPUResourceManifest:
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

    return GPUResourceManifest(count=gpu_count, model=gpu_model)


def _parse_gpu_resources(
    gpu: str | List[str] | None,
) -> List[GPUResourceManifest]:
    """Parses GPU resources from `gpu` attribute of Function."""
    if gpu is None:
        return []
    if isinstance(gpu, str):
        return [_parse_gpu_resource(gpu)]
    if isinstance(gpu, list):
        return [_parse_gpu_resource(g) for g in gpu]
    raise ValueError(f"Invalid GPU format: {gpu}. Expected str or List[str].")


def resources_for_function(
    function: Function,
) -> FunctionResourcesManifest:
    return FunctionResourcesManifest(
        cpus=function._function_config.cpu,
        memory_mb=math.ceil(
            function._function_config.memory * 1024
        ),  # float GB to int MB
        ephemeral_disk_mb=math.ceil(
            function._function_config.ephemeral_disk * 1024
        ),  # float GB to int MB
        gpus=_parse_gpu_resources(function._function_config.gpu),
    )
