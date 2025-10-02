from typing import (
    Any,
    Dict,
    List,
)

from pydantic import BaseModel


class GPUResourceManifest(BaseModel):
    count: int
    model: str


class FunctionResourcesManifest(BaseModel):
    cpus: float
    memory_mb: int
    ephemeral_disk_mb: int
    gpus: List[GPUResourceManifest]


class ParameterManifest(BaseModel):
    name: str
    data_type: Dict[str, Any]  # JSON Schema object with optional "default" property
    description: str | None
    required: bool


class RetryPolicyManifest(BaseModel):
    max_retries: int
    initial_delay_sec: float
    max_delay_sec: float
    delay_multiplier: float


class PlacementConstraintsManifest(BaseModel):
    filter_expressions: List[str]
