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


class FunctionManifest(BaseModel):
    name: str
    description: str
    is_api: bool
    secret_names: List[str]
    initialization_timeout_sec: int
    timeout_sec: int
    resources: FunctionResourcesManifest
    retry_policy: RetryPolicyManifest
    cache_key: str | None
    parameters: List[ParameterManifest] | None
    return_type: Dict[str, Any] | None  # JSON Schema object
    placement_constraints: PlacementConstraintsManifest
    max_concurrency: int


class ApplicationManifest(BaseModel):
    name: str
    description: str
    tags: Dict[str, str]
    version: str
    functions: Dict[str, FunctionManifest]
    default_api: str
