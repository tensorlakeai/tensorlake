from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from tensorlake.functions_sdk.image import ImageInformation

from .object_serializer import get_serializer
from .resources import ResourceMetadata


class ParameterMetadata(BaseModel):
    name: str
    data_type: Dict[str, Any]  # JSON Schema object with optional "default" property
    description: Optional[str] = None
    required: bool = True


class RetryPolicyMetadata(BaseModel):
    max_retries: int
    initial_delay_sec: float
    max_delay_sec: float
    delay_multiplier: float


class FunctionMetadata(BaseModel):
    name: str
    fn_name: str
    description: str
    reducer: bool = False
    image_information: Optional[ImageInformation]
    input_encoder: str = "cloudpickle"
    output_encoder: str = "cloudpickle"
    secret_names: Optional[List[str]] = None
    timeout_sec: Optional[int] = None
    resources: Optional[ResourceMetadata] = None
    retry_policy: Optional[RetryPolicyMetadata] = None
    cache_key: Optional[str] = None
    parameters: Optional[List[ParameterMetadata]] = None
    return_type: Optional[Dict[str, Any]] = None  # JSON Schema object


class RuntimeInformation(BaseModel):
    major_version: int
    minor_version: int
    sdk_version: str


class ComputeGraphMetadata(BaseModel):
    name: str
    description: str
    entrypoint: FunctionMetadata
    tags: Dict[str, str] = {}
    functions: Dict[str, FunctionMetadata]
    edges: Dict[str, List[str]]
    accumulator_zero_values: Dict[str, bytes] = {}
    runtime_information: RuntimeInformation
    version: str

    def get_input_payload_serializer(self):
        return get_serializer(self.entrypoint.compute_fn.input_encoder)

    def get_input_encoder(self) -> str:
        if self.entrypoint.input_encoder:
            return self.entrypoint.input_encoder

        raise ValueError("start node is not set on the graph")
