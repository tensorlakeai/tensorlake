from typing import Dict, List, Optional

from pydantic import BaseModel

from tensorlake.functions_sdk.image import ImageInformation

from .object_serializer import get_serializer
from .resources import ResourceMetadata


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


class RuntimeInformation(BaseModel):
    major_version: int
    minor_version: int
    sdk_version: str


class ComputeGraphMetadata(BaseModel):
    name: str
    description: str
    start_node: FunctionMetadata
    tags: Dict[str, str] = {}
    nodes: Dict[str, FunctionMetadata]
    edges: Dict[str, List[str]]
    accumulator_zero_values: Dict[str, bytes] = {}
    runtime_information: RuntimeInformation
    version: str

    def get_input_payload_serializer(self):
        return get_serializer(self.start_node.compute_fn.input_encoder)

    def get_input_encoder(self) -> str:
        if self.start_node.input_encoder:
            return self.start_node.input_encoder

        raise ValueError("start node is not set on the graph")
