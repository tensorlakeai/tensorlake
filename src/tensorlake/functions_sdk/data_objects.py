from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Json


class FileInput(BaseModel):
    url: str
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None


class Metrics(BaseModel):
    timers: Dict[str, float]
    counters: Dict[str, int]


class RouterOutput(BaseModel):
    edges: List[str]


class TensorlakeData(BaseModel):
    id: Optional[str] = None
    payload: Union[bytes, str]
    encoder: Literal["cloudpickle", "json"] = "cloudpickle"


class File(BaseModel):
    data: bytes
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    sha_256: Optional[str] = None
