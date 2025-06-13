from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Json

from ..user_error import InvocationError


class FileInput(BaseModel):
    url: str
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Json]] = None
    sha_256: Optional[str] = None


class Metrics(BaseModel):
    timers: Dict[str, float]
    counters: Dict[str, int]


class TensorlakeData(BaseModel):
    id: Optional[str] = None
    payload: Union[bytes, str]
    encoder: Literal["cloudpickle", "json"] = "cloudpickle"


class File(BaseModel):
    data: bytes
    mime_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    sha_256: Optional[str] = None


class FailureScope(str, Enum):
    Task = "task"
    Invocation = "invocation"


class Failure(BaseModel):
    scope: FailureScope
    cls: str
    msg: str
    trace: str

    @classmethod
    def from_exception(cls, exc: Exception, trace: str) -> "Failure":
        scope = FailureScope.Task
        if isinstance(exc, InvocationError):
            scope = FailureScope.Invocation

        return cls(scope=scope, cls=type(exc).__name__, msg=str(exc), trace=trace)
