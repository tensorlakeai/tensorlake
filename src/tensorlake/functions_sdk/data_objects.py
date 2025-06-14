from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Json

from ..user_error import InvocationArgumentError


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
    # The task threw an exception; this is retryable, and does not
    # break the graph.
    Task = "task"

    # The task has a problem with its arguments; this is not
    # retryable, and does not break the graph.
    InvocationArgument = "invocation_argument"


class Failure(BaseModel):
    scope: FailureScope
    cls: str
    msg: str
    trace: str

    @classmethod
    def from_exception(cls, exc: Exception, trace: str) -> "Failure":
        scope = FailureScope.Task
        if isinstance(exc, InvocationArgumentError):
            scope = FailureScope.InvocationArgument

        return cls(scope=scope, cls=type(exc).__name__, msg=str(exc), trace=trace)
