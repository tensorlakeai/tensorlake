from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class TaskInfoMetadata(BaseModel):
    pending_tasks: int
    successful_tasks: int
    failed_tasks: int


class InvocationStateRunning(BaseModel):
    status: Literal["Running"]


class InvocationStateSuccess(BaseModel):
    status: Literal["Success"]


class InvocationStateError(BaseModel):
    status: Literal["Failed"]
    failed_compute_fn: None | str = None
    failure_cls: None | str = None
    failure_msg: None | str = None
    failure_trace: None | str = None


class InvocationMetadata(BaseModel):
    id: str
    completed: bool
    status: str
    outcome: str
    outstanding_tasks: int
    task_analytics: dict[str, TaskInfoMetadata]
    graph_version: str
    created_at: datetime
    state: (
        None | InvocationStateRunning | InvocationStateSuccess | InvocationStateError
    ) = Field(default=None, discriminator="status")
