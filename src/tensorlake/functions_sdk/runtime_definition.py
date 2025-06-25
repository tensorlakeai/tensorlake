from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class TaskInfoMetadata(BaseModel):
    pending_tasks: int
    successful_tasks: int
    failed_tasks: int


class InvocationErrorMetadata(BaseModel):
    function_name: str
    message: str


class InvocationMetadata(BaseModel):
    id: str
    completed: bool
    status: str
    outcome: str
    failure_reason: str
    outstanding_tasks: int
    task_analytics: dict[str, TaskInfoMetadata]
    graph_version: str
    created_at: datetime
    invocation_error: Optional[InvocationErrorMetadata] = None
